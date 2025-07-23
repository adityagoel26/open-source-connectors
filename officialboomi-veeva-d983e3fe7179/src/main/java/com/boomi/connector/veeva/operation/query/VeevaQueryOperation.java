// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.operation.query;

import com.boomi.common.apache.http.response.HttpResponseUtil;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.veeva.VeevaOperationConnection;
import com.boomi.connector.veeva.util.ExecutionUtils;
import com.boomi.connector.veeva.util.HttpClientFactory;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class VeevaQueryOperation extends BaseQueryOperation {

    public static final String GET_ITEMS_AT_PATH_LIST_OBJECT_TYPE = "get_items_at_path_list";
    public static final String CUSTOM_VQL_OPERATION_FIELD_ID = "CUSTOM_TERMS";
    private static final Logger LOG = LogUtil.getLogger(VeevaQueryOperation.class);
    private static final int SLASHES_AT_URI = 2;
    private static final String QUERY_ENDPOINT_PATH = "/query";
    private static final String RESPONSE_HEADERS_TRACKED_GROUP_PROPERTY = "response";
    private final PropertyMap _opProps;
    private final String _objectTypeId;

    public VeevaQueryOperation(VeevaOperationConnection conn) {
        super(conn);
        _opProps = conn.getContext().getOperationProperties();
        _objectTypeId = conn.getContext().getObjectTypeId();
    }

    /**
     * Extracts and processes the nextPage URI and adds the results found in the data node
     *
     * @param httpResponse    from the current page to process
     * @param responseHandler responsible for adding successful responses an errors
     * @param metadata        to set the tracked group properties
     * @return the next page URI, or {@code null} if there aren't any more pages
     * @throws IOException when an error happens while reading the content of the httpResponse or instantiating a
     *                     {@link VQLResponseSplitter}
     */
    protected static String processResponse(CloseableHttpResponse httpResponse, QueryResponseHandler responseHandler,
            PayloadMetadata metadata) throws IOException {
        InputStream stream = null;
        VQLResponseSplitter splitter = null;
        try {
            stream = httpResponse.getEntity().getContent();
            StatusLine status = httpResponse.getStatusLine();
            int httpResponseCode = status.getStatusCode();

            String nextPage = null;

            Map<String, String> condensedHeaders = HttpResponseUtil.getCondensedHeaders(httpResponse);
            metadata.setTrackedGroupProperties(RESPONSE_HEADERS_TRACKED_GROUP_PROPERTY, condensedHeaders);

            if (stream != null && httpResponseCode == HttpStatus.SC_OK) {
                splitter = new VQLResponseSplitter(stream, "/data/*", "/responseDetails/next_page", metadata);
                for (Payload payload : splitter) {
                    addPartialResult(splitter, payload, responseHandler);
                }
                nextPage = splitter.getNextPageElementValue();
            } else {
                responseHandler.addApplicationError(PayloadUtil.toPayload(stream, metadata), status.getReasonPhrase(),
                        String.valueOf(httpResponseCode));
                String reason = status.getReasonPhrase();
                LOG.log(Level.WARNING, () -> httpResponseCode + " " + reason);
            }

            return nextPage;
        } finally {
            IOUtil.closeQuietly(splitter, httpResponse, stream);
        }
    }

    private static void addPartialResult(VQLResponseSplitter respSplitter, Payload payload,
            QueryResponseHandler responseHandler) {
        if (respSplitter.hasError()) {
            responseHandler.addApplicationError(payload,
                    "Verify VQL parameters and privileges for all selected fields. View response for details.",
                    "ERROR");
        } else {
            responseHandler.addSuccess(payload);
        }
    }

    /**
     * Logs a message both in the provided {@link Logger} and in the container log.
     *
     * @param logger  specific logger
     * @param message message to log
     */
    private static void logInfo(Logger logger, String message) {
        logger.info(message);
        LOG.info(message);
    }

    @Override
    public VeevaOperationConnection getConnection() {
        return (VeevaOperationConnection) super.getConnection();
    }

    @Override
    protected void executeQuery(QueryRequest request, OperationResponse response) {
        FilterData filterData = request.getFilter();

        QueryResponseHandler responseHandler = new QueryResponseHandler(request, response);

        try (CloseableHttpClient client = HttpClientFactory.createHttpClient(getContext())) {
            CloseableHttpResponse httpResponse = executeFirstPage(response, client, filterData);

            boolean pendingPage = true;
            while (pendingPage) {
                String nextPage = processResponse(httpResponse, responseHandler, response.createMetadata());
                if (StringUtil.isNotBlank(nextPage)) {
                    String nextPageUri = extractNextPageRequestUri(nextPage);
                    httpResponse = ExecutionUtils.execute("GET", nextPageUri, client, null, getConnection(),
                            filterData);
                } else {
                    pendingPage = false;
                }
            }
        } catch (IOException | URISyntaxException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            responseHandler.addFailure(e, ConnectorException.getStatusCode(e));
        } finally {
            responseHandler.finish();
        }
    }

    private CloseableHttpResponse executeFirstPage(OperationResponse response, CloseableHttpClient client,
            FilterData filterData) throws IOException {
        if (GET_ITEMS_AT_PATH_LIST_OBJECT_TYPE.equals(_objectTypeId)) {
            String uri = buildItemsAtPathUri(filterData);
            return ExecutionUtils.execute("GET", uri, client, null, getConnection(), filterData);
        }

        String query = buildVQLQuery(filterData);
        logInfo(response.getLogger(), "Query: " + query);
        return ExecutionUtils.execute("POST", QUERY_ENDPOINT_PATH, client,
                new ByteArrayInputStream(query.getBytes(StandardCharsets.UTF_8)), getConnection(), filterData);
    }

    /**
     * Creates a get_items_at_path_list object type query by concatenating query params in the uri.
     * If the user input in the path field is empty it will ignore it.
     * It will append a recursive field if checked by the user (default is false).
     *
     * @return the corresponding URI
     */
    protected String buildItemsAtPathUri(FilterData filterData) {
        String uri = "/services/file_staging/items";
        String userInputtedPath = filterData.getDynamicOperationProperties().getProperty("PATH");

        if (StringUtil.isNotBlank(userInputtedPath)) {
            uri += "/" + userInputtedPath;
        }

        if (Boolean.parseBoolean(filterData.getDynamicOperationProperties().getProperty("RECURSIVE"))) {
            uri += "?recursive=true";
        }
        return uri;
    }

    /**
     * Creates a VeevaQueryLanguage query.
     * If the user provided any value in the CUSTOM_TERMS operation field, it will take it as is and append it,
     * ignoring the operation filters.
     * If the CUSTOM_TERMS operation field is empty, it will create it through a VQLQueryBuilder using the operation
     * filters.
     *
     * @return the VQL Query to be used as a request body.
     */
    private String buildVQLQuery(FilterData filterData) {
        String query = StringUtil.trimToEmpty(_opProps.getProperty(CUSTOM_VQL_OPERATION_FIELD_ID));

        if (StringUtil.isNotBlank(query)) {
            return "q=" + query;
        }

        return new VQLQueryBuilder(getContext()).buildVQLQuery(filterData.getFilter());
    }

    /**
     * The nextPage field in the response contains Veeva Vault API endpoint to interact with, including the API
     * version, they need to be removed since the ExecutionUtils.execute method will add those fields to the URL
     * In the case of regular queries the endpoint is expected to be {@code /api/v23.3/query/434b3248-5fc7-4299-8574
     * -b4e036a05e5a?pagesize=1000&pageoffset=1000"}
     * In the case of "get_item_at_path_list" the url is expected to be
     * {@code https://myvault.veevavault.com/api/v24.1/services/file_staging/items/{path}}
     *
     * @param nextPage the json node in string format as extracted from the response
     * @return the http response from the service
     */
    String extractNextPageRequestUri(String nextPage) throws URISyntaxException {

        // this cleans the url in case it has a host and https scheme (as will happen in get_items_at_path_list)
        URIBuilder uriBuilder = new URIBuilder(nextPage).setHost(StringUtil.EMPTY_STRING).setScheme(
                StringUtil.EMPTY_STRING);
        // removes unnecessary slashes added by URIBuilder at the beginning of the URI
        String toRemove = ":///";
        // appends the api and version segments of the URI to the string to be removed
        toRemove += uriBuilder.getPathSegments().stream().limit(SLASHES_AT_URI).collect(Collectors.joining("/"));
        // normalizes the URI by removing the string built and returns the expected format
        return uriBuilder.toString().replace(toRemove, StringUtil.EMPTY_STRING);
    }
}