// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.resource;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.batch.Batch;
import com.boomi.connector.googlebq.operation.retry.GoogleBqPhasedRetry;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.restlet.data.Response;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * This class represents a table data resource in Google big query which
 * supports the streaming api to insert records into a big query table.
 *
 */
public class TableDataResource extends GoogleBqResource {

    private static final String INSERT_ALL_URL_PREFIX = "projects/%s";
    private static final String INSERT_ALL_SUFFIX = "insertAll";
    private static final String NODE_INSERT_ERRORS = "insertErrors";
    private static final String NODE_INDEX = "index";
    private static final String NODE_ERRORS = "errors";
    private static final String NODE_ROWS = "rows";
    private static final String NODE_JSON = "json";
    private static final String NODE_INSERT_ID = "insertId";
    private final boolean _generateInsertId;

    private final OperationContext _context;

    public TableDataResource(GoogleBqOperationConnection connection) {
        super(connection);
        _context = connection.getContext();
        _generateInsertId = _context.getOperationProperties()
                .getBooleanProperty(GoogleBqConstants.PROP_GENERATE_INSERT_ID, Boolean.TRUE);
    }

    /**
     * Executes the big query streaming api to load data into a big query table. Input
     * documents contained in a batch are added as rows in a POST request body. The POST request
     * is executed and records are uploaded into a big query table. If certain rows are not inserted
     * the api response contains error indicating the index of row where failure occurred. There is also
     * a error message enplaning the reason. Input documents with that index in {@link Batch} are marked
     * Application error.
     *
     * If a 503 error appears during the process, a phased retry strategy will be apply for the failing batch.
     *
     * @param batch
     * @return {@link Response}
     */
    public Response insertAll(Batch batch) throws IOException, GeneralSecurityException {

        PropertyMap operationProperties = _context.getOperationProperties();
        Boolean skipInvalidRows = operationProperties.getBooleanProperty(GoogleBqConstants.PROP_SKIP_INVALID_ROWS);
        Boolean ignoreUnknownValues = operationProperties.getBooleanProperty(
                GoogleBqConstants.PROP_IGNORE_UNKNOWN_VALUES);
        String templateSuffix = batch.getTemplateSuffix();

        ObjectNode requestNode = JSONUtil.newObjectNode();
        requestNode.put(GoogleBqConstants.PROP_SKIP_INVALID_ROWS, skipInvalidRows);
        requestNode.put(GoogleBqConstants.PROP_IGNORE_UNKNOWN_VALUES, ignoreUnknownValues);

        if (StringUtil.isNotEmpty(templateSuffix)) {
            requestNode.put(GoogleBqConstants.PROP_TEMPLATE_SUFFIX, templateSuffix);
        }
        addRows(requestNode, batch);

        String projectId = _context.getConnectionProperties().getProperty(GoogleBqConstants.PROP_PROJECT_ID);

        String suffixUrl = _context.getObjectTypeId();

        String serviceUrl = URLUtil.makeUrlString(String.format(INSERT_ALL_URL_PREFIX, projectId), suffixUrl,
                INSERT_ALL_SUFFIX);

        return executePost(serviceUrl, requestNode, new GoogleBqPhasedRetry());
    }

    /**
     * Adds input documents as rows to a json document which can be sent as a POST body
     * to the streaming api.
     * @param requestNode
     * @param batch
     */
    private void addRows(ObjectNode requestNode, Batch batch) throws IOException {

        ArrayNode rows = requestNode.putArray(NODE_ROWS);
        InputStream inputStream = null;
        try {
            ObjectNode row;
            for (ObjectData document : batch) {
                row = requestNode.objectNode();
                if (_generateInsertId) {
                    row.put(NODE_INSERT_ID, UUID.randomUUID().toString());
                }
                inputStream = document.getData();
                row.set(NODE_JSON, JSONUtil.parseNode(inputStream));
                rows.add(row);
            }
        } finally {
            IOUtil.closeQuietly(inputStream);
        }
    }

    /**
     * Reads the response to create a map of document/row index -> Error {@link JsonNode} received in response.
     * Also checks if an index present in the response is a valid index i.e. a tracked document is available for
     * that index. If it is not a valid index an IndexOutOfBounds exception is thrown by this method.
     *
     * @param response
     * @param batchCount
     * @return
     * @throws IOException
     */
    public Map<Integer, JsonNode> createIndexToErrorMap(Response response, int batchCount) throws IOException {

        InputStream stream = null;
        try {
            stream = response.getEntity().getStream();
            JsonNode responseNode = JSONUtil.parseNode(stream);
            JsonNode insertErrors = responseNode.path(NODE_INSERT_ERRORS);
            Map<Integer, JsonNode> indexToError = new HashMap<>();
            if (!insertErrors.isMissingNode()) {
                for (JsonNode error : insertErrors) {

                    int index = error.path(NODE_INDEX).asInt();
                    if (index < 0 || index > batchCount - 1) {
                        throw new IndexOutOfBoundsException("Error returned for an unrecognized input document");
                    }
                    JsonNode errorPayload = error.path(NODE_ERRORS);
                    if (errorPayload.isArray() && errorPayload.get(0) != null) {
                        indexToError.put(index, errorPayload);
                    }
                }
            }
            return indexToError;
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }
}
