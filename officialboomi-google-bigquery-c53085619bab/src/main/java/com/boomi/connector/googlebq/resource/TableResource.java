// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.resource;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.operation.update.UpdateTable;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class represents a table resource of Big Query.
 * This class holds different methods that can be performed
 * against a Big Query Table.
 *
 */
public class TableResource extends GoogleBqResource {
    private static final String LIST_TABLES_URL = "projects/%s/datasets/%s/tables";
    private static final String DELETE_TABLE_URL = "projects/%s/datasets/%s/tables/%s";
    private static final String PROJECTS_SUFFIX = "projects";

    private static final String NODE_TABLES = "tables";
    private static final String NODE_TABLE_REFERENCE = "tableReference";
    private static final String NODE_TABLE_ID = "tableId";

    private final String _projectId;

    /**
     * Every Big query api request requires an access token.
     * Every api request against a table resource needs a project id and dataset id
     * where the target table resides.
     *
     * @param connection
     *         a {@link GoogleBqBaseConnection<BrowseContext>} instance.
     */
    public TableResource(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        super(connection);
        _projectId = connection.getProjectId();
    }

    /**
     * This method lists all the tables present in a project and its dataset.
     * If the response returns a nextPageToken, this method will iteratively execute requests
     * for each token to get every page. The final computed list of tables will thereby contain
     * tables retrieved from all the pages.
     *
     * @return SortedSet<Resource>
     */
    public SortedSet<String> listTables(String datasetId, CollectionUtil.Filter<JsonNode> filter)
            throws IOException, GeneralSecurityException {
        SortedSet<String> resources = new TreeSet<>();
        String nextPageToken = null;

        do {
            String url = String.format(LIST_TABLES_URL, _projectId, datasetId);

            Map<String, String> queryParameters = new HashMap<>();
            addPageToken(queryParameters, nextPageToken);

            Response response = executeGet(queryParameters, url);

            JsonNode responseNode = handleResponse(response);
            nextPageToken = getNextPageToken(responseNode);

            JsonNode tables = responseNode.path(NODE_TABLES);
            if (tables.isArray()) {
                for (JsonNode table : CollectionUtil.filter(tables, filter)) {
                    String tableId = table.path(NODE_TABLE_REFERENCE).path(NODE_TABLE_ID).asText();
                    if (StringUtil.isNotEmpty(tableId)) {
                        resources.add(tableId);
                    }
                }
            }
        } while (StringUtil.isNotEmpty(nextPageToken));

        return resources;
    }

    /**
     * Gets the meta data of the table i.e. the field names and their data types in the table.
     * This method returns the response as a {@link JsonNode}.
     *
     * @return a JsonNode instance with the table received from the API
     */
    public JsonNode getTable(String suffixUrl) throws IOException, GeneralSecurityException {
        String serviceUrl = getEndpoint(suffixUrl);
        Response response = executeGet(new HashMap<String, String>(), serviceUrl);
        return handleResponse(response);
    }

    /**
     * Executes an update operation over a Google BigQuery resource and returns a {@link Response}
     * <p>
     * This execution will make a
     * <a href="https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/update>tables.update</a>
     * request to the Google Cloud API
     *
     * @param update
     *         an {@link UpdateTable} instance.
     * @return {@link Response}
     * @throws GeneralSecurityException
     *         this shouldn't never happen
     */
    public Response updateTable(UpdateTable update) throws GeneralSecurityException {
        return patchOrUpdate(update, false);
    }

    /**
     * Executes an patch operation over a Google BigQuery resource and returns a {@link Response}
     * <p>
     * This execution will make a
     * <a href="https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch>tables.patch</a>
     * request to the Google Cloud API
     *
     * @param update
     *         an {@link UpdateTable} instance.
     * @return {@link Response}
     * @throws GeneralSecurityException
     *         this shouldn't never happen
     */
    public Response patchTable(UpdateTable update) throws GeneralSecurityException {
        return patchOrUpdate(update, true);
    }

    /**
     * Executes a delete operation over a Google BigQuery resource and returns a {@link Response}
     *
     * @param datasetId
     * @param tableId
     * @return {@link Response} with empty body and 204 status if table is deleted
     * @throws GeneralSecurityException
     */
    public Response deleteTable(String datasetId, String tableId) throws GeneralSecurityException {
        String url = String.format(DELETE_TABLE_URL, _projectId, datasetId, tableId);
        return executeDelete(Collections.<String, String>emptyMap(), url);
    }


    private Response patchOrUpdate(UpdateTable update, boolean isPatch) throws GeneralSecurityException {
        JsonNode body = update.getRequestPayload();
        String endpoint = getEndpoint(update.getEndpoint());

        return isPatch? executePatch(endpoint, body) : executePut(endpoint, body);
    }


    private String getEndpoint(String suffixUrl) {
        return URLUtil.makeUrlString(PROJECTS_SUFFIX, _projectId, suffixUrl);
    }

    /**
     * Returns {@link JsonNode} present in the response by reading the {@link InputStream}.
     * Also validates the {@link Response} for errors and throws the error as an exception.
     *
     * @param response the restlet Response received from the API
     * @return a JsonNode instance with the response payload
     * @throws IOException if the payload is not parseable
     */
    private static JsonNode handleResponse(Response response) throws IOException {
        InputStream stream = null;
        try {
            if (!ResponseUtil.validateResponse(response)) {
                Status status = response.getStatus();
                throw new ConnectorException(
                        "Error occurred when retrieving table meta data. " + status.getDescription());
            }
            stream = response.getEntity().getStream();
            return JSONUtil.parseNode(stream);
        }
        finally {
            IOUtil.closeQuietly(stream);
        }
    }
}
