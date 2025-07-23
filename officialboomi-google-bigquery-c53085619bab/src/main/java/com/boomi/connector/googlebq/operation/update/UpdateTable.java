// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.update;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.InputStream;

/**
 * Represents a Table Update/Patch Request input. The Update Operations supports both statically defined tables if one
 * specific resource was selected from a list offered during the import process or dynamic types if "Dynamic Table"
 * is selected from that list
 *
 * To handle a Dynamically defined table this class will take the payload as it's received while the objectTypeId
 * (used as the request endpoint) will be build by reading the "datasetId" & "tableId" values from the "tableResource"
 * object included on the input document.
 *
 * A Statically defined table will be handled in the opposite way, by building the "tableResource" object of the
 * request payload be extracting those values from the objectTypeId formed during the import.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class UpdateTable {

    private static final String NODE_PROJECT_ID = "projectId";
    private static final String NODE_DATASET_ID = "datasetId";
    private static final String NODE_TABLE_ID = "tableId";

    private static final String ERROR_INVALID_INPUT = "the input document is missing the tableResource object";
    private static final String ERROR_INVALID_ID_FORMAT = "invalid object type id format";

    private static final int PATH_LIMIT = 4;
    private static final int PATH_DATASET_INDEX = 1;
    private static final int PATH_RESOURCE_INDEX = 3;

    private final ObjectNode _requestPayload;
    private final String _endpoint;

    /**
     * Creates a new instance of {@link UpdateTable}.
     *
     * @param data
     *         a {@link ObjectData} instance.
     * @param objectTypeId
     *         the selectedObjectType Id as a String value.
     * @param projectId
     *         the projectId configured for the connection.
     */
    public UpdateTable(ObjectData data, String objectTypeId, String projectId) {
        _requestPayload = parseInput(data);

        if (GoogleBqConstants.DYNAMIC_OBJECT_ID.equals(objectTypeId)) {
            _endpoint = buildDynamicObjectId(_requestPayload);
        }
        else {
            prepareStaticTablePayload(objectTypeId, _requestPayload, projectId);
            _endpoint = objectTypeId;
        }
    }

    /**
     * Getter method to access the endpoint for the table update request.
     *
     * @return the url endpoint as a String
     */
    public String getEndpoint() {
        return _endpoint;
    }

    /**
     * Getter method to access the table update request payload.
     *
     * @return a TableUpdate Resource body as a {@link JsonNode}
     */
    public JsonNode getRequestPayload() {
        return _requestPayload;
    }

    private static String buildDynamicObjectId(ObjectNode requestPayload) {
        JsonNode tableReference = requestPayload.path(GoogleBqConstants.NODE_TABLE_REFERENCE);
        JsonNode datasetId = tableReference.path(NODE_DATASET_ID);
        JsonNode tableId = tableReference.path(NODE_TABLE_ID);

        if (datasetId.isMissingNode() || tableId.isMissingNode()) {
            throw new ConnectorException(ERROR_INVALID_INPUT);
        }

        return String.format(GoogleBqConstants.DATASET_TABLES_URL_SUFFIX, datasetId.asText(), tableId.asText());
    }

    private static void prepareStaticTablePayload(String objectTypeId, ObjectNode requestPayload, String projectId) {
        String[] parts = StringUtil.fastSplit(URLUtil.URL_SEPARATOR_STRING, objectTypeId, PATH_LIMIT);
        if (parts.length != PATH_LIMIT) {
            throw new ConnectorException(ERROR_INVALID_ID_FORMAT);
        }
        ObjectNode tableReference = requestPayload.putObject(GoogleBqConstants.NODE_TABLE_REFERENCE);
        tableReference.put(NODE_PROJECT_ID, projectId);
        tableReference.put(NODE_DATASET_ID, parts[PATH_DATASET_INDEX]);
        tableReference.put(NODE_TABLE_ID, parts[PATH_RESOURCE_INDEX]);
    }

    private static ObjectNode parseInput(ObjectData data) {
        InputStream stream = null;
        try {
            stream = data.getData();
            return (ObjectNode) JSONUtil.parseNode(stream);
        }
        catch (Exception e) {
            throw new ConnectorException(GoogleBqConstants.ERROR_PARSE_JSON, e);
        }
        finally {
            IOUtil.closeQuietly(stream);
        }
    }

}
