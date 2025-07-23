// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.JsonPayloadUtil;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.update.UpdateTable;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;

import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

public class GoogleBqUpdateOperation extends BaseUpdateOperation {
    private static final String ERROR_CANNOT_PARSE_RESPONSE_BODY = "could not parse response body";
    private static final String ERROR_NO_RESPONSE_BODY = "there's no response body";

    private final TableResource _tableResource;
    private final String _projectId;

    /**
     * Constructs a new GoogleBqUpdateOperation instance.
     *
     * @param conn
     *         a {@link GoogleBqOperationConnection} instance.
     */
    public GoogleBqUpdateOperation(GoogleBqOperationConnection conn) {
        this(conn, new TableResource(conn));
    }

    /**
     * Constructs a new GoogleBqUpdateOperation instance.
     *
     * @param conn
     *         a {@link GoogleBqOperationConnection } instance.
     * @param tableResource
     *         a {@link TableResource} instance.
     */
    GoogleBqUpdateOperation(GoogleBqOperationConnection conn, TableResource tableResource) {
        super(conn);
        _tableResource = tableResource;
        _projectId = conn.getProjectId();
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse opResponse) {
        String objectTypeId = getContext().getObjectTypeId();

        for (ObjectData data : request) {
            try {
                UpdateTable update = new UpdateTable(data, objectTypeId, _projectId);
                Response response = isUpdate() ? _tableResource.updateTable(update) : _tableResource.patchTable(update);

                handleResponse(opResponse, data, response);
            }
            catch (ConnectorException e) {
                data.getLogger().log(Level.WARNING, e.getMessage(), e);
                opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getStatusMessage(),
                        null);
            }
            catch (Exception e) {
                ResponseUtil.addExceptionFailure(opResponse, data, e);
            }
        }
    }

    private static void handleResponse(OperationResponse opResponse, ObjectData data, Response response) {
        Status status = response.getStatus();
        String code = String.valueOf(status.getCode());
        OperationStatus opStatus = status.isSuccess() ? OperationStatus.SUCCESS : OperationStatus.APPLICATION_ERROR;

        if (!response.isEntityAvailable()) {
            throw new ConnectorException(code, ERROR_NO_RESPONSE_BODY);
        }

        try {
            opResponse.addResult(data, opStatus, code, status.getDescription(), getBody(response.getEntity()));
        }
        catch (IOException e) {
            throw new ConnectorException(code, ERROR_CANNOT_PARSE_RESPONSE_BODY, e);
        }
    }

    private static Payload getBody(Representation entity) throws IOException {
        InputStream stream = null;
        try {
            stream = entity.getStream();
            return JsonPayloadUtil.toPayload(JSONUtil.parseNode(stream));
        }
        finally {
            IOUtil.closeQuietly(stream);
        }
    }

    private boolean isUpdate() {
        return getContext().getOperationProperties().getBooleanProperty(GoogleBqConstants.PROP_IS_UPDATE, false);
    }
}
