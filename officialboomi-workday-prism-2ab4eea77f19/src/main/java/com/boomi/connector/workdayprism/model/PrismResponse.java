//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.json.JsonPayloadUtil;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.workdayprism.utils.HttpStatusUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

/**
 * Custom class to encapsulate a {@link CloseableHttpResponse} from HttpClient
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class PrismResponse implements Closeable {
    private static final String ERROR_PROCESSING_BODY_CONTENT = "couldn't process response body";
    private static final String ERROR_STATUS_NOT_PRESENT = "status not present";

    private final CloseableHttpResponse response;
    private JsonNode jsonNode;

    /**
     * Creates a new {@link PrismResponse} instance
     *
     * @param response
     *         a {@link CloseableHttpResponse} instance.
     */
    public PrismResponse(CloseableHttpResponse response) {
        this.response = response;
    }

    /**
     * @return <code>true</code> if the internal response is a Success, <code>false</code> otherwise
     */
    public boolean isSuccess() {
        return HttpStatusUtils.isSuccess(response.getStatusLine());
    }

    /**
     * Returns a {@link JsonNode} representing the entity from the response body
     *
     * @return the json entity
     * @throws IOException
     *         if an error happens deserializing the entity
     */
    public JsonNode getJsonEntity() throws IOException {
        if(jsonNode == null){
            jsonNode = buildJsonEntity();
        }
        return jsonNode;
    }

    /**
     * Returns if the status is a not found status.
     *
     * @return true if the internal response is a Not Found HTTP code, false otherwise
     */
    public boolean isNotFound() {
        return HttpStatusUtils.isNotFound(response.getStatusLine());
    }

    /**
     * Adds a new result based on the http response from the Workday API into the provided {@link OperationResponse}
     *
     * @param data
     *         a {@link TrackedData} instance.
     * @param operationResponse
     *         an {@link OperationResponse} instance.
     * @throws ConnectorException
     *         if the {@link StatusLine} is not set in the internal response
     */
    public void addResult(TrackedData data, OperationResponse operationResponse) {
        StatusLine statusLine = getStatus();
        addResult(data, operationResponse, statusLine.getReasonPhrase(), getStatusCode());
    }

    /**
     * Adds a new result based on the http response from the Workday API and the given message into the provided
     * {@link OperationResponse}
     *
     * @param data
     *         a {@link TrackedData} instance.
     * @param operationResponse
     *         an {@link OperationResponse} instance.
     * @param message
     *         the status message
     * @throws ConnectorException
     *         if the {@link StatusLine} is not set in the internal response
     */
    public void addResult(TrackedData data, OperationResponse operationResponse, String message) {
        addResult(data, operationResponse, message, getStatusCode());
    }
     
    /** Returns the first line of a Response message.
      * @return String
      */
    String getStatusMessage(){
        return getStatus().getReasonPhrase();
    }

    
    /** Obtains the status line of this response
     * @return an instance of StatusLine
     */
    private StatusLine getStatus() {
        StatusLine status = response.getStatusLine();
        if (status == null) {
            throw new ConnectorException(ERROR_STATUS_NOT_PRESENT);
        }
        return status;
    }

    /** Returns the status code from the response
     * @return integer
     */
    public int getStatusCode() {
        return getStatus().getStatusCode();
    }

    /** Returns JsonNode if input is null
     * @return JsonNode
     * @throws IOException
     */
    private JsonNode buildJsonEntity() throws IOException {
        InputStream content = null;
        try {
            if (response.getEntity() != null) {
                content = response.getEntity().getContent();
                return JSONUtil.parseNode(content);
            }
        }
        finally {
            IOUtil.closeQuietly(content);
        }
        return null;
    }

    /** Appends API response to the final response pay load of a process using ResponseUtility methods
     * @param data 
     * @param operationResponse
     * @param message
     * @param code
     */
    private void addResult(TrackedData data, OperationResponse operationResponse, String message, int code) {
        InputStream content = null;
        Payload payload = null;
        try {
            if (jsonNode != null) {
                payload = JsonPayloadUtil.toPayload(jsonNode);
            }
            else {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    content = entity.getContent();
                    payload = PayloadUtil.toPayload(content);
                }
            }

            ResponseUtil.addResultWithHttpStatus(operationResponse, data, code, message, payload);
        }
        catch (Exception e) {
            data.getLogger().log(Level.WARNING, e.getMessage(), e);
            operationResponse.addResult(data, OperationStatus.APPLICATION_ERROR, String.valueOf(code),
                    ERROR_PROCESSING_BODY_CONTENT, null);
        }
        finally {
            IOUtil.closeQuietly(content);
            IOUtil.closeQuietly(payload);
        }
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(response);
    }
}
