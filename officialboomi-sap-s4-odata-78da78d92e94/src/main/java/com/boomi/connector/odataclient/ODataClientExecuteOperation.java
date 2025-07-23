package com.boomi.connector.odataclient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Scanner;
import java.util.logging.Logger;

import com.boomi.connector.api.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

public class ODataClientExecuteOperation extends BaseUpdateOperation {

	Logger logger = Logger.getLogger(this.getClass().getName());

    public enum OperationProperties {SERVICEPATH, EXTRA_URL_PARAMETERS}
    ODataParseUtil oDataParseUtil;

    protected ODataClientExecuteOperation(ODataClientConnection conn) {
        super(conn);
    }

    //TODO build a $batch from parent and child objects
    //TODO Execute Function Import
    /**
     * * 1. Inbound JSON to Form Encoded parameters...Things like dates encoded? UTC? DateTimeOffset('xxxxx')?
     * * 2. Do a POST/GET according to m:httpMethod parameter in the metadata
     * * 3. Response could be a complex type, collection or even an entitytype?
     *
     * @param request
     * @param response
     */
    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        Logger log = response.getLogger();
        getConnection().setLogger(log);
        String customOperation = getContext().getCustomOperationType();
        OperationCookie inputCookie = new OperationCookie(getContext().getObjectDefinitionCookie(ObjectDefinitionRole.INPUT));
        OperationCookie outputCookie = null;
        oDataParseUtil = new ODataParseUtil();
        if (OperationType.GET.name().contentEquals(customOperation) || ODataConstants.POST.contentEquals(customOperation) || OperationType.EXECUTE.name().contentEquals(customOperation)) {
            outputCookie = new OperationCookie(getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT));
        }
        PropertyMap opProps = this.getContext().getOperationProperties();
        boolean captureHeaders = opProps.getBooleanProperty(ODataConstants.FETCH_HEADERS, true);

        String path = opProps.getProperty(OperationProperties.SERVICEPATH.name(), "").trim();
        if (StringUtil.isBlank(path))
            throw new ConnectorException("A Service URL Path is required");

        //If Capture Headers is turned on, For POST we only do this once to get x-csrf-token and session cookies, not for each document
        if (captureHeaders && ODataConstants.POST.contentEquals(customOperation)) {
            getSessionHeadersFromServiceCall(path);
        }

        path += getContext().getObjectTypeId();
        for (ObjectData input : request) {
            getExecuteResponse(response, customOperation, inputCookie, outputCookie, opProps, captureHeaders, path, input);
        }
    }

    /**
     * @param response
     * @param customOperation
     * @param inputCookie
     * @param outputCookie
     * @param opProps
     * @param captureHeaders
     * @param path
     * @param input
     */
    private void getExecuteResponse(OperationResponse response, String customOperation, OperationCookie inputCookie, OperationCookie outputCookie, PropertyMap opProps, boolean captureHeaders, String path, ObjectData input) {
        OutputStream tempOutputStreamIn = null;
        InputStream dataOut = null;
        OutputStream tempOutputStreamOut = null;
        CloseableHttpResponse httpResponse = null;
        InputStream inputData=null;
            //POST, GET, PUT, PATCH, DELETE, EXECUTE
        try(InputStream dataIn = input.getData()) {
            boolean hasInputBody = false;
            //GET, DELETE have only key values as predicate keys, nothing to send to api server
            //EXECUTE input get converted to url parameters so it has no input body
            if (ODataConstants.POST.contentEquals(customOperation) || ODataConstants.PUT.contentEquals(customOperation) || ODataConstants.PATCH.contentEquals(customOperation)) {
                hasInputBody = true;
                tempOutputStreamIn = getContext().createTempOutputStream();
            }
            String httpMethod = customOperation;
            String predicate = null;
            String uriParams = "";
            if (ODataConstants.EXECUTE.contentEquals(customOperation)) {
                uriParams = "?" + ODataParseUtil.parseBoomiToFunctionImportURL(dataIn, inputCookie);
                httpMethod = inputCookie.getHttpMethod();
            } else {
                //Note predicate ignored for POST
                //TODO if POST (not deep), PATCH, PUT and has child keys, build
                predicate = oDataParseUtil.parseBoomiToOData(dataIn, tempOutputStreamIn, inputCookie);
            }

            if (hasInputBody) {
                inputData = getContext().tempOutputStreamToInputStream(tempOutputStreamIn);
            }

            String resolvedPath = path;
            if (!ODataConstants.POST.contentEquals(customOperation) && !ODataConstants.EXECUTE.contentEquals(customOperation)) {
                if (StringUtil.isEmpty(predicate))
                    throw new ConnectorException("A predicate key is required for this operation: " + customOperation);
                resolvedPath += predicate;
                //If capture headers is turned on, For for DELETE, PUT, POST we will do GET to get the etag for each inbound document
                if (captureHeaders && !ODataConstants.GET.contentEquals(customOperation)) {
                    getETagHeaders(resolvedPath, inputCookie);
                }
            }
            uriParams = appendPath(uriParams, input.getDynamicProperties().get(OperationProperties.EXTRA_URL_PARAMETERS.name()));
            uriParams = appendPath(uriParams, opProps.getProperty(OperationProperties.EXTRA_URL_PARAMETERS.name()));

            resolvedPath += uriParams;
            httpResponse = getConnection().doExecute(resolvedPath, input, httpMethod, inputData);

            //If a GET capture the headers into the document properties
            PayloadMetadata payloadMetadata = null;
            if (ODataConstants.GET.contentEquals(httpMethod))
                payloadMetadata = getConnection().setSecurityHeadersToDocumentProperties(getContext());

            if (httpResponse.getEntity() != null)
                dataOut = httpResponse.getEntity().getContent();

            OperationStatus status = OperationStatus.SUCCESS;
            int httpResponseCode = httpResponse.getStatusLine().getStatusCode();
            String statusMessage = httpResponse.getStatusLine().getReasonPhrase();
            if (httpResponseCode >= 300)
                status = OperationStatus.APPLICATION_ERROR;

            if (dataOut != null) {
                //Only attempt to parse response if OK
                if (status == OperationStatus.SUCCESS) {
                    tempOutputStreamOut = getContext().createTempOutputStream();

                    oDataParseUtil.parseODataToBoomi(dataOut, tempOutputStreamOut, outputCookie);
                    dataOut = getContext().tempOutputStreamToInputStream(tempOutputStreamOut);
                } else {
                    //TODO For errors we want to append the stream contents so that it shows in shape exception in BUILD Tab
                    //TODO do we want to scrap this and require looking in the log?
                    try (Scanner scanner = new Scanner(dataOut, StandardCharsets.UTF_8.name())) {
                        String responseString = scanner.useDelimiter("\\A").next();
                        statusMessage += " " + responseString;
                        dataOut = new ByteArrayInputStream(responseString.getBytes());
                    }
                }
                response.addResult(input, status, httpResponseCode + "",
                        statusMessage, PayloadUtil.toPayload(dataOut, payloadMetadata));
            } else {
                response.addEmptyResult(input, status, httpResponseCode + "", statusMessage);
            }
        } catch (Exception e) {
            // make best effort to process every input
            ResponseUtil.addExceptionFailure(response, input, e);
        } finally {
            IOUtil.closeQuietly(inputData, tempOutputStreamIn, httpResponse, dataOut, tempOutputStreamOut);

        }
    }

    /**
     * Execute a GET on the service root to pull in the session cookies and CSRF Token.
     * This is required for POST operations.
     * The cookie and token are persisted in the ODataCLientConnection object
     *
     * @param resolvedPath
     */
    private void getSessionHeadersFromServiceCall(String resolvedPath) {
        CloseableHttpResponse httpResponse = null;
        //GET on the service URL to capture the session cookies and xcsrf. Note that our GET operations always set x-csrf-token to fetch for each request
        //The headers are captured in the connection object
        try {
            httpResponse = getConnection().doExecute(resolvedPath, null, ODataConstants.GET, null);
        } catch (IOException | GeneralSecurityException e) {
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(httpResponse);
        }
    }

    /**
     * Execute a GET on the predicate key to pull in the etag, session cookies and CSRF Token.
     * This is required for PATCH, PUT and DELETE operations.
     * The etag, cookie and token are persisted in the ODataCLientConnection object.
     * Note if the eTag is not returned in the header, an attempt to pull from the response payload will occur.
     *
     * @param resolvedPath
     * @param inputCookie
     */
    private void getETagHeaders(String resolvedPath, OperationCookie inputCookie) {
        CloseableHttpResponse httpResponse = null;
        InputStream dataOut = null;
        //GET on the service URL to capture the session cookies and xcsrf. Note that our GET operations always set x-csrf-token to fetch for each request
        //The headers are captured in the connection object
        try {
            httpResponse = getConnection().doExecute(resolvedPath, null, ODataConstants.GET, null);
            dataOut = httpResponse.getEntity().getContent();
            //if we didn't find an etag in the header...
            if (StringUtil.isEmpty(getConnection().geteTag())) {
                String eTag = oDataParseUtil.parseODataToBoomi(dataOut, null, inputCookie);
                if (StringUtil.isNotBlank(eTag)) {
                    getConnection().seteTag(eTag);
                }
            }
        } catch (IOException | GeneralSecurityException e) {
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(dataOut, httpResponse);
        }
    }

    /**
     * @param uriParams
     * @param newParams
     * @return
     */
    static String appendPath(String uriParams, String newParams) {
        if (newParams != null && newParams.trim().length() > 0) {
            newParams = newParams.trim();
            if (uriParams == null)
                uriParams = "";
            if (uriParams.length() > 0)
                uriParams += "&" + newParams;
            else
                uriParams = "?" + newParams;
        }
        return uriParams;
    }

    /**
     * @return
     */
    @Override
    public ODataClientConnection getConnection() {
        return (ODataClientConnection) super.getConnection();
    }
}