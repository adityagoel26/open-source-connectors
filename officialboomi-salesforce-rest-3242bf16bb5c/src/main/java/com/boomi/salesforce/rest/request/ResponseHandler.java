// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.util.JSONUtils;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.LogUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpException;

import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.boomi.salesforce.rest.constant.Constants.API_LIMIT_KEY_RESPONSE;

/**
 * Helper class that is similar to RequestHandler, responsible for receiving and preparing responses
 */
public class ResponseHandler {
    private static final Logger LOG = LogUtil.getLogger(ResponseHandler.class);
    private static final String SOAP_FAULT_MESSAGE = "faultstring";
    private static final String REST_FAULT_MESSAGE = "message";
    private final ClassicHttpResponse _response;

    public ResponseHandler(ClassicHttpResponse response) {
        _response = response;
    }

    /**
     * Returns true if the response header contains success code
     *
     * @return true if Salesforce returned success code in response
     */
    public boolean isSuccess() {
        int code = _response.getCode();
        return code == Constants.OK_GET_CODE || code == Constants.OK_POST_CODE || code == Constants.OK_NO_CONTENT;
    }

    /**
     * Returns true if the response header contains session expired code SESSION_EXPIRES_CODE
     *
     * @return true if Salesforce returned session expired code in response
     */
    public boolean isSessionExpired() {
        return _response.getCode() == Constants.SESSION_EXPIRES_CODE;
    }

    /**
     * Returns true if the response header contains unauthorized or failed code
     *
     * @return true if Salesforce returned any error code in response
     */
    public boolean isAnyError() {
        return !isSuccess();
    }

    /**
     * Returns true if Salesforce the response header contains URI_LENGTH_LIMIT_EXCEEDED or HEADER_LENGTH_LIMIT_EXCEEDED
     * code
     */
    private boolean isURILengthExceeded() {
        return _response.getCode() == Constants.URI_LENGTH_LIMIT_EXCEEDED ||
               _response.getCode() == Constants.HEADER_LENGTH_LIMIT_EXCEEDED;
    }

    /**
     * @return true if the response content type is XML
     * @throws HttpException when failed to get response header
     */
    private boolean isContentXML() throws HttpException {
        return _response.getHeader(Constants.CONTENT_TYPE_REQUEST).getValue().contains("xml");
    }

    /**
     * @return true if the response content type is XML
     * @throws HttpException when failed to get response header
     */
    private boolean isContentJSON() throws HttpException {
        return _response.getHeader(Constants.CONTENT_TYPE_REQUEST).getValue().contains("json");
    }

    /**
     * Gets the error message if detected either XML or JSON content, returns null if was unable to detect error
     * message
     *
     * @return Response error message
     */
    public String getErrorMessage() {
        try {
            if (isContentXML()) {
                return XMLUtils.getValueSafely(_response, REST_FAULT_MESSAGE);
            } else if (isContentJSON()) {
                // parse small sized error message
                return JSONUtils.getValueSafely(_response, REST_FAULT_MESSAGE);
            } else if (isURILengthExceeded()) {
                return "The length of the URI exceeds the 16,384 byte limit";
            } else {
                return "Unexpected Errors occurred while requesting Salesforce, This is generally due to invalid "
                        + "Connection URL";
            }
        } catch (Exception e) {
            Supplier<String> errorMessage = () -> "[Failed to read error response] " + e.getMessage();
            LOG.log(Level.INFO, e, errorMessage);
            return errorMessage.get();
        }
    }

    public String getAPIInfo() {
        String apiLimit = null;
        try {
            apiLimit = _response.getHeader(API_LIMIT_KEY_RESPONSE).toString();
        } catch (Exception e) {
            LOG.log(Level.INFO, e,
                    () -> "an error happened extracting the " + API_LIMIT_KEY_RESPONSE + " header: " + e.getMessage());
        }
        return apiLimit;
    }

    public String getAuthenticationErrorMessage() {
        try {
            return XMLUtils.getValueSafely(_response, SOAP_FAULT_MESSAGE);
        } catch (Exception e) {
            Supplier<String> errorMessage = () -> "[Failed to read error response] " + e.getMessage();
            LOG.log(Level.INFO, e, errorMessage);
            return errorMessage.get();
        }
    }
}
