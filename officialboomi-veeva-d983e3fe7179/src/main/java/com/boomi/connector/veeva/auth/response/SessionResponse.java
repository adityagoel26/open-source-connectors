// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.auth.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a Veeva API authentication response. If the response is successful, it includes a session id.
 */
public class SessionResponse {

    private static final String RESPONSE_STATUS_FAILURE = "FAILURE";
    private static final String SEPARATOR = ". ";
    private static final String EXPIRED_TOKEN_ERROR = "EXPIRED_TOKEN";
    private static final String INVALID_TOKEN_ERROR = "INVALID_TOKEN";
    private static final String LIMIT_EXCEEDED_ERROR = "API_LIMIT_EXCEEDED";

    private final String _responseStatus;
    private final String _sessionId;
    private final String _responseMessage;
    private final List<SessionResponseError> _errors;
    private final String _errorType;

    @JsonCreator
    public SessionResponse(@JsonProperty("responseStatus") String responseStatus,
            @JsonProperty("sessionId") String sessionId, @JsonProperty("responseMessage") String responseMessage,
            @JsonProperty("errors") Collection<SessionResponseError> errors,
            @JsonProperty("errorType") String errorType) {
        _responseStatus = responseStatus;
        _sessionId = sessionId;
        _responseMessage = responseMessage;
        _errors = errors != null ? new ArrayList<>(errors) : null;
        _errorType = errorType;
    }

    private static void appendProperty(StringBuilder errorMessageBuilder, String propertyLabel, String propertyValue) {
        errorMessageBuilder.append(propertyLabel).append(propertyValue).append(SEPARATOR);
    }

    /**
     * Indicates whether this authentication response is a failure.
     *
     * @return true if the response status is "FAILURE", false otherwise
     */
    public boolean isFailure() {
        return RESPONSE_STATUS_FAILURE.equals(_responseStatus);
    }

    /**
     * Indicates whether this authentication response has an OAuth error.
     *
     * @return true if the response has an expired/invalid access token error, false otherwise
     */
    public boolean isOAuthError() {
        return hasTargetError(EXPIRED_TOKEN_ERROR) || hasTargetError(INVALID_TOKEN_ERROR);
    }

    /**
     * Indicates whether this authentication response has an error due to exceeding the Auth API limits, which is only
     * possible when using User Credentials authentication.
     *
     * @return true if the response has an Auth API Burst Limit exceeded error, false otherwise
     */
    public boolean isApiLimitExceededError() {
        return hasTargetError(LIMIT_EXCEEDED_ERROR);
    }

    /**
     * Extracts a consolidated error message from this Veeva API authentication response.
     * Depending on the authentication method used, the response contains different properties.
     *
     * @return a consolidated error message for this response
     */
    public String extractErrorMessage() {
        StringBuilder errorMessageBuilder = new StringBuilder();
        if (_responseStatus != null) {
            appendProperty(errorMessageBuilder, "Response status:", _responseStatus);
        }
        if (_responseMessage != null) {
            appendProperty(errorMessageBuilder, "Response message:'", _responseMessage + "'");
        }
        if (_errorType != null) {
            appendProperty(errorMessageBuilder, "Error type:", _errorType);
        }
        if (_errors != null && (!_errors.isEmpty())) {
            appendErrorsArray(errorMessageBuilder);
        }
        return errorMessageBuilder.toString();
    }

    /**
     * Gets a string representing a Veeva Vault session.
     *
     * @return string, Session ID
     */
    public String getSessionId() {
        return _sessionId;
    }

    /**
     * Gets a string representing the status of this authentication response.
     *
     * @return string, the response status
     */
    String getResponseStatus() {
        return _responseStatus;
    }

    private void appendErrorsArray(StringBuilder errorMessageBuilder) {
        errorMessageBuilder.append("Errors: ");
        for (SessionResponseError error : _errors) {
            errorMessageBuilder.append(error.getType()).append(" ");
        }
    }

    private boolean hasTargetError(String targetError) {
        if (_errors == null || _errors.isEmpty()) {
            return false;
        }

        for (SessionResponseError error : _errors) {
            if (targetError.equals(error.getType())) {
                return true;
            }
        }

        return false;
    }
}
