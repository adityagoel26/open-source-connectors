// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.auth.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a Veeva API auth error. The error message is not included considering it includes the access token for
 * OAuth 2.0 expired/invalid error.
 */
public class SessionResponseError {

    private final String _type;

    @JsonCreator
    public SessionResponseError(@JsonProperty("type") String type) {
        _type = type;
    }

    /**
     * Returns a string representing the error type.
     *
     * @return string, the error type
     */
    String getType() {
        return _type;
    }
}
