// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.auth.response;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SessionResponseTest {

    @Test
    void notFailureTest() {
        List<SessionResponseError> errors = new ArrayList<>();
        SessionResponse response = new SessionResponse("SUCCESS", "sessionId", "responseMessage", errors, "errorType");
        assertFalse(response.isOAuthError());
        assertFalse(response.isApiLimitExceededError());
    }

    @Test
    void nullErrorsTest() {
        SessionResponse response = new SessionResponse("FAILURE", "sessionId", "responseMessage", null, "errorType");
        assertFalse(response.isOAuthError());
        assertFalse(response.isApiLimitExceededError());
    }

    @Test
    void emptyErrorsTest() {
        List<SessionResponseError> errors = new ArrayList<>();
        SessionResponse response = new SessionResponse("FAILURE", "sessionId", "responseMessage", errors, "errorType");
        assertFalse(response.isOAuthError());
        assertFalse(response.isApiLimitExceededError());
    }

    @Test
    void anotherErrorTest() {
        List<SessionResponseError> errors = new ArrayList<>();
        errors.add(new SessionResponseError("VEEVA_ERROR"));
        SessionResponse response = new SessionResponse("FAILURE", "sessionId", "responseMessage", errors, "errorType");
        assertFalse(response.isOAuthError());
        assertFalse(response.isApiLimitExceededError());
    }

    @Test
    void expiredTokenErrorTest() {
        List<SessionResponseError> errors = new ArrayList<>();
        errors.add(new SessionResponseError("EXPIRED_TOKEN"));
        SessionResponse response = new SessionResponse("FAILURE", "sessionId", "responseMessage", errors, "errorType");
        assertTrue(response.isOAuthError());
        assertFalse(response.isApiLimitExceededError());
    }

    @Test
    void invalidTokenErrorTest() {
        List<SessionResponseError> errors = new ArrayList<>();
        errors.add(new SessionResponseError("INVALID_TOKEN"));
        SessionResponse response = new SessionResponse("FAILURE", "sessionId", "responseMessage", errors, "errorType");
        assertTrue(response.isOAuthError());
        assertFalse(response.isApiLimitExceededError());
    }

    @Test
    void apiLimitExceededErrorTest() {
        List<SessionResponseError> errors = new ArrayList<>();
        errors.add(new SessionResponseError("API_LIMIT_EXCEEDED"));
        SessionResponse response = new SessionResponse("FAILURE", null, "responseMessage", errors, "errorType");
        assertFalse(response.isOAuthError());
        assertTrue(response.isApiLimitExceededError());
    }
}

