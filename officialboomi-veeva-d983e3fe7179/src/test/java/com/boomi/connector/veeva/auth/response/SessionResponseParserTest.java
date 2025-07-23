// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.auth.response;

import com.boomi.util.StringUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SessionResponseParserTest {

    @Test
    void parseThrowsExceptionTest() throws IOException {
        String wrongPayload =
                "{\n" + "\t\"responseMessage\": \"Authentication failed for user dummyUser\",\n" + "\t\"errors\": [{\n"
                + "\t\t\"type\": \"USERNAME_OR_PASSWORD_INCORRECT\",\n"
                + "\t\t\"message\": \"Authentication failed for user: dummyUser.\"\n" + "\t}],\n"
                + "\t\"errorType\": \"AUTHENTICATION_FAILED\"\n" + "}";

        String expectedErrorMessage = "The service response cannot be parsed. Response Status is not present.";
        InputStream inputStream = new ByteArrayInputStream(wrongPayload.getBytes(StandardCharsets.UTF_8));
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

        when(response.getEntity().getContent()).thenReturn(inputStream);

        Executable executable = () -> SessionResponseParser.parse(response);

        Throwable t = Assertions.assertThrows(IOException.class, executable);
        assertEquals(expectedErrorMessage, t.getMessage());
    }

    @Test
    void parseApiLimitExceededPayload() throws IOException {
        CloseableHttpResponse responseMock = Mockito.mock(CloseableHttpResponse.class, Mockito.RETURNS_DEEP_STUBS);

        final byte[] body =
                ("{\"responseStatus\":\"FAILURE\",\"errors\":[{\"type\":\"API_LIMIT_EXCEEDED\",\"message\":\"You have "
                 + "exceeded the maximum number of authentication API calls allowed in a [1] minute period.\"}]}").getBytes(
                        StringUtil.UTF8_CHARSET);
        when(responseMock.getEntity().getContent()).thenReturn(new ByteArrayInputStream(body));

        SessionResponse sessionResponse = SessionResponseParser.parse(responseMock);

        Assertions.assertEquals("FAILURE", sessionResponse.getResponseStatus());
        Assertions.assertEquals("Response status:FAILURE. Errors: API_LIMIT_EXCEEDED ",
                sessionResponse.extractErrorMessage());
        Assertions.assertTrue(sessionResponse.isApiLimitExceededError());
        Assertions.assertNull(sessionResponse.getSessionId());
    }
}
