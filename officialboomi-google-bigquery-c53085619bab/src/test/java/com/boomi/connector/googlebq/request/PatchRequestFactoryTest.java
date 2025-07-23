// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.request;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.restlet.RestletUtil;

import org.junit.Test;
import org.restlet.data.Request;
import org.restlet.resource.Representation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PatchRequestFactoryTest {

    private static final String X_HTTP_METHOD_OVERRRIDE = "X-HTTP-Method-Override";
    private static final String PATCH = "PATCH";
    private static final String ENDPOINT = "test_endpoint";
    private static final String ACCESS_TOKEN = "access_token";
    private static final int ATTEMPTS = 2;

    private final Representation _representation = mock(Representation.class);
    private final GoogleBqBaseConnection<BrowseContext> _connection = mock(GoogleBqBaseConnection.class);

    private final PatchRequestFactory _factory = new PatchRequestFactory(_connection, ENDPOINT, _representation);

    @Test
    public void shouldCreateRequest() throws Exception {
        when(_connection.getAccessToken(false)).thenReturn(ACCESS_TOKEN);

        Request request = _factory.createRequest(ATTEMPTS);
        assertNotNull(request);
        String value = RestletUtil.getHttpHeader(request, X_HTTP_METHOD_OVERRRIDE);
        assertEquals(PATCH, value);

        verify(_connection).getAccessToken(false);
    }
}
