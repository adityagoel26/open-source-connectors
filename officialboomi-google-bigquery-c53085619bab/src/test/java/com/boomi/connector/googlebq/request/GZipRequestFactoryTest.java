// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.request;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;

import org.junit.Test;
import org.restlet.data.ClientInfo;
import org.restlet.data.Encoding;
import org.restlet.data.Method;
import org.restlet.data.Preference;
import org.restlet.data.Request;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GZipRequestFactoryTest {
    private static final String PARAM_USER_AGENT_VALUE = "Boomi (gzip)";
    private static final String ENDPOINT = "test_endpoint";
    private static final String ACCESS_TOKEN = "access_token";
    private static final int ATTEMPTS = 2;

    private final GoogleBqBaseConnection<BrowseContext> _connection = mock(GoogleBqBaseConnection.class);

    private final GZipRequestFactory _requestFactory = new GZipRequestFactory(_connection, Method.GET, ENDPOINT);

    @Test
    public void shouldCreateRequest() throws Exception{
        when(_connection.getAccessToken(false)).thenReturn(ACCESS_TOKEN);

        Request request = _requestFactory.createRequest(ATTEMPTS);
        assertNotNull(request);
        ClientInfo info = request.getClientInfo();
        assertNotNull(info);
        assertEquals(PARAM_USER_AGENT_VALUE, info.getAgent());
        List<Preference<Encoding>> encoding = info.getAcceptedEncodings();
        assertNotNull(encoding);
        assertEquals(Encoding.GZIP, encoding.get(0).getMetadata());

        verify(_connection).getAccessToken(false);
    }

}
