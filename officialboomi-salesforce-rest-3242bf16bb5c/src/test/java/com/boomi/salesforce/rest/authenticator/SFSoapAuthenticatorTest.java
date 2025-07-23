// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.authenticator;

import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.request.AuthenticationRequestExecutor;
import com.boomi.salesforce.rest.request.RequestBuilder;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SFSoapAuthenticatorTest {

    private static final String EXPECTED_LOGIN_PAYLOAD =
            "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?><env:Envelope xmlns:env=\"http://schemas"
            + ".xmlsoap.org/soap/envelope/\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3"
            + ".org/2001/XMLSchema-instance\"> \n    <env:Body> \n"
            + "        <n1:login xmlns:n1=\"urn:partner.soap.sforce.com\">\n"
            + "           <n1:username>theUsername</n1:username>\n"
            + "           <n1:password>thePassword</n1:password>\n       </n1:login> \n   </env:Body>\n"
            + "</env:Envelope>";

    @Test
    void generateAccessToken() throws IOException {
        RequestBuilder builderMock = mock(RequestBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        AuthenticationRequestExecutor executorMock = mock(AuthenticationRequestExecutor.class, RETURNS_DEEP_STUBS);
        ConnectionProperties propertiesMock = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);

        when(propertiesMock.getUsername()).thenReturn("theUsername");
        when(propertiesMock.getPassword()).thenReturn("thePassword");

        ArgumentCaptor<HttpEntity> loginBodyCaptor = ArgumentCaptor.forClass(HttpEntity.class);

        when(builderMock.loginSoap(loginBodyCaptor.capture())).thenReturn(mock(ClassicHttpRequest.class));
        when(executorMock.executeAuthenticate(any()).getEntity().getContent()).thenReturn(
                new ByteArrayInputStream("<sessionId>theToken</sessionId>".getBytes(StringUtil.UTF8_CHARSET)));

        SFSoapAuthenticator authenticator = new SFSoapAuthenticator(builderMock, executorMock, propertiesMock);
        String accessToken = authenticator.getAccessToken();

        assertEquals("theToken", accessToken);
        HttpEntity capturedEntity = loginBodyCaptor.getValue();

        String loginPayload = StreamUtil.toString(capturedEntity.getContent(), StringUtil.UTF8_CHARSET);
        assertEquals(EXPECTED_LOGIN_PAYLOAD, loginPayload);

        verify(propertiesMock, times(1)).cacheSession("theToken");
    }

    @Test
    void getAccessTokenFromCache() {
        RequestBuilder builderMock = mock(RequestBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        AuthenticationRequestExecutor executorMock = mock(AuthenticationRequestExecutor.class, RETURNS_DEEP_STUBS);
        ConnectionProperties propertiesMock = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);

        when(propertiesMock.getSessionFromCache()).thenReturn("cachedToken");

        SFSoapAuthenticator authenticator = new SFSoapAuthenticator(builderMock, executorMock, propertiesMock);
        String accessToken = authenticator.getAccessToken();

        assertEquals("cachedToken", accessToken);
    }
}
