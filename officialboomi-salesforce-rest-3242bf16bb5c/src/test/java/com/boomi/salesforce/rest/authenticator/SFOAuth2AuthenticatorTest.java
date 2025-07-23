// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.authenticator;

import com.boomi.connector.api.OAuth2Context;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SFOAuth2AuthenticatorTest {

    @Test
    void getAccessToken() throws IOException {
        OAuth2Context context = mock(OAuth2Context.class, Mockito.RETURNS_DEEP_STUBS);

        SFOAuth2Authenticator authenticator = new SFOAuth2Authenticator(context);
        authenticator.getAccessToken();

        verify(context, times(1)).getOAuth2Token(false);
    }

    @Test
    void generateAccessToken() throws IOException {
        OAuth2Context context = mock(OAuth2Context.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getOAuth2Token(false).getAccessToken()).thenReturn("oldToken");
        when(context.getOAuth2Token(true).getAccessToken()).thenReturn("newToken");

        SFOAuth2Authenticator authenticator = new SFOAuth2Authenticator(context);
        String token = authenticator.generateAccessToken();

        assertEquals("newToken", token);

        verify(context, atLeast(1)).getOAuth2Token(true);
    }
}
