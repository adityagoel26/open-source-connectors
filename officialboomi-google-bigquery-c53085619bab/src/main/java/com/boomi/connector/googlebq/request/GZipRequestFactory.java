// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.request;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;

import org.restlet.data.Encoding;
import org.restlet.data.Method;
import org.restlet.data.Preference;
import org.restlet.data.Request;

import java.security.GeneralSecurityException;

public class GZipRequestFactory extends GoogleBqRequestFactory {

    private static final String PARAM_USER_AGENT_VALUE = "Boomi (gzip)";

    public GZipRequestFactory(GoogleBqBaseConnection<? extends BrowseContext> connection, Method method,
            String endpoint) {
        super(connection, method, endpoint);
    }

    @Override
    public Request createRequest(int numAttempts) throws GeneralSecurityException {
        Request request = super.createRequest(numAttempts);
        request.getClientInfo().setAgent(PARAM_USER_AGENT_VALUE);
        request.getClientInfo().getAcceptedEncodings().add(new Preference<>(Encoding.GZIP));
        return request;
    }
}
