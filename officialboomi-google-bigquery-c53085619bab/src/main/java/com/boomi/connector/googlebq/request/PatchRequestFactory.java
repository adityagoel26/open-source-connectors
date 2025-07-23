// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.request;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.restlet.RestletUtil;

import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.resource.Representation;

import java.security.GeneralSecurityException;

public class PatchRequestFactory extends GoogleBqRequestFactory {
    private static final String X_HTTP_METHOD_OVERRRIDE = "X-HTTP-Method-Override";
    private static final String PATCH = "PATCH";

    public PatchRequestFactory(GoogleBqBaseConnection<? extends BrowseContext> connection, String endpoint,
            Representation representation) {
        super(connection, Method.POST, endpoint, representation);
    }

    @Override
    public Request createRequest(int numAttempts) throws GeneralSecurityException {
        Request request = super.createRequest(numAttempts);
        RestletUtil.setHttpHeader(request, X_HTTP_METHOD_OVERRRIDE, PATCH);
        return request;
    }
}
