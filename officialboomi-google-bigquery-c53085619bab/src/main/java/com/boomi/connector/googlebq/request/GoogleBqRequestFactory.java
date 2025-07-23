// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.googlebq.request;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.restlet.auth.AuthorizationProvider;
import com.boomi.restlet.auth.BearerAuthorizationProvider;
import com.boomi.restlet.client.RequestFactory;
import com.boomi.restlet.client.RequestUtil;

import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.resource.Representation;

import java.security.GeneralSecurityException;

/**
 *
 */
public class GoogleBqRequestFactory extends RequestFactory {

    private final Method _method;
    private final String _endpoint;
    private final GoogleBqBaseConnection<? extends BrowseContext> _connection;
    private final Representation _representation;

    public GoogleBqRequestFactory(GoogleBqBaseConnection<? extends BrowseContext> connection, Method method,
            String endpoint) {
        this(connection, method, endpoint, null);
    }

    public GoogleBqRequestFactory(GoogleBqBaseConnection<? extends BrowseContext> connection, Method method,
            String endpoint, Representation representation) {
        _endpoint = endpoint;
        _method = method;
        _connection = connection;
        _representation = representation;
    }

    @Override
    public Request createRequest(int numAttempts) throws GeneralSecurityException {
        //We only want to force an access token refresh ONLY if it's the first retry to get a new working token.
        String accessToken = _connection.getAccessToken(numAttempts == 1);

        AuthorizationProvider authProvider = new BearerAuthorizationProvider(accessToken);
        return RequestUtil.formatRequest(_method, _endpoint, _representation, authProvider);
    }
}
