// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.request;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.restlet.RestletUtil;

import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.resource.Representation;

import java.security.GeneralSecurityException;

public class ResumableUploadRequestFactory extends GoogleBqRequestFactory {
    private static final String X_UPLOAD_CONTENT_TYPE = "X-Upload-Content-Type";

    public ResumableUploadRequestFactory(GoogleBqBaseConnection<? extends BrowseContext> connection, String endpoint,
            Representation metadata) {
        super(connection, Method.PUT, endpoint, metadata);
    }

    @Override
    public Request createRequest(int numAttempts) throws GeneralSecurityException {
        Request request = super.createRequest(numAttempts);
        RestletUtil.setHttpHeader(request, X_UPLOAD_CONTENT_TYPE, MediaType.ALL.getName());
        return request;
    }

}