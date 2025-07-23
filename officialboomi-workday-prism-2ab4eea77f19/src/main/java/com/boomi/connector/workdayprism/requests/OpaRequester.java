//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.requests;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.workdayprism.model.AuthProvider;
import com.boomi.connector.workdayprism.operations.upload.multipart.FilePart;
import com.boomi.connector.workdayprism.operations.upload.multipart.GzipFilePartWrapper;
import com.boomi.connector.workdayprism.operations.upload.multipart.MultiPartEntity;
import com.boomi.connector.workdayprism.operations.upload.multipart.Part;
import org.apache.http.client.methods.HttpPost;
import java.io.IOException;
import java.util.Collections;

/**
 * An implementation of {@link Requester} aimed to make request to the different endpoints of the opa API.
 * It will set the url path and ensure that a Bearer Authorization token is included into the request.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class OpaRequester extends AuthorizedRequester {
    private static final String BASE_PATH = "wday/opa/tenant";
    private static final String CONTENT_BOUNDARY = "BoomiFormBoundary";

    private final FilePart filePart;

    private OpaRequester(String httpMethod, int maxRetries, AuthProvider authProvider, FilePart filePart) {
        super(httpMethod, authProvider.getBasePath(BASE_PATH), maxRetries, authProvider);
        this.filePart = filePart;
    }

    /**
     * Static factory method to create and return a new {@link OpaRequester} instance
     * specifically designed for making POST requests to the opa endpoints.
     *
     * @param authProvider
     *         a {@link AuthProvider} instance
     * @param maxRetries
     *         an int value indicating how many retries are allowed before a failed request is returned.
     * @param filePart
     *         the {@link FilePart} to be sent as body in the request
     */
    public static Requester post(AuthProvider authProvider, int maxRetries, FilePart filePart) {
        return new OpaRequester(HttpPost.METHOD_NAME, maxRetries, authProvider, filePart);
    }

    @Override
    void prepareRequest(int attempt) {
        try {
            filePart.reset();
        }
        catch (IOException e) {
            throw new ConnectorException(e);
        }

        Part filePartInner = new GzipFilePartWrapper(filePart);
        MultiPartEntity entity = new MultiPartEntity(Collections.singleton(filePartInner), CONTENT_BOUNDARY);
        builder.setEntity(entity);
        setAuthorization(attempt > 0);
    }
}
