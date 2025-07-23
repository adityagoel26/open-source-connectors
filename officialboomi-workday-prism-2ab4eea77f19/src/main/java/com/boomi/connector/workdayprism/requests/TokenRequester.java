//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.requests;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.workdayprism.model.Credentials;
import com.boomi.connector.workdayprism.responses.TokenResponse;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.retry.NeverRetry;

import org.apache.commons.codec.Charsets;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class to get requests to Workday Access Token API
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class TokenRequester extends Requester {
    private static final String BASE_ENDPOINT = "ccx/oauth2/";
    private static final String ENDPOINT_PATTERN = "token";
    private static final String GRANT_TYPE_KEY = "grant_type";
    private static final String REFRESH_TOKEN = "refresh_token";

    private static final BasicNameValuePair GRANT_FORM_PARAM = new BasicNameValuePair(GRANT_TYPE_KEY, REFRESH_TOKEN);

    private final Credentials credentials;

    /**
     * Creates a new {@link TokenRequester} instance
     *
     * @param credentials
     *         a {@link Credentials} instance
     */
    public TokenRequester(Credentials credentials) {
        super(HttpPost.METHOD_NAME, credentials.getBasePath(BASE_ENDPOINT), NeverRetry.INSTANCE);
        this.credentials = credentials;
    }

    /**
     * Executes a request to the given endpoint to get the access token
     *
     * @return the response from the service
     * @throws IOException
     *         if an error happens calling the endpoint or parsing the response
     * @throws ConnectorException
     *         if an error related to handling the response happens
     */
    public TokenResponse get() throws IOException {
        setUrlEncodedFormParams();
        return setEndpoint(ENDPOINT_PATTERN)
                .setContentType(ContentType.APPLICATION_FORM_URLENCODED)
                .doRequest(TokenResponse.class);
    }

    /**
     * Sets the Authorization Basic token.
     *
     * @param forceRefresh
     *         the Basic Authorization string is static and this argument won't have any effect.
     */
    @Override
    void setAuthorization(boolean forceRefresh){
        setAuthorizationHeaders(Constants.AUTHORIZATION_BASIC_PATTERN, credentials.getBasicAuth());
    }

    private void setUrlEncodedFormParams() {
        List<BasicNameValuePair> params = Arrays.asList(GRANT_FORM_PARAM,
                new BasicNameValuePair(REFRESH_TOKEN, credentials.getRefreshToken()));

        builder.setEntity(new UrlEncodedFormEntity(params, Charsets.UTF_8));
    }
}