// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.resource;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.operation.retry.GoogleBqRetryStrategy;
import com.boomi.connector.googlebq.request.GZipRequestFactory;
import com.boomi.connector.googlebq.request.GoogleBqRequestFactory;
import com.boomi.connector.googlebq.request.PatchRequestFactory;
import com.boomi.restlet.client.ClientUtil;
import com.boomi.restlet.client.RequestFactory;
import com.boomi.restlet.client.RequestUtil;
import com.boomi.restlet.resource.JsonRepresentation;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.retry.RetryStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.restlet.data.Method;
import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * This class represents a Google Bq Resource. Various Resources in Google Big Query are Datasets, Jobs, Projects,
 * Tabledata, Tables etc. Every API request in Google Big Query is against a resource. Methods that affect resources
 * include delete, get, insert, list, patch and update.
 * <p>
 * This class provides a base methods common to all resources. Methods such as executing requests are provided by this
 * class. The classes that extend this class i.e. the resource specific class should provide information to execute the
 * request. Information may include service path, request parameters, request body etc.
 */
public abstract class GoogleBqResource {

    private static final String NEXT_PAGE_TOKEN = "nextPageToken";
    private static final String PAGE_TOKEN = "pageToken";
    private static final String BASE_URL = "https://www.googleapis.com/bigquery/v2/";

    private GoogleBqBaseConnection<? extends BrowseContext> _connection;

    protected GoogleBqResource(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        _connection = connection;
    }

    /**
     * Generates a url which can be executed against Google Big Query REST API. {@link URLUtil#makeUrlString(Collection,
     * Object...)} will create a url by appending query parameters to BASE_URL + methodPath
     *
     * @param methodPath
     *         :   suffix path to execute a particular google api method
     * @param queryParameters
     *         :   query parameters to be added to the url
     * @return
     */
    private static String generateUrl(String methodPath, Map<String, String> queryParameters) {
        return URLUtil.makeUrlString(queryParameters.entrySet(), BASE_URL, methodPath);
    }

    /**
     * Executes a GET request by first generating the request url and then handling it.
     *
     * @param queryParameters
     * @param methodPath
     * @return
     * @throws IOException
     */
    protected Response executeGet(Map<String, String> queryParameters, String methodPath)
            throws GeneralSecurityException {
        RequestFactory requestFactory = new GoogleBqRequestFactory(_connection, Method.GET,
                generateUrl(methodPath, queryParameters));

        return executeRequest(requestFactory);
    }

    protected Response executeGet(String methodPath) throws GeneralSecurityException {
        return executeGet(Collections.<String, String>emptyMap(), methodPath);
    }

    /**
     * Executes a GET request with GZip compression by first generating the request url and then handling it.
     *
     * @param queryParameters
     *         the parameters to be included on the request
     * @param methodPath
     *         the url endpoint
     * @return a {@link Response } instance.
     * @throws GeneralSecurityException
     *         this should not really happen
     */
    protected Response executeGZipCompressedGet(Map<String, String> queryParameters, String methodPath)
            throws GeneralSecurityException {

        String url = generateUrl(methodPath, queryParameters);
        RequestFactory requestFactory = new GZipRequestFactory(_connection, Method.GET, url);

        return executeRequest(requestFactory);
    }

    /**
     * Executes a POST request by first generating the request url and then handling it.
     *
     * @param queryParameters
     *         the parameters to be included on the request.
     * @param methodPath
     *         the url endpoint.
     * @param body
     *         an {@link ObjectNode} to be used as a payload.
     * @param retryStrategy
     *         an instance of {@link RetryStrategy}.
     * @return a {@link Response}
     * @throws GeneralSecurityException
     *         this shouldn't never happen
     */
    protected Response executePost(Map<String, String> queryParameters, String methodPath, JsonNode body,
            RetryStrategy retryStrategy) throws GeneralSecurityException {

        RequestFactory requestFactory = new GoogleBqRequestFactory(_connection, Method.POST,
                generateUrl(methodPath, queryParameters), new JsonRepresentation(body));

        return executeRequest(requestFactory, retryStrategy);
    }

    protected Response executePost(String methodPath, JsonNode body) throws GeneralSecurityException {
        return executePost(Collections.<String, String>emptyMap(), methodPath, body, null);
    }

    protected Response executePost(String methodPath, JsonNode body, RetryStrategy retryStrategy)
            throws GeneralSecurityException {
        return executePost(Collections.<String, String>emptyMap(), methodPath, body, retryStrategy);
    }

    /**
     * Executes a PUT request.
     *
     * @param endpoint
     *         the selected Bigquery endpoint to make the request.
     * @param body
     *         an {@link ObjectNode} to be used as a payload.
     * @return a {@link Response} instance.
     * @throws GeneralSecurityException
     *         this shouldn't never happen
     */
    Response executePut(String endpoint, JsonNode body) throws GeneralSecurityException {
        String url = URLUtil.makeUrlString(BASE_URL, endpoint);

        RequestFactory requestFactory = new GoogleBqRequestFactory(_connection, Method.PUT, url,
                new JsonRepresentation(body));

        return executeRequest(requestFactory);
    }

    /**
     * Executes a DELETE request by first generating the request url and then handling it.
     * @param queryParameters
     * @param methodPath
     * @return
     * @throws GeneralSecurityException
     */
    protected Response executeDelete(Map<String, String> queryParameters, String methodPath)
            throws GeneralSecurityException {
        RequestFactory requestFactory = new GoogleBqRequestFactory(_connection, Method.DELETE,
                generateUrl(methodPath, queryParameters));

        return executeRequest(requestFactory);
    }

    /**
     * Executes a PATCH request.
     * <p>
     * HttpUrlConnection doesn't allows the use of the PATCH method but Google Cloud supports the use of the
     * "X-HTTP-Method-Override" header. Based on that this request will be a POST with the aforementioned header set as
     * PATCH by the use of a custom PatchAuthorizationProvider.
     *
     * @param endpoint
     *         the selected Bigquery endpoint to make the request.
     * @param body
     *         an {@link ObjectNode} to be used as a payload.
     * @return a {@link Response} instance.
     * @throws GeneralSecurityException
     *         this shouldn't never happen
     */
    Response executePatch(String endpoint, JsonNode body) throws GeneralSecurityException {
        String url = URLUtil.makeUrlString(BASE_URL, endpoint);
        RequestFactory requestFactory = new PatchRequestFactory(_connection, url, new JsonRepresentation(body));

        return executeRequest(requestFactory);
    }

    protected static Response executeRequest(RequestFactory requestFactory) throws GeneralSecurityException {
        return executeRequest(requestFactory, null);
    }

    protected static Response executeRequest(RequestFactory requestFactory, RetryStrategy retryStrategy)
            throws GeneralSecurityException {
        return ClientUtil.handle(RequestUtil.newClient(), requestFactory, new GoogleBqRetryStrategy(retryStrategy));
    }

    /**
     * Returns nextPageToken in a given {@link JsonNode}. nextPageToken is used to request the next page of results.
     * Returns null if there is no nextPageToken present. Default maxResults value is 50 except for Tabledata.list api.
     * https://cloud.google.com/bigquery/docs/paging-results
     *
     * @param node
     * @return
     */
    protected String getNextPageToken(JsonNode node) {
        return node.path(NEXT_PAGE_TOKEN).asText();
    }

    /**
     * Adds the pageToken to the given properties map. The map can later be used to build a request url having a
     * pageToken parameter. This will request the next page of results. If the pageToken is empty it is not added to the
     * properties map.
     *
     * @param properties
     * @param pageToken
     */
    protected void addPageToken(Map<String, String> properties, String pageToken) {
        if (StringUtil.isNotEmpty(pageToken)) {
            properties.put(PAGE_TOKEN, pageToken);
        }
    }
}
