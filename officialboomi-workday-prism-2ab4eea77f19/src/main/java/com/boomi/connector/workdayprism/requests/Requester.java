//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.requests;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.responses.ErrorResponse;
import com.boomi.connector.workdayprism.utils.HttpStatusUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.URLUtil;
import com.boomi.util.retry.RetryStrategy;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

/**
 * Abstract base class to define and execute request to the Workday Prism API
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public abstract class Requester {
    private static final String ERROR_NULL_ENTITY = "invalid response entity";
    private static final String ERROR_DESERIALIZING_ENTITY = "error deserializing entity";

    /**
     * HttpClientBuilder instance configured to use system properties for SSL settings.
     * Ensures that the HTTP client aligns with the JVM and environment's SSL configurations,
     * promoting better security and compatibility.
     * </p>
     *
     * @see HttpClientBuilder#useSystemProperties()
     */
    private static final HttpClientBuilder HTTP_CLIENT_BUILDER = HttpClientBuilder.create()
            .useSystemProperties()
            .disableAutomaticRetries();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

    private final String basePath;
    private final RetryStrategy retryStrategy;
    final RequestBuilder builder;
    private ObjectData objectData;
    private InputStream objectDataStream;
        

    /**
     * Creates a new {@link Requester} instance
     *
     * @param httpMethod
     *         the http method as a String
     * @param basePath
     *         the base url for the destination API.
     * @param retryStrategy
     *         a {@link RetryStrategy} instance
     */
    Requester(String httpMethod, String basePath, RetryStrategy retryStrategy) {
        this.builder = RequestBuilder.create(httpMethod);
        this.retryStrategy = retryStrategy;
        this.basePath = basePath;
    }

    private static <T> T parseResponse(InputStream content, Class<T> type) {
        try {
            return OBJECT_MAPPER.readValue(content, type);
        }
        catch (Exception e) {
            throw new ConnectorException(ERROR_DESERIALIZING_ENTITY, e);
        }
    }

    public Requester setEndpoint(String endpoint) {
        builder.setUri(URLUtil.makeUrlString(basePath, endpoint));
        return this;
    }

    Requester setAuthorizationHeaders(String pattern, String authorization) {
        builder.addHeader(HttpHeaders.AUTHORIZATION, String.format(pattern, authorization));
        return this;
    }

    public Requester setContentType(ContentType contentType) {
        builder.addHeader(HttpHeaders.CONTENT_TYPE, contentType.toString());
        return this;
    }

    private void setBody(InputStream body) {
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(body);
        builder.setEntity(entity);
    }

    public Requester setBody(ObjectData body) {
        objectData = body;
        return this;
    }

    public Requester setBody(JsonNode body) {
        StringEntity entity = new StringEntity(body.toString(), ContentType.APPLICATION_JSON);
        builder.setEntity(entity);
        closeResources();
        objectData = null;
        return this;
    }

    /**
     * Executes requests to the destination API.
     * Depending on the {@link RetryStrategy} passed to the construction of this class, this method will
     * retry the request, setting a new authorization header every time.
     *
     * @param responseType
     *         specifies the expected response type.
     * @return an instance of the type provided as a parameter.
     * @throws IOException
     *         if there's a communication problem while the request is executed.
     * @throws ConnectorException
     *         if there the response does not matches the expected status and content.
     */
    public <T> T doRequest(Class<T> responseType) throws IOException {
        CloseableHttpResponse response = null;
        InputStream content = null;

        try {
            response = executeRequest();

            HttpEntity entity = response.getEntity();
            if (entity == null) {
                throw new ConnectorException(ERROR_NULL_ENTITY);
            }

            content = entity.getContent();

            StatusLine status = response.getStatusLine();
            if (!HttpStatusUtils.isSuccess(status)) {
                ErrorResponse error = parseResponse(content, ErrorResponse.class);
                throw new ConnectorException(status.getReasonPhrase(), error.getError());
            }

            return parseResponse(content, responseType);
        }
        finally {
            IOUtil.closeQuietly(response, content);
        }
    }
    

    /**
     * Executes requests to the destination API.
     * Depending on the {@link RetryStrategy} passed to the construction of this class, this method will
     * retry the request, setting a new authorization header every time.
     *
     * @return a {@link PrismResponse} instance.
     * @throws IOException
     *         if there's a communication problem while the request is executed.
     */
    public PrismResponse doRequest() throws IOException {
        return new PrismResponse(executeRequest());
    }

    public CloseableHttpResponse executeRequest() throws IOException {
        int attempts = 0;
        CloseableHttpResponse response = null;
        try {
            do {
                IOUtil.closeQuietly(response);
                closeResources();
                prepareRequest(attempts);
                attempts++;
                response = HTTP_CLIENT_BUILDER.build().execute(builder.build());
            } while (retryStrategy.shouldRetry(attempts, response));
        }
        catch (Exception e) {
            IOUtil.closeQuietly(response);
            throw e;
        }
        finally {
            closeResources();
        }

        return response;
    }

    abstract void setAuthorization(boolean forceRefresh);

    private void closeResources() {
        IOUtil.closeQuietly(objectDataStream);
        objectDataStream = null;
    }

    void prepareRequest(int attempt) {
        if(objectData != null) {
            objectDataStream = objectData.getData();
            setBody(objectDataStream);
        }
        setAuthorization(attempt > 0);
    }
    
}