// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva;

import com.boomi.common.rest.RestClient;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.openapi.OpenAPIOperationConnection;
import com.boomi.connector.veeva.browser.VeevaOperationType;
import com.boomi.connector.veeva.retry.RetryableClient;
import com.boomi.connector.veeva.retry.UnauthorizedRetryStrategy;
import com.boomi.connector.veeva.util.OpenAPIAction;
import com.boomi.connector.veeva.vault.api.VaultApiHandler;
import com.boomi.connector.veeva.vault.api.VaultApiHandlerFactory;

import org.apache.http.entity.ContentType;

import java.net.URL;

public class VeevaOperationConnection extends OpenAPIOperationConnection implements VeevaBaseConnection {

    private final VaultApiHandler _vaultApiHandler;

    public VeevaOperationConnection(OperationContext context) {
        super(context);
        _vaultApiHandler = VaultApiHandlerFactory.create(context);
    }

    @Override
    public RestClient getClient() {
        return new RetryableClient(this, new UnauthorizedRetryStrategy());
    }

    /**
     * Returns the Veeva Vault API endpoint to interact with, including the API version in the path. The endpoint format
     * is https://{vault_domain_name}/api/{version}.
     *
     * @return the Veeva Vault API endpoint {@link URL}
     */
    @Override
    public URL getUrl() {
        return _vaultApiHandler.getVaultApiEndpoint();
    }

    /**
     * Returns a Session ID from the connector cache.
     *
     * @return string representing a Veeva Vault session
     */
    @Override
    public String getSessionIdFromCache() {
        return _vaultApiHandler.getSessionIdFromCache(getContext());
    }

    /**
     * Returns a key used to store Session IDs in the connector cache.
     *
     * @return string representing a key to cache Veeva Vault sessions
     */
    @Override
    public String getSessionCacheKey() {
        return _vaultApiHandler.getSessionCacheKey();
    }

    /**
     * Gets the appropriate HTTP method
     *
     * @return the http method
     */
    @Override
    public String getHttpMethod() {
        VeevaOperationType operationType = VeevaOperationType.from(getContext());
        if (operationType == VeevaOperationType.EXECUTE) {
            OpenAPIAction action = new OpenAPIAction(getContext().getObjectTypeId());
            return action.getMethod().name();
        }

        return super.getHttpMethod();
    }

    /**
     * Retrieve the content type for this operation. For OpenAPI operations, delegates to the super implementation in
     * OpenAPIOperationConnection. For other operations, the content type is application/json
     *
     * @return the content-type
     */
    @Override
    public ContentType getEntityContentType() {
        if (isOpenApiOperation()) {
            return super.getEntityContentType();
        }
        return ContentType.APPLICATION_JSON;
    }

    /**
     * Return true if the operation is an action loaded from the OpenAPI yaml spec, false otherwise
     *
     * @return true if the operation is an OpenAPI action, false otherwise
     */
    public boolean isOpenApiOperation() {
        return getContext().getObjectTypeId().contains("/");
    }
}
