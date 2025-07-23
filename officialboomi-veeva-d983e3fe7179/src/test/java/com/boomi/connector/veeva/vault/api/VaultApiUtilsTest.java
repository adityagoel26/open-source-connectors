// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.vault.api;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VaultApiUtilsTest {

    private PropertyMap _connectionProperties;

    @BeforeEach
    void setup() {
        _connectionProperties = new MutablePropertyMap();
    }

    @Test
    void validateNullUser() {
        _connectionProperties.put("username", null);
        Executable executable = () -> VaultApiUtils.username(_connectionProperties);
        assertEquals("Username is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateKeyEmptyUser() {
        _connectionProperties.put("username", " ");
        Executable executable = () -> VaultApiUtils.username(_connectionProperties);
        assertEquals("Username is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateNullPassword() {
        _connectionProperties.put("password", null);
        Executable executable = () -> VaultApiUtils.password(_connectionProperties);
        assertEquals("Password is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateEmptyPassword() {
        _connectionProperties.put("password", " ");
        Executable executable = () -> VaultApiUtils.password(_connectionProperties);
        assertEquals("Password is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateNullVaultDNS() {
        _connectionProperties.put("vaultDNS", null);
        Executable executable = () -> VaultApiUtils.vaultDNS(_connectionProperties);
        assertEquals("Vault DNS is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateEmptyVaultDNS() {
        _connectionProperties.put("vaultDNS", " ");
        Executable executable = () -> VaultApiUtils.vaultDNS(_connectionProperties);
        assertEquals("Vault DNS is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateNullVaultSubdomain() {
        _connectionProperties.put("vaultSubdomain", null);
        Executable executable = () -> VaultApiUtils.vaultSubdomain(_connectionProperties);
        assertEquals("Vault Subdomain is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateEmptyVaultSubdomain() {
        _connectionProperties.put("vaultSubdomain", " ");
        Executable executable = () -> VaultApiUtils.vaultSubdomain(_connectionProperties);
        assertEquals("Vault Subdomain is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateExceptionWhenNullAndNotOverrideableAPIVersion() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", false);
        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("apiVersion", null);

        Executable executable = () -> VaultApiUtils.apiVersion(connectionProperties, operationProperties);
        assertEquals("API Version is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateExceptionWhenEmptyAndNotOverrideableAPIVersion() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", false);
        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("apiVersion", " ");

        Executable executable = () -> VaultApiUtils.apiVersion(connectionProperties, operationProperties);
        assertEquals("API Version is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateNullAndOverrideableAPIVersion() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", true);
        operationProperties.put("operationApiVersion", null);
        _connectionProperties.put("apiVersion", null);
        Executable executable = () -> VaultApiUtils.apiVersion(_connectionProperties, operationProperties);
        assertEquals("API Version is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateEmptyAndOverrideableAPIVersion() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", true);
        operationProperties.put("operationApiVersion", " ");
        _connectionProperties.put("apiVersion", " ");

        Executable executable = () -> VaultApiUtils.apiVersion(_connectionProperties, operationProperties);
        assertEquals("API Version is a required field.", getThrowableMessage(executable));
    }

    @Test
    void validateAPIVersionOperation() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", true);
        operationProperties.put("operationApiVersion", "23.3");
        _connectionProperties.put("apiVersion", StringUtil.EMPTY_STRING);
        assertEquals("23.3", VaultApiUtils.apiVersion(_connectionProperties, operationProperties));
    }

    @Test
    void validateAPIVersionConnection() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", true);
        operationProperties.put("operationApiVersion", null);
        _connectionProperties.put("apiVersion", "21.1");
        assertEquals("21.1", VaultApiUtils.apiVersion(_connectionProperties, operationProperties));
    }

    @Test
    void validateAPIVersionOperationAndConnectionWhenOverrideable() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", true);
        operationProperties.put("operationApiVersion", "23.3");
        _connectionProperties.put("apiVersion", "21.1");
        assertEquals("23.3", VaultApiUtils.apiVersion(_connectionProperties, operationProperties));
    }

    @Test
    void validateAPIVersionOperationAndConnectionWhenNotOverrideable() {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", false);
        operationProperties.put("operationApiVersion", "23.3");
        _connectionProperties.put("apiVersion", "21.1");
        assertEquals("21.1", VaultApiUtils.apiVersion(_connectionProperties, operationProperties));
    }

    @Test
    void validateMinimumTimeout() {
        _connectionProperties.put("sessionTimeout", 9L);
        Executable executable = () -> VaultApiUtils.timeout(_connectionProperties);
        assertEquals("The minimum session timeout is 10 minutes.", getThrowableMessage(executable));
    }

    @Test
    void validateMaximumTimeout() {
        _connectionProperties.put("sessionTimeout", 481L);
        Executable executable = () -> VaultApiUtils.timeout(_connectionProperties);
        assertEquals("The maximum session timeout is 480 minutes (8 hours).", getThrowableMessage(executable));
    }

    @Test
    void validateNullOAuth2Context() {
        _connectionProperties.put("veevaOauth", null);
        Executable executable = () -> VaultApiUtils.oAuth2Context(_connectionProperties);
        assertEquals("OAuth2 context is not available.", getThrowableMessage(executable));
    }

    private String getThrowableMessage(Executable executable) {
        Throwable t = Assertions.assertThrows(IllegalArgumentException.class, executable);
        return t.getMessage();
    }
}
