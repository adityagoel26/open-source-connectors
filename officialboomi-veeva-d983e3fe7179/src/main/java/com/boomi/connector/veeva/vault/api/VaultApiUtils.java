// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.vault.api;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.PropertyMap;
import com.boomi.util.StringUtil;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Utility class to extract, validate and transform the Connection properties to interact with the Veeva Vault API.
 */
class VaultApiUtils {

    private static final long DEFAULT_TIMEOUT_MINUTES = 10L;
    private static final long MIN_TIMEOUT_MINUTES = 10L;
    private static final long MAX_TIMEOUT_MINUTES = 8L * 60L;

    private VaultApiUtils() {
    }

    /**
     * Creates a URL object from the String representation. If the URL is not valid, it throws a connector exception.
     *
     * @param urlString the String to parse as a URL
     * @return a {@link URL} instance
     */
    static URL getUrlOf(String urlString) {
        try {
            return new URL(urlString);
        } catch (MalformedURLException e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * Extracts the Veeva Vault API Version. If 'Override API Version' is checked, the value from the operation
     * properties is considered and has precedence over the value from the connection properties. If the value is not
     * provided, an exception is thrown.</p>
     *
     * @param connectionProperties to extract the API Version from the connection
     * @param operationProperties  to extract the API Version from the operation
     * @return a string representing an API Version or throws an exception
     */
    static String apiVersion(PropertyMap connectionProperties, PropertyMap operationProperties) {
        String connectionApiVersion = connectionProperties.getProperty("apiVersion", StringUtil.EMPTY_STRING);

        if (operationProperties.getBooleanProperty("overrideApiVersion", false)) {
            return validateNonBlankApiVersion(
                    operationProperties.getProperty("operationApiVersion", connectionApiVersion));
        }

        return validateNonBlankApiVersion(connectionApiVersion);
    }

    static long timeout(PropertyMap connectionProperties) {
        long timeout = connectionProperties.getLongProperty("sessionTimeout", DEFAULT_TIMEOUT_MINUTES);
        if (timeout < MIN_TIMEOUT_MINUTES) {
            throw new IllegalArgumentException("The minimum session timeout is 10 minutes.");
        }
        if (timeout > MAX_TIMEOUT_MINUTES) {
            throw new IllegalArgumentException("The maximum session timeout is 480 minutes (8 hours).");
        }
        return timeout;
    }

    static OAuth2Context oAuth2Context(PropertyMap connectionProperties) {
        OAuth2Context oAuth2Context = connectionProperties.getOAuth2Context("veevaOauth");
        if (oAuth2Context == null) {
            throw new IllegalArgumentException("OAuth2 context is not available.");
        }
        return oAuth2Context;
    }

    static String username(PropertyMap connectionProperties) {
        return validateNonBlank(connectionProperties, "username", "Username");
    }

    static String password(PropertyMap connectionProperties) {
        return validateNonBlank(connectionProperties, "password", "Password");
    }

    static String profileId(PropertyMap connectionProperties) {
        return validateNonBlank(connectionProperties, "oathProfileId", "Profile Id");
    }

    static String vaultDNS(PropertyMap connectionProperties) {
        return validateNonBlank(connectionProperties, "vaultDNS", "Vault DNS");
    }

    static String authServerClientApplicationId(PropertyMap connectionProperties) {
        // optional field
        return connectionProperties.getProperty("authServerClientApplicationId", StringUtil.EMPTY_STRING);
    }

    static String vaultSubdomain(PropertyMap connectionProperties) {
        return validateNonBlank(connectionProperties, "vaultSubdomain", "Vault Subdomain");
    }

    private static String validateNonBlankApiVersion(String apiVersion) {
        if (StringUtil.isBlank(apiVersion)) {
            throw new IllegalArgumentException("API Version is a required field.");
        }
        return apiVersion;
    }

    private static String validateNonBlank(PropertyMap properties, String propertyId, String propertyLabel) {
        String property = properties.getProperty(propertyId);
        if (StringUtil.isBlank(property)) {
            throw new IllegalArgumentException(String.format("%s is a required field.", propertyLabel));
        }
        return property;
    }
}
