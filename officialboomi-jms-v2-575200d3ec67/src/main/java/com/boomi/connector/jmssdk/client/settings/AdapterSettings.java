// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client.settings;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.jmssdk.client.websphere.WebsphereSettings;
import com.boomi.connector.jmssdk.pool.AdapterPoolSettings;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.util.EqualsBuilder;
import com.boomi.util.HashCodeBuilder;
import com.boomi.util.StringUtil;

import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.Map;

/**
 * Value object holding the configuration related to the Adapter.
 * <p>
 * This class overrides {@link #equals(Object)} & {@link #hashCode()} methods: two instances are considered equal if
 * they shared the same values for all their fields.
 */
public class AdapterSettings {

    private final AdapterPoolSettings _poolSettings;
    private final JMSConstants.JMSVersion _jmsVersion;
    private final JMSConstants.ServerType _serverType;
    private final boolean _authentication;
    private final String _username;
    private final String _password;
    private final String _jndiLookupFactory;
    private final String _initialContextFactory;
    private final String _providerUrl;
    private final String _jdbcUrl;
    private final Map<String, String> _jmsProperties;
    private final WebsphereSettings _websphereSettings;

    public AdapterSettings(PropertyMap properties, AdapterPoolSettings poolSettings) {
        _poolSettings = poolSettings;

        _jmsVersion = JMSConstants.JMSVersion.valueOf(properties.getProperty(JMSConstants.PROPERTY_JMS_VERSION));
        _serverType = JMSConstants.ServerType.valueOf(properties.getProperty(JMSConstants.PROPERTY_SERVER_TYPE));

        _authentication = properties.getBooleanProperty(JMSConstants.PROPERTY_USE_AUTHENTICATION, false);
        _username = _authentication ? properties.getProperty(JMSConstants.PROPERTY_USERNAME, StringUtil.EMPTY_STRING)
                : StringUtil.EMPTY_STRING;
        _password = _authentication ? properties.getProperty(JMSConstants.PROPERTY_PASSWORD, StringUtil.EMPTY_STRING)
                : StringUtil.EMPTY_STRING;

        _jndiLookupFactory = properties.getProperty(JMSConstants.PROPERTY_JNDI_LOOKUP_FACTORY);
        _initialContextFactory = properties.getProperty(JMSConstants.PROPERTY_INITIAL_CONTEXT_FACTORY);
        _providerUrl = properties.getProperty(JMSConstants.PROPERTY_PROVIDER_URL);
        _jdbcUrl = properties.getProperty(JMSConstants.PROPERTY_JDBC_URL);
        _jmsProperties = properties.getCustomProperties(JMSConstants.PROPERTY_JMS_PROPERTIES);
        _websphereSettings = new WebsphereSettings(properties, _serverType);
    }

    public JMSConstants.JMSVersion getJmsVersion() {
        return _jmsVersion;
    }

    public JMSConstants.ServerType getServerType() {
        return _serverType;
    }

    public boolean useAuthentication() {
        return _authentication;
    }

    public String getUsername() {
        return _username;
    }

    public String getPassword() {
        return _password;
    }

    public String getJndiLookupFactory() {
        return _jndiLookupFactory;
    }

    public String getInitialContextFactory() {
        return _initialContextFactory;
    }

    public String getProviderUrl() {
        return _providerUrl;
    }

    public String getJdbcUrl() {
        return _jdbcUrl;
    }

    public Map<String, String> getJmsProperties() {
        return _jmsProperties;
    }

    public boolean isPoolEnabled() {
        return _poolSettings.isEnabled();
    }

    public GenericObjectPool.Config getPoolConfig() {
        return _poolSettings.toConfig();
    }

    public WebsphereSettings getWebsphereSettings() {
        return _websphereSettings;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(_poolSettings).append(_jmsVersion).append(_serverType)
                .append(_jndiLookupFactory).append(_initialContextFactory).append(_providerUrl).append(_jmsProperties);

        if (_authentication) {
            builder.append(_username).append(_password);
        }

        return builder.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof AdapterSettings)) {
            return false;
        }

        AdapterSettings other = (AdapterSettings) o;
        EqualsBuilder equalsBuilder = new EqualsBuilder().append(_poolSettings, other._poolSettings).append(_jmsVersion,
                other._jmsVersion).append(_serverType, other._serverType).append(_jndiLookupFactory,
                other._jndiLookupFactory).append(_initialContextFactory, other._initialContextFactory).append(
                _providerUrl, other._providerUrl).append(_jmsProperties, other._jmsProperties);

        if (_authentication) {
            equalsBuilder.append(_username, other._username).append(_password, other._password);
        }

        return equalsBuilder.isEquals();
    }
}
