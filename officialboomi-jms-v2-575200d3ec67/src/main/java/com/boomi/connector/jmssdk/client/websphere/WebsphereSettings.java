// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.util.EqualsBuilder;
import com.boomi.util.HashCodeBuilder;

/**
 * Value object holding the connection configuration related to Websphere.
 * <p>
 * This class overrides {@link #equals(Object)} & {@link #hashCode()} methods: two instances are considered equal if
 * they shared the same values for all their fields.
 */
public class WebsphereSettings {

    private static final String CUSTOM_SSL_SUITE_VALUE = "CUSTOM_SSL_SUITE";
    private final String _hostName;
    private final String _hostList;
    private final int _hostPort;
    private final String _queueManager;
    private final String _channel;
    private final boolean _isMultiInstance;
    private final boolean _useSsl;
    private final String _sSLCipherSuite;

    /**
     * Construct a WebsphereSettings instance from the given connection properties and server type.
     *
     * @param properties containing the connection settings
     * @param serverType indicating if the service is single or multi instance
     */
    public WebsphereSettings(PropertyMap properties, JMSConstants.ServerType serverType) {
        _isMultiInstance = (serverType == JMSConstants.ServerType.WEBSPHERE_MQ_MULTI_INSTANCE);
        _hostName = properties.getProperty(JMSConstants.WEBSPHERE_HOST_NAME);
        _hostPort = properties.getLongProperty(JMSConstants.WEBSPHERE_HOST_PORT, 0L).intValue();
        _queueManager = properties.getProperty(JMSConstants.WEBSPHERE_QUEUE_MANAGER);
        _channel = properties.getProperty(JMSConstants.WEBSPHERE_CHANNEL);
        _hostList = properties.getProperty(JMSConstants.WEBSPHERE_HOST_LIST);
        _useSsl = properties.getBooleanProperty(JMSConstants.WEBSPHERE_USE_SSL, false);
        _sSLCipherSuite = getSslCipherSuiteValue(properties);
    }

    /**
     * Extract the SSL Cipher Suite from the given properties. If the select option is 'CUSTOM_SSL_SUITE', the custom
     * SSL Suite is returned
     *
     * @param properties to extract the SSL Cipher Suite
     * @return the SSL Cipher Suite
     */
    private static String getSslCipherSuiteValue(PropertyMap properties) {
        String sslCipherSuite = properties.getProperty(JMSConstants.WEBSPHERE_SSL_SUITE_OPTION);
        return CUSTOM_SSL_SUITE_VALUE.equals(sslCipherSuite) ? properties.getProperty(
                JMSConstants.WEBSPHERE_SSL_SUITE_TEXT) : sslCipherSuite;
    }

    /**
     * Get the hostname, or null if the server type is multi instance
     *
     * @return the hostname
     */
    public String getHostName() {
        return _hostName;
    }

    /**
     * Get the host list, or null if the server type is single instance
     *
     * @return the host list
     */
    public String getHostList() {
        return _hostList;
    }

    /**
     * Get the port, or 0 if the server type is multi instance
     *
     * @return the port
     */
    public int getHostPort() {
        return _hostPort;
    }

    /**
     * Get the queue manager name
     *
     * @return the queue manager name
     */
    public String getQueueManager() {
        return _queueManager;
    }

    /**
     * Get the channel name
     *
     * @return the channel name
     */
    public String getChannel() {
        return _channel;
    }

    /**
     * Check if the server type is multi instance
     *
     * @return true if the server type is multi instance, false otherwise
     */
    public boolean isMultiInstance() {
        return _isMultiInstance;
    }

    /**
     * Check if SSL is enabled
     *
     * @return true if SSL is enabled, false otherwise
     */
    public boolean useSsl() {
        return _useSsl;
    }

    /**
     * Get the SSL Cipher Suite name, or null if SSL is not enabled
     *
     * @return the SSL Cipher Suite
     */
    public String getSslCipherSuite() {
        return _sSLCipherSuite;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(_hostName).append(_hostList).append(_hostPort).append(_queueManager).append(
                _channel).append(_isMultiInstance).append(_useSsl).append(_sSLCipherSuite).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof WebsphereSettings)) {
            return false;
        }
        WebsphereSettings other = (WebsphereSettings) o;

        return new EqualsBuilder().append(_hostName, other._hostName).append(_hostList, other._hostList).append(
                        _hostPort, other._hostPort).append(_queueManager, other._queueManager).append(_channel,
                        other._channel)
                .append(_isMultiInstance, other._isMultiInstance).append(_useSsl, other._useSsl).append(_sSLCipherSuite,
                        other._sSLCipherSuite).isEquals();
    }
}
