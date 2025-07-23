// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.pool.AdapterPoolSettings;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.util.BaseConnection;

/**
 * BaseConnection implementation for JMS Connector
 *
 * @param <C> indicating the Context type being held by this connection
 */
public class JMSConnection<C extends BrowseContext> extends BaseConnection<C> {

    private final AdapterSettings _adapterSettings;

    public JMSConnection(C context) {
        super(context);
        PropertyMap connectionProperties = context.getConnectionProperties();
        _adapterSettings = new AdapterSettings(connectionProperties, new AdapterPoolSettings(connectionProperties));
    }

    public String getBrowseFilter() {
        return getContext().getOperationProperties().getProperty(JMSConstants.OP_PROP_ENTITY_FILTER);
    }

    public AdapterSettings getAdapterSettings() {
        return _adapterSettings;
    }

    public JMSConstants.JMSVersion getJMSVersion() {
        return _adapterSettings.getJmsVersion();
    }
}
