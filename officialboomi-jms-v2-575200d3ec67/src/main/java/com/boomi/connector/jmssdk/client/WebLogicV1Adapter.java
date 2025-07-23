// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.NamingException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class WebLogicV1Adapter extends GenericJndiJmsV1Adapter {

    private static final Logger LOG = LogUtil.getLogger(WebLogicV1Adapter.class);

    private ThreadLocal<JndiContext> _jndiContext;

    public WebLogicV1Adapter(AdapterSettings settings) {
        super(settings);
    }

    @Override
    protected ConnectionFactory createConnectionFactory() {
        try {
            return getJndiContext().getConnectionFactory();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "error creating connection factory", e);
            throw new ConnectorException(e);
        }
    }

    @Override
    public Destination createDestination(String destinationName, int transactionalMode) {
        try {
            return (Destination) getJndiContext().lookup(destinationName);
        } catch (NamingException e) {
            throw new ConnectorException(
                    String.format("cannot create destination for %s - transactionalMode %s", destinationName,
                            transactionalMode), e);
        }
    }

    private JndiContext getJndiContext() {
        if (_jndiContext == null) {
            _jndiContext = ThreadLocal.withInitial(() -> new JndiContext(this));
        }
        return _jndiContext.get();
    }

    @Override
    public void close() {
        super.close();
        IOUtil.closeQuietly(getJndiContext());
    }

    @Override
    public void deactivate() {
        super.deactivate();
        IOUtil.closeQuietly(getJndiContext());
    }
}
