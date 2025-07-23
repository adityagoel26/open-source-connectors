// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.GenericJndiJmsV1Adapter;
import com.boomi.connector.jmssdk.client.GenericJndiJmsV2Adapter;
import com.boomi.connector.jmssdk.client.OracleAQAdapter;
import com.boomi.connector.jmssdk.client.WebLogicV1Adapter;
import com.boomi.connector.jmssdk.client.WebLogicV2Adapter;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.client.websphere.WebsphereV1Adapter;
import com.boomi.connector.jmssdk.client.websphere.WebsphereV2Adapter;
import com.boomi.util.IOUtil;

import org.apache.commons.pool.PoolableObjectFactory;

/**
 * Factory class for Adapter creation. Implements {@link PoolableObjectFactory} to be able to be injected to a
 * {@link org.apache.commons.pool.impl.GenericObjectPool}
 */
public class AdapterFactory implements PoolableObjectFactory<GenericJndiBaseAdapter> {

    private final AdapterSettings _settings;

    public AdapterFactory(AdapterSettings settings) {
        _settings = settings;
    }

    /**
     * Build an appropriate JMS Adapter for the configured service
     *
     * @return a JMS Adapter
     */
    @Override
    public GenericJndiBaseAdapter makeObject() {
        switch (_settings.getJmsVersion()) {
            case V2_0:
                return createV2Adapter();
            case V1_1:
                return createV1Adapter();
            default:
                throw new UnsupportedOperationException();
        }
    }

    private GenericJndiJmsV2Adapter createV2Adapter() {
        // more server types will be added to this switch in following tickets
        switch (_settings.getServerType()) {
            case GENERIC_JNDI:
            case ACTIVEMQ_ARTEMIS:
                return new GenericJndiJmsV2Adapter(_settings);
            case ORACLE_AQ_WEBLOGIC:
                return new WebLogicV2Adapter(_settings);
            case WEBSPHERE_MQ_SINGLE:
            case WEBSPHERE_MQ_MULTI_INSTANCE:
                return new WebsphereV2Adapter(_settings);
            default:
                throw new UnsupportedOperationException(
                        String.format("Server type %s does not support JMS 1.1", _settings.getServerType()));
        }
    }

    private GenericJndiJmsV1Adapter createV1Adapter() {
        // more server types will be added to this switch in following tickets
        switch (_settings.getServerType()) {
            case GENERIC_JNDI:
            case ACTIVEMQ_CLASSIC:
            case ACTIVEMQ_ARTEMIS:
            case SONICMQ:
                return new GenericJndiJmsV1Adapter(_settings);
            case ORACLE_AQ:
                return new OracleAQAdapter(_settings);
            case ORACLE_AQ_WEBLOGIC:
                return new WebLogicV1Adapter(_settings);
            case WEBSPHERE_MQ_SINGLE:
            case WEBSPHERE_MQ_MULTI_INSTANCE:
                return new WebsphereV1Adapter(_settings);
            default:
                throw new UnsupportedOperationException(
                        String.format("Server type %s does not support JMS 1.1", _settings.getServerType()));
        }
    }

    /**
     * Closes the given adapter
     *
     * @param adapter to be closed
     */
    @Override
    public void destroyObject(GenericJndiBaseAdapter adapter) {
        IOUtil.closeQuietly(adapter);
    }

    /**
     * Validates if the given adapter is still working
     *
     * @param adapter to be validated
     * @return {@code true} if the adapter can be reused, {@code false} if it is closed and should be disposed
     */
    @Override
    public boolean validateObject(GenericJndiBaseAdapter adapter) {
        return adapter.validate();
    }

    @Override
    public void activateObject(GenericJndiBaseAdapter adapter) {
        // no-op
    }

    /**
     * Deactivate the given adapter
     *
     * @param adapter to be deactivated
     */
    @Override
    public void passivateObject(GenericJndiBaseAdapter adapter) {
        adapter.deactivate();
    }
}
