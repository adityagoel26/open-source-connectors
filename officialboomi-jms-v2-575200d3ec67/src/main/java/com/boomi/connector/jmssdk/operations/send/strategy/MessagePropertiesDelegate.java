// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class with a fluent API used to populate {@link Message}'s properties.
 */
class MessagePropertiesDelegate {

    private static final Logger LOG = LogUtil.getLogger(MessagePropertiesDelegate.class);

    private final Message _message;

    MessagePropertiesDelegate(Message message) {
        _message = message;
    }

    /**
     * Set the given {@code correlationID} into the {@link Message} held by this instance. If the {@code correlationID}
     * is {@code null}, no action is performed.
     *
     * @param correlationID to be set into the {@link Message}
     * @return the same instance of {@link MessagePropertiesDelegate}
     */
    MessagePropertiesDelegate setCorrelationID(String correlationID) {
        if (StringUtil.isBlank(correlationID)) {
            return this;
        }

        try {
            _message.setJMSCorrelationID(correlationID);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "cannot set correlation id", e);
        }
        return this;
    }

    /**
     * Set the given {@code type} into the {@link Message} held by this instance. If the {@code type} is {@code null},
     * no action is performed.
     *
     * @param type to be set into the {@link Message}
     * @return the same instance of {@link MessagePropertiesDelegate}
     */
    MessagePropertiesDelegate setType(String type) {
        if (StringUtil.isBlank(type)) {
            return this;
        }

        try {
            _message.setJMSType(type);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "cannot set message type", e);
        }
        return this;
    }

    /**
     * Set the given {@code priority} into the {@link Message} held by this instance. If the {@code priority} is
     * {@code null}, no action is performed.
     *
     * @param priority to be set into the {@link Message}
     * @return the same instance of {@link MessagePropertiesDelegate}
     */
    MessagePropertiesDelegate setPriority(String priority) {
        if (StringUtil.isBlank(priority)) {
            return this;
        }

        try {
            _message.setJMSPriority(Integer.parseInt(priority));
        } catch (Exception e) {
            LOG.log(Level.WARNING, "cannot set priority", e);
        }
        return this;
    }

    /**
     * Look for the {@link Destination} associated with the given {@code replyTo} and set it into the {@link Message}
     * held by this instance. If the {@code replyTo} is {@code null} or the {@link Destination} was not found, no action
     * is performed.
     *
     * @param replyTo to be set into the {@link Message}
     * @return the same instance of {@link MessagePropertiesDelegate}
     */
    MessagePropertiesDelegate setReplyTo(String replyTo, GenericJndiBaseAdapter adapter) {
        if (StringUtil.isBlank(replyTo)) {
            return this;
        }

        try {
            Destination destination = adapter.createDestination(replyTo, Session.AUTO_ACKNOWLEDGE);
            _message.setJMSReplyTo(destination);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "cannot set reply to", e);
        }
        return this;
    }

    /**
     * Set the given {@code customProperties} into the {@link Message} held by this instance. If the
     * {@code customProperties} are {@code null} or empty, no action is performed.
     *
     * @param customProperties to be set into the {@link Message}
     * @return the same instance of {@link MessagePropertiesDelegate}
     */
    MessagePropertiesDelegate setCustomProperties(Map<String, String> customProperties) {
        if (CollectionUtil.isEmpty(customProperties)) {
            return this;
        }

        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            try {
                if (StringUtil.isBlank(property.getValue())) {
                    continue;
                }
                _message.setObjectProperty(property.getKey(), property.getValue());
            } catch (Exception e) {
                String errorMessage = String.format("%s. Cannot set custom property %s - %s", e.getMessage(),
                        property.getKey(), property.getValue());
                LOG.log(Level.WARNING, errorMessage, e);
                throw new ConnectorException(errorMessage, e);
            }
        }

        return this;
    }
}
