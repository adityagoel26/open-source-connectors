// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import oracle.jms.AQjmsFactory;
import oracle.jms.AQjmsSession;
import oracle.jms.AdtMessage;
import oracle.sql.Datum;
import oracle.sql.ORAData;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQObjectFactory;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQTargetDestination;
import com.boomi.connector.jmssdk.operations.model.oracleaq.BaseStructMetaDataFactory;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

import org.w3c.dom.Document;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.naming.Context;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class OracleAQAdapter extends GenericJndiJmsV1Adapter {

    private static final String QUEUE_PREFIX = "queue:";
    private static final String TOPIC_PREFIX = "topic:";
    private static final int DESTINATION_SECTION_NUMBERS = 2;
    private AQDBConnection _aqDBConnection;

    public OracleAQAdapter(AdapterSettings settings) {
        super(settings);
    }

    /**
     * get a @{@link AQDBConnection} in lazy mode.
     *
     * @return
     * @throws SQLException
     */
    private AQDBConnection getAqDBConnection() throws SQLException {
        if (_aqDBConnection == null) {
            _aqDBConnection = new AQDBConnection(_settings);
        }
        return _aqDBConnection;
    }

    /**
     * return all destinations according to the filter, if the method is not redefined will return an empty list
     *
     * @param filter
     * @return @List<{@link TargetDestination>}
     */
    public List<TargetDestination> getAllDestinations(String filter) {
        try {
            return getAqDBConnection().getAllDestination(filter);
        } catch (SQLException e) {
            throw new ConnectorException("unable to get connection", e);
        }
    }

    private TargetDestination getDestination(String name) {
        return CollectionUtil.getFirst(getAllDestinations(name));
    }

    @Override
    public ObjectDefinition getObjectDefinition(TargetDestination targetDestination) {
        if (!targetDestination.isProfileRequired()) {
            return super.getObjectDefinition(targetDestination);
        }
        try {
            return getAqDBConnection().getObjectDefinition(targetDestination);
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    protected Properties createJMSProperties(AdapterSettings settings) {
        Properties jmsProperties = new Properties();
        if (settings.useAuthentication()) {
            jmsProperties.setProperty(Context.SECURITY_PRINCIPAL, settings.getUsername());
            jmsProperties.setProperty(Context.SECURITY_CREDENTIALS, settings.getPassword());
        }
        return jmsProperties;
    }

    @Override
    protected javax.jms.Connection createJMSConnection() {
        javax.jms.Connection connection;
        try {
            ConnectionFactory connectionFactory = AQjmsFactory.getConnectionFactory(_settings.getJdbcUrl(),
                    new Properties());
            if (_settings.useAuthentication()) {
                connection = connectionFactory.createConnection(_settings.getUsername(), _settings.getPassword());
            } else {
                connection = connectionFactory.createConnection();
            }
            connection.start();
        } catch (JMSException e) {
            throw new ConnectorException(e);
        }
        return connection;
    }

    @Override
    public Destination createDestination(String destinationName, int transactionalMode) {
        Session session = null;
        try {
            session = createSession(transactionalMode);
            if (destinationName.startsWith(QUEUE_PREFIX)) {
                return session.createQueue(getQueueName(destinationName));
            } else if (destinationName.toLowerCase().startsWith(TOPIC_PREFIX)) {
                return session.createTopic(getTopicName(destinationName));
            } else {
                throw new ConnectorException(
                        "Invalid destination name, destination name must match " + QUEUE_PREFIX + " or "
                                + TOPIC_PREFIX);
            }
        } catch (JMSException e) {
            throw new ConnectorException("cannot create destination for " + destinationName, e);
        } finally {
            Utils.closeQuietly(session);
        }
    }

    @Override
    public TargetDestination createTargetDestination(String id) {
        return new AQTargetDestination(id);
    }

    @Override
    public TargetDestination createTargetDestination(String destinationName, DestinationType destinationType) {
        String[] destinationData = destinationName.split(":");
        if (destinationData.length != DESTINATION_SECTION_NUMBERS) {
            throw new ConnectorException("Destination name bad formatting %s", destinationName);
        }
        TargetDestination targetDestination = new AQTargetDestination(destinationData[1], destinationData[0], null);
        targetDestination.setDestinationType(destinationType);
        return targetDestination;
    }

    @Override
    public TargetDestination createAndLoadTargetDestination(String destinationName) {
        TargetDestination targetDestination = createTargetDestination(destinationName, null);
        return getDestination(targetDestination.getName());
    }

    private static String getQueueName(String destName) {
        return StringUtil.isNotEmpty(destName) ? destName.substring(QUEUE_PREFIX.length()) : destName;
    }

    private static String getTopicName(String destName) {
        return StringUtil.isNotEmpty(destName) ? destName.substring(TOPIC_PREFIX.length()) : destName;
    }

    public Message createObjectMessage(ObjectData data, TargetDestination targetDestination) {
        Session session = null;
        InputStream stream = data.getData();
        try {
            session = createSession(Session.CLIENT_ACKNOWLEDGE);
            ORAData createORAData = connection -> {
                AQObjectFactory factory = AQObjectFactory.instance(connection);
                return (Datum) factory.createStruct(stream, BaseStructMetaDataFactory.instance(connection),
                        targetDestination);
            };
            return ((AQjmsSession) session).createORAMessage(createORAData);
        } catch (Exception e) {
            throw new ConnectorException("unable to create object message", e);
        } finally {
            IOUtil.closeQuietly(stream);
            Utils.closeQuietly(session);
        }
    }

    @Override
    public JMSReceiver createReceiver(TargetDestination targetDestination, String messageSelector,
            int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSAQReceiver(session, destination, messageSelector, targetDestination.isProfileRequired());
    }

    @Override
    public JMSReceiver createReceiver(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSAQReceiver(session, destination, subscriptionName, messageSelector,
                targetDestination.isProfileRequired());
    }

    @Override
    public JMSListener createListener(TargetDestination targetDestination, String messageSelector,
            int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSAQListener(session, destination, messageSelector, targetDestination.isProfileRequired());
    }

    @Override
    public JMSListener createListener(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSAQListener(session, destination, subscriptionName, messageSelector,
                targetDestination.isProfileRequired());
    }

    public Document createAdtMessageDocument(TargetDestination targetDestination, AdtMessage message) {
        try {
            return getAqDBConnection().createAdtMessageDocument(targetDestination, message);
        } catch (Exception e) {
            throw new ConnectorException("unable to create adt message document", e);
        }
    }

    @Override
    public void close() {
        super.close();
        IOUtil.closeQuietly(_aqDBConnection);
    }
}
