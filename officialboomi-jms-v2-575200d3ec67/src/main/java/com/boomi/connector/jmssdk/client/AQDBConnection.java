// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import oracle.jms.AdtMessage;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQObjectFactory;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQStructMetaData;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQStructMetaDataFactory;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQStructPayloadFactory;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQTargetDestination;
import com.boomi.connector.jmssdk.operations.model.oracleaq.BaseStructMetaDataFactory;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

import org.w3c.dom.Document;

import javax.jms.JMSException;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;

/**
 * the class {@link AQDBConnection} is responsible to access of DB for Oracle AQ.
 */
class AQDBConnection implements Closeable {

    private static final String FIELD_ID = "id";
    private static final String BASE_QUEUE_SQL =
            "SELECT (CASE  t.recipients WHEN 'SINGLE' THEN 'queue:' ELSE CASE t.recipients WHEN 'MULTIPLE' THEN "
                    + "'topic:' END END) || NVL(t.object_type,'') || ':' || q.name AS id, q.name AS name "
                    + "FROM all_queues q, all_queue_tables t "
                    + "WHERE q.queue_type = 'NORMAL_QUEUE' AND LOWER(q.owner) = ? AND q.queue_table = t.queue_table ";
    private static final String ORDER_BY_Q_NAME = "order by q.name";
    private static final String ALL_QUEUE_SQL = BASE_QUEUE_SQL + ORDER_BY_Q_NAME;
    private static final String FILTER_QUEUE_NAME = " AND q.name like ? ";
    private static final String FILTER_QUEUE_SQL = BASE_QUEUE_SQL + FILTER_QUEUE_NAME + ORDER_BY_Q_NAME;
    private static final int INDEX_FILTER_NAME = 2;
    private static final int INDEX_USER_PARAM = 1;
    private final AdapterSettings _settings;
    private final Connection _connection;
    private final AQObjectFactory _objectFactory;
    private final AQStructMetaDataFactory _metaDataFactory;

    public AQDBConnection(AdapterSettings settings) throws SQLException {
        _settings = settings;
        _connection = createConnection(_settings);
        _objectFactory = AQObjectFactory.instance(_connection);
        _metaDataFactory = BaseStructMetaDataFactory.instance(_connection);
    }

    private static Connection createConnection(AdapterSettings settings) throws SQLException {
        if (settings.useAuthentication()) {
            return DriverManager.getConnection(settings.getJdbcUrl(), settings.getUsername(), settings.getPassword());
        } else {
            return DriverManager.getConnection(settings.getJdbcUrl());
        }
    }

    /**
     * return the ObjectDefinition using the schema stored in the db regarding of  queue data type
     *
     * @param targetDestination
     * @return @{@link ObjectDefinition}
     */
    public ObjectDefinition getObjectDefinition(TargetDestination targetDestination) {
        try {
            AQStructMetaData structMetaData = _metaDataFactory.getTypeMetaData(targetDestination.getName(),
                    targetDestination.getDataType());
            return targetDestination.getObjectDefinition(AQObjectFactory.createProfileSchema(structMetaData),
                    structMetaData.toJson().toString());
        } catch (SQLException e) {
            throw new ConnectorException(
                    String.format("The scheme for target destination %s with data type %s cannot be return.",
                            targetDestination.getName(), targetDestination.getDataType()), e);
        }
    }

    /**
     * Create a Document from the message and the message struct, just for adt messages.
     *
     * @param targetDestination
     * @param message
     * @return {@link Document}
     * @throws JMSException
     */
    public Document createAdtMessageDocument(TargetDestination targetDestination, AdtMessage message)
            throws JMSException {
        AQStructPayloadFactory payload = (AQStructPayloadFactory) message.getAdtPayload();
        Struct struct = payload.getStruct();
        try {
            return _objectFactory.createDocument(
                    _metaDataFactory.getStructMetaData(targetDestination.getDestinationName(), struct), struct);
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * return all destinations according to of the filter
     *
     * @param filter
     * @return
     * @throws SQLException
     */
    public List<TargetDestination> getAllDestination(String filter) throws SQLException {
        PreparedStatement statement = null;
        ResultSet result = null;
        filter = filter.replaceAll("\\*", "%").replaceAll("\\?", "_");
        List<TargetDestination> targetDestinations = new ArrayList<>();
        try {
            statement = _connection.prepareStatement(StringUtil.isNotBlank(filter) ? FILTER_QUEUE_SQL : ALL_QUEUE_SQL);
            statement.setString(INDEX_USER_PARAM, _settings.getUsername().toLowerCase());
            if (StringUtil.isNotBlank(filter)) {
                statement.setString(INDEX_FILTER_NAME, filter);
            }
            result = statement.executeQuery();
            while (result.next()) {
                targetDestinations.add(new AQTargetDestination(result.getString(FIELD_ID)));
            }
        } finally {
            IOUtil.closeQuietly(statement, result);
        }
        return targetDestinations;
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_connection);
    }
}
