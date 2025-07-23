// Copyright (c) 2025 Boomi, LP.
package com.boomi.snowflake.override;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.StringUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Deque;

/**
 * Utility class for managing and overriding connection settings.
 * Provides static methods to configure connections as needed.
 */
public class ConnectionOverrideUtil {

    /* Private constructor to prevent instantiation */
    private ConnectionOverrideUtil() {
        throw new ConnectorException("Unable to instantiate class");
    }

    /**
     * Checks if connection settings override is enabled based on the {@code propertyMap}.
     *
     * @param propertyMap the {@code PropertyMap} containing connection settings.
     * @return {@code true} if the connection settings override is enabled; otherwise, {@code false}.
     */
    public static boolean isConnectionSettingsOverride(PropertyMap propertyMap) {
        return propertyMap.getBooleanProperty(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE,false);
    }

    /**
     * Updates {@code inputProperties} with values from {@code operationProperties} for DATABASE and SCHEMA if present.
     * Replaces existing values in {@code inputProperties} with those from {@code operationProperties}.
     *
     * @param inputProperties     the {@code PropertyMap} to be updated with new values.
     * @param operationProperties the {@code PropertyMap} containing new values for DATABASE and SCHEMA.
     */
    public static void overrideConnectionProperties(PropertyMap inputProperties, PropertyMap operationProperties) {
        if (null != operationProperties.getProperty(SnowflakeOverrideConstants.DATABASE)) {
            inputProperties.computeIfPresent(SnowflakeOverrideConstants.DATABASE,
                    (k, v) -> operationProperties.getProperty(SnowflakeOverrideConstants.DATABASE));
        }
        if (null != operationProperties.getProperty(SnowflakeOverrideConstants.SCHEMA)) {
            inputProperties.computeIfPresent(SnowflakeOverrideConstants.SCHEMA,
                    (k, v) -> getSchemaName(operationProperties));
        }
    }

    /**
     * Updates {@code operationPropertyMap} with values from {@code dynamicPropertyMap} for DATABASE and SCHEMA if
     * not empty. Replaces existing values in {@code operationPropertyMap} with those from {@code dynamicPropertyMap}.
     *
     * @param dynamicPropertyMap   the {@code DynamicPropertyMap} containing new values for DATABASE and SCHEMA.
     * @param operationPropertyMap the {@code PropertyMap} to be updated with new values.
     */
    public static void overrideOperationConnectionPropertiesWithDynamicValues(DynamicPropertyMap dynamicPropertyMap,
            PropertyMap operationPropertyMap) {
        operationPropertyMap.computeIfPresent(SnowflakeOverrideConstants.DATABASE,
                (k, v) -> dynamicPropertyMap.getProperty(SnowflakeOverrideConstants.DATABASE));
        operationPropertyMap.computeIfPresent(SnowflakeOverrideConstants.SCHEMA,
                (k, v) -> dynamicPropertyMap.getProperty(SnowflakeOverrideConstants.SCHEMA));
    }

    /**
     * Updates the connection's catalog and schema based on dynamic properties.
     *
     * @param connection        the database connection to be updated
     * @param dynamicProperties the dynamic properties containing the new catalog and schema
     * @throws SQLException if an SQL error occurs while setting catalog or schema
     */
    public static void overrideConnectionWithDynamicProperties(Connection connection,
            DynamicPropertyMap dynamicProperties) throws SQLException {
        if(null!=dynamicProperties)
        {
            String db = dynamicProperties.getProperty(SnowflakeOverrideConstants.DATABASE);
            String schema =  dynamicProperties.getProperty(SnowflakeOverrideConstants.SCHEMA);
            overrideConnection(connection, db, schema);
        }

    }

    /**
     * Normalizes the input string.
     *
     * If the string is not empty and does not start and end with quotes, it
     * converts the string to uppercase. If it does start and end with quotes,
     * those quotes are removed.
     *
     * @param value the string to normalize
     * @return the normalized string, or an empty string if input is empty
     */
    public static String normalizeString(String value) {
        if(!StringUtil.isEmpty(value)) {
            value = !value.startsWith(SnowflakeOverrideConstants.DOUBLE_QUOTE) &&
                    !value.endsWith(SnowflakeOverrideConstants.DOUBLE_QUOTE) ? value.toUpperCase()
                    : value.substring(1,value.length()-1);
        }
        return value;
    }

    /**
     * Updates the connection's catalog based on operation properties.
     *
     * @param connection          the database connection to be updated
     * @param db the property map containing the new catalog and schema values
     * @throws SQLException if an SQL error occurs while setting catalog or schema
     */
    public static void overrideConnectionForDb(Connection connection,
            String db) throws SQLException {
        String normalizedDb = normalizeString(db);
        if (!normalizedDb.equals(connection.getCatalog())) {
            connection.setCatalog(normalizedDb);
        }
    }

    /**
     * Retrieves the schema name from the given property map.
     *
     * @param operationProperties the property map containing the schema name
     * @return the schema name as a string
     */
    public static String getSchemaName(PropertyMap operationProperties) {
        return operationProperties.getProperty(SnowflakeOverrideConstants.SCHEMA);
    }

    /**
     * Checks if the override is enabled by validating the database and schema properties.
     *
     * @param operationProperties the operation properties map
     * @return {@code true} if both database and schema properties are non-null, {@code false} otherwise
     */
    public static boolean isOverrideEnabled(PropertyMap operationProperties) {
       return null != operationProperties.getProperty(SnowflakeOverrideConstants.DATABASE) &&
               null != getSchemaName(operationProperties);
    }

    /**
     * Resets the current connection to its initial state with db and schema
     * connection properties before returning to connection pool
     *
     * @throws SQLException if the connection cannot be reset
     */
    public static void resetConnection(SnowflakeConnection snfConnection, Connection connection,
            Deque<RuntimeException> exceptionStack) {
        try {
            String connectionDb = snfConnection.getContext().getConnectionProperties()
                    .getProperty(SnowflakeOverrideConstants.DATABASE);
            String connectionSchema = snfConnection.getContext().getConnectionProperties()
                    .getProperty(SnowflakeOverrideConstants.SCHEMA);
            doReset(connection,connectionDb,connectionSchema);
        } catch (SQLException e) {
            exceptionStack.push(new ConnectorException("Unable to reset connection", e));
        }
    }

    /**
     * Updates the connection's catalog and schema by new db and schema property.
     *
     * @param connection        the database connection to be updated
     * @param db                the catalog
     * @param schema            the schema
     * @throws SQLException if an SQL error occurs while setting catalog or schema
     */
    public static void overrideConnection(Connection connection,
            String db, String schema) throws SQLException {
        String normalizeDb = normalizeString(db);
        String normalizeSchema =  normalizeString(schema);
        if ((normalizeDb != null) && !normalizeDb.equals(connection.getCatalog())) {
            connection.setCatalog(normalizeDb);
        }
        if ((normalizeSchema != null) && !normalizeSchema.equals(connection.getSchema())) {
            connection.setSchema(normalizeSchema);
        }
    }

    /**
     * Resets the connection to the specified database and schema.
     * If schema is blank and db is not, sets the catalog to the normalized db value.
     *
     * @param connection the Connection object to reset
     * @param db the database name
     * @param schema the schema name (can be blank)
     * @throws SQLException if a database access error occurs
     */
    private static void doReset(Connection connection,String db,String schema) throws SQLException {
        //To handle a case where schema is default.
        if(StringUtil.isNotBlank(db) && StringUtil.isBlank(schema)){
            connection.setCatalog(normalizeString(db));
        }else {
            overrideConnection(connection, db, schema);
        }
    }

    /**
     * Updates the connection with the specified database and schema settings.
     * Only updates the catalog or schema if the provided values are not blank
     * and different from the current connection settings.
     * 
     * @param connection the database connection to be updated
     * @param database the database name to set as catalog
     * @param schema the schema name to set
     * @throws SQLException if a database access error occurs while setting catalog or schema
     */
    public static void updateConnection(Connection connection, String database, String schema) throws SQLException {
        if (StringUtil.isNotBlank(database) && !connection.getCatalog().equals(database)){
            connection.setCatalog(database);
        }
        if (StringUtil.isNotBlank(schema) && !connection.getSchema().equals(schema)){
            connection.setSchema(schema);
        }
    }
}
