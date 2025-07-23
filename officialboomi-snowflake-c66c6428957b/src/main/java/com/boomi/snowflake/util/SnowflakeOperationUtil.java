// Copyright (c) 2025 Boomi, LP

package com.boomi.snowflake.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.override.ConnectionOverrideUtil;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import net.snowflake.client.jdbc.internal.apache.commons.codec.digest.DigestUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Optional;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

/**
 * Utility class for Snowflake operations.
 */
public final class SnowflakeOperationUtil {

    private static final String SHOW_COLUMN = "SHOW COLUMNS IN ";

    private static volatile ObjectMapper OBJECT_MAPPER;

    /** The Constant FOUR. */
    private static final int FOUR = 4;
    /** The Constant SIX. */
    private static final int SIX = 6;

    public static final int TABLE_NAME_INDEX = 2;
    public static final int COLUMN_NAME_INDEX = 4;
    public static final int DATA_TYPE_INDEX = 6;
    public static final int COLUMN_NAME = 3;
    public static final int DEFAULT_VALUE_INDEX = 6;
    public static final int COLUMN_KIND_INDEX = 7;
    public static final int AUTOINCREMENT_INDEX = 11;
    public static final int FIVE = 5;

    private SnowflakeOperationUtil() {
        // Prevent initialization
    }

    /**
     * Returns the given {@code BoundedMap} if it is not {@code null}. If the input is {@code null},
     * a new instance of {@code BoundedMap} is created with a default capacity (defined by {@code FIVE})
     * and returned.
     *
     * @param boundedMap the existing bounded map, or {@code null} if no map is provided.
     * @return a non-null {@code BoundedMap} instance, either the original or a new one if the input was {@code null}.
     */
    public static BoundedMap<String, TableDefaultAndMetaDataObject> getBoundedMap( BoundedMap<String,
            TableDefaultAndMetaDataObject> boundedMap) {
        if (boundedMap == null) {
            boundedMap =  new BoundedMap<>(FIVE);
        }
        return boundedMap;
    }

    /**
     * Provides a singleton instance of {@link ObjectMapper} for JSON processing.
     * If the {@code OBJECT_MAPPER} is null, a new instance of {@link ObjectMapper} is created and returned.
     *
     * @return A singleton {@link ObjectMapper} instance.
     */
    public static ObjectMapper getObjectMapper() {
        if (OBJECT_MAPPER == null) {
            synchronized (SnowflakeOperationUtil.class) {
                if (OBJECT_MAPPER == null) {
                    OBJECT_MAPPER = new ObjectMapper();
                }
            }
        }
        return OBJECT_MAPPER;
    }
        /**
         * Get the actual Snowflake table metadata for data type mapping
         *
         * @param objectTypeId table details
         * @param connection   Snowflake connection
         * @return metadata JSON object
         */
    public static SortedMap<String, String> getTableMetadata(String objectTypeId, Connection connection,
            String dataBase, String schema) {
        SortedMap<String, String> metadata = new TreeMap<>();
        String[] objectData = objectTypeId.split("\\.");
        try {
            if (null == dataBase && null == schema) {
                dataBase = connection.getCatalog();
                schema = connection.getSchema();
            } else {
                dataBase = ConnectionOverrideUtil.normalizeString(dataBase);
                schema = ConnectionOverrideUtil.normalizeString(schema);
            }
            try (ResultSet columnsRS = connection.getMetaData().getColumns(dataBase, schema,
                    objectData[0].replace("\"", ""), "%")) {
                if (columnsRS.next()) {
                    do {
                        metadata.putIfAbsent(columnsRS.getString(FOUR), columnsRS.getString(SIX));
                    } while (columnsRS.next());
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException("Unable to connect: Unexpected database (and/or) schema name", e);
        }
        return metadata;
    }

    private static String getNextModifiedQuery(Object object) {
        return "SELECT " + object;
    }

    /**
     * Retrieves default values for database columns based on the specified object type.
     *
     * @param objectTypeId      The ID of the object type used to determine the table name
     * @param connection        The database connection to execute queries
     * @param dynamicProperties Dynamic properties used to override connection settings
     * @return A SortedMap containing column names as keys and their corresponding default values as values
     * @throws SQLException       If there's an error executing database queries
     * @throws ConnectorException If unable to fetch default values due to invalid database or schema name
     */
    public static SortedMap<String, String> getDefaultsValues(String objectTypeId, Connection connection,
            DynamicPropertyMap dynamicProperties) throws SQLException {
        SortedMap<String, String> defaultValues = new TreeMap<>();
        String tableName = getModifiedTableName(objectTypeId);
        String query = getModifiedQuery(tableName);
        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(connection, dynamicProperties);
        try (PreparedStatement stmt1 = connection.prepareStatement(query);
             ResultSet resultSet = stmt1.executeQuery()) {
            while (resultSet.next()) {
                if (!resultSet.getString(DEFAULT_VALUE_INDEX).isEmpty()) {
                    String queryNext = getNextModifiedQuery(resultSet.getObject(DEFAULT_VALUE_INDEX));
                    String defaultValue = getDefaultValueForQuery(connection, queryNext);
                    if (defaultValue != null) {
                        defaultValues.putIfAbsent(resultSet.getString(COLUMN_NAME), defaultValue);
                    }
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException("Unable to fetch default values: Unexpected database (and/or) schema name", e);
        }
        return defaultValues;
    }

    /**
     * Executes the secondary query to fetch the default value.
     *
     * @param connection the active database connection
     * @param query      the query to execute
     * @return the default value from the first column of the result set, or null if no value is found
     * @throws SQLException if a database error occurs
     */
    private static String getDefaultValueForQuery(Connection connection, String query) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }



    /**
     * Get snowflake table name from objectTypeId.
     *
     * @param tableName the objectTypeId
     * @return table name
     */
    public static String getModifiedQuery(String tableName) {
        String sanitizedTableName = tableName.replaceAll("[^a-zA-Z0-9_]", "");
        return SHOW_COLUMN +sanitizedTableName;
    }

    /**
     * Get the actual Snowflake table metadata for data type mapping
     *
     * @param objectTypeId table details
     * @param connection   Snowflake connection
     * @return metadata JSON object
     */
    public static SortedMap<String, String> getTableMetadataForCreate(String objectTypeId, Connection connection,
              String dataBase, String schema) throws SQLException {
        SortedMap<String, String> metadata = new TreeMap<>();
        String tableName = getModifiedTableName(objectTypeId);
        String query = getModifiedQuery(tableName);
        List<String> filterColumns = new ArrayList<>();
        if (null == dataBase && null == schema) {
            dataBase = connection.getCatalog();
            schema = connection.getSchema();
        } else {
            ConnectionOverrideUtil.updateConnection(connection, dataBase, schema);
        }
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet resultSet = stmt.executeQuery();
             ResultSet columnsRS = connection.getMetaData().getColumns(dataBase, schema, tableName, "%")) {
            while (resultSet.next()) {
                if(!resultSet.getString(COLUMN_KIND_INDEX).equals("COLUMN") ||
                        !resultSet.getString(AUTOINCREMENT_INDEX).isEmpty()){
                    filterColumns.add(resultSet.getString(COLUMN_NAME));
                }
            }
            while (columnsRS.next()) {
                if(!filterColumns.contains(columnsRS.getString(COLUMN_NAME_INDEX))) {
                    metadata.putIfAbsent(columnsRS.getString(COLUMN_NAME_INDEX),
                            columnsRS.getString(DATA_TYPE_INDEX));
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException("Unable to connect: Unexpected database (and/or) schema name", e);
        }
        return metadata;
    }

    /**
     * Get snowflake table name from connection properties.
     *
     * @param properties the connection properties
     * @return table name
     */
    public static String getTableName(ConnectionProperties properties) {
        String tableName = properties.getTableName();
        if (tableName != null && tableName.lastIndexOf("\".\"") != -1) {
            tableName = tableName.substring(tableName.lastIndexOf("\".\"") + TABLE_NAME_INDEX);
        }
        return tableName;
    }

    /**
     * Sets default values for the database and schema by populating the provided `input` map with default values.
     * If no default values exist in the cache (`boundedMap`), it fetches the default values from the database
     * using {@code SnowflakeOperationUtil} and stores them in the cache.
     *
     * @param input                A sorted map containing input data to which default values may be added.
     * @param emptyFieldSelection  A flag indicating how empty fields should be handled. If this matches
     *                             {@code SnowflakeDataTypeConstants.DEFAULT_SELECTION} and batch size is greater
     *                             than 1, default values are added to the `input`.
     * @param tableName           The connection properties, including the target table name.
     * @param boundedMap           A bounded map used as a cache for default values keyed by database and schema.
     * @param dynamicProperties    Dynamic properties that may influence the fetching of default values.
     * @throws SQLException        If an error occurs while accessing the database or retrieving the connection.
     */
    public static void setDefaultValuesForDBAndSchema(SnowflakeWrapper wrapper, SortedMap<String, String> input,
             String emptyFieldSelection, String tableName, BoundedMap<String, TableDefaultAndMetaDataObject> boundedMap,
             DynamicPropertyMap dynamicProperties, Long batchSize) throws SQLException {
        Connection connection = wrapper.getPreparedStatement().getConnection();
        SortedMap<String, String> defaultValues;
        String key = createHashKey(connection);
        TableDefaultAndMetaDataObject defaultValueMap = boundedMap.get(key);
        if( defaultValueMap == null || defaultValueMap.getDefaultValues().isEmpty()){
            defaultValues = SnowflakeOperationUtil.getDefaultsValues(tableName,
                    connection, dynamicProperties);
            TableDefaultAndMetaDataObject defaultValueObject = new TableDefaultAndMetaDataObject();
            defaultValueObject.setDefaultValues(defaultValues);
            boundedMap.put(key, defaultValueObject);
        }
        else{
            defaultValues = Optional.ofNullable(defaultValueMap
                    .getDefaultValues() ).orElse(new TreeMap<>());
        }

        if(SnowflakeDataTypeConstants.DEFAULT_SELECTION.equals(emptyFieldSelection) && batchSize > 1) {
            defaultValues.forEach(input::putIfAbsent);
        }
    }

    /**
     * Generates a SHA-256 hash key using the database connection metadata.
     *
     * <p>The hash key is created using the combination of the database URL,
     * catalog, and schema, separated by a pipe ("|") character. This ensures
     * a unique identifier for different database connections.</p>
     *
     * @param connection The database connection used to retrieve metadata.
     * @return A SHA-256 hashed string representing the connection's metadata.
     * @throws SQLException If an error occurs while retrieving metadata.
     */
    public static String createHashKey(Connection connection) throws SQLException {
        return DigestUtils.sha256Hex(connection.getMetaData().getURL() + "|" +
                connection.getCatalog() + "|" + connection.getSchema());
    }

    /**
     * Retrieves metadata values for the specified table in a Snowflake database.
     * Depending on the provided connection properties, this method determines whether to fetch
     * metadata suitable for creating a table or general table metadata.
     *
     * @param properties The {@link ConnectionProperties} containing connection settings and logger.
     * @param tableName The name of the table for which metadata is to be retrieved.
     * @param database The name of the database containing the table.
     * @param schema The schema containing the table.
     * @return A {@link SortedMap} containing the metadata values for the table.
     * @throws SQLException If there is an error during the database operation.
     */
    public static SortedMap<String, String> getMetadataValues(ConnectionProperties properties,
                    String tableName, String database, String schema) throws SQLException {
        return (SnowflakeDataTypeConstants.NULL_SELECTION.equals(properties.getEmptyValueInput())
                || properties.getBatchSize() > 1) ? SnowflakeOperationUtil.getTableMetadataForCreate(tableName,
                properties.getConnectionGetter().getConnection(properties.getLogger()),
                ConnectionOverrideUtil.normalizeString(database), ConnectionOverrideUtil.normalizeString(schema))
                : SnowflakeOperationUtil.getTableMetadata(tableName,
                        properties.getConnectionGetter().getConnection(properties.getLogger()), database, schema);
    }

    /**
     * Processes cookie values and sets metadata and default values based on the input.
     *
     * This method parses JSON data from the given cookie value to extract default values,
     * metadata values, and table data values. It then updates instance variables based on
     * the presence or absence of these values and certain conditions from the provided properties.
     *
     * @param cookieValue The JSON string containing cookie data.
     * @throws JsonProcessingException \Exception If an error occurs while processing the JSON data
     * or fetching metadata.
     */
    public static SortedMap<String, TableDefaultAndMetaDataObject> readDataFromCookie(String cookieValue,
                    ConnectionProperties properties) throws JsonProcessingException{
        SortedMap<String,TableDefaultAndMetaDataObject> tableMetadataMap = new TreeMap<>();
        if(cookieValue != null) {
            SortedMap<String, String> tableMetaData;
            SortedMap<String, String> defaultValues;
            SortedMap<String, String> metaData;
            TypeFactory typeFactory = getObjectMapper().getTypeFactory();
            SortedMap<String, TableDefaultAndMetaDataObject> cookieValueMap = getObjectMapper()
                    .readValue(
                    cookieValue,
                    typeFactory.constructMapType(TreeMap.class, String.class, TableDefaultAndMetaDataObject.class)
            );
            Map.Entry<String, TableDefaultAndMetaDataObject> tableDefaultAndMetaDataObject
                    = cookieValueMap.entrySet().iterator().next();
            String dynamicKey = tableDefaultAndMetaDataObject.getKey();
            TableDefaultAndMetaDataObject tableDefaultAndMetaDataObjectMap = tableDefaultAndMetaDataObject.getValue();
            defaultValues = tableDefaultAndMetaDataObjectMap.getDefaultValues().isEmpty() ? new TreeMap<>() :
                    tableDefaultAndMetaDataObjectMap.getDefaultValues();
            metaData = tableDefaultAndMetaDataObjectMap.getMetaDataValues().isEmpty() ? null :
                    tableDefaultAndMetaDataObjectMap.getMetaDataValues();
            tableMetaData = tableDefaultAndMetaDataObjectMap.getTableMetaDataValues().isEmpty() ?  null :
                    tableDefaultAndMetaDataObjectMap.getTableMetaDataValues();

            metaData = (SnowflakeDataTypeConstants.NULL_SELECTION.equals(properties.getEmptyValueInput())
                    || properties.getBatchSize() > 1) ? tableMetaData : metaData;

            TableDefaultAndMetaDataObject defaultValueObjectMap = new TableDefaultAndMetaDataObject();
            defaultValueObjectMap.setDefaultValues(defaultValues);
            defaultValueObjectMap.setMetaDataValues(metaData);
            tableMetadataMap.put(dynamicKey, defaultValueObjectMap);
        }
        return tableMetadataMap;
    }

    /**
     * Processes the cookie value to extract metadata information.
     *
     * @param operationProperties an object containing operation properties.
     * @param properties          connection properties used in the Snowflake operations.
     * @return a SortedMap of metadata values.
     * @throws JsonProcessingException if reading the cookie or processing fails.
     */
    public static SortedMap<String, String> processCookieAndMetadata(
            PropertyMap operationProperties, ConnectionProperties properties, String cookieValue,
            BoundedMap<String, TableDefaultAndMetaDataObject> boundedMap) throws JsonProcessingException {
        SortedMap<String, TableDefaultAndMetaDataObject> tableMetadataMap;
        SortedMap<String, String> metaData = null;
        if (operationProperties.getLongProperty("batchSize") > 1) {
            tableMetadataMap = SnowflakeOperationUtil.readDataFromCookie(cookieValue, properties);
            metaData = tableMetadataMap.entrySet().stream().findFirst()
                    .map(entry -> {
                        boundedMap.put(entry.getKey(), entry.getValue());
                        return entry.getValue().getMetaDataValues();
                    })
                    .orElse(null);
        }
        return metaData;
    }


    /**
     * Get snowflake table name from objectTypeId.
     *
     * @param objectTypeId the objectTypeId
     * @return table name
     */
    public static String getModifiedTableName(String objectTypeId) {
        return objectTypeId.split("\\.")[0].replace("\"", "");
    }

    /**
     * Sets up and returns snowflake wrapper.
     *
     * @param properties the connection properties
     * @return snowflake wrapper
     */
    public static SnowflakeWrapper setupWrapper(ConnectionProperties properties) {
        SnowflakeWrapper wrapper =
                new SnowflakeWrapper(properties.getConnectionGetter(), properties.getConnectionTimeFormat(),
                        properties.getLogger(), properties.getTableName());
        wrapper.setPreparedStatement(null);
        Long batchSize = properties.getBatchSize();
        if (batchSize > 1) {
            wrapper.setAutoCommit(false);
        }
        return wrapper;
    }

    /**
     * Handles ConnectorException for operations.
     *
     * @param response the operation response
     * @param request  the request data
     * @param e        the exception
     */
    public static void handleConnectorException(OperationResponse response, ObjectData request, ConnectorException e) {
        request.getLogger().log(Level.WARNING, e.getMessage());
        response.addResult(request, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getMessage(),
                ResponseUtil.toPayload(e.getMessage()));
    }

    /**
     * Handles general exception.
     *
     * @param response    the operation response
     * @param requestData the request data
     * @param e           the exception
     */
    public static void handleGeneralException(OperationResponse response, ObjectData requestData, Exception e) {
        requestData.getLogger().log(Level.SEVERE, e.getMessage());
        ResponseUtil.addExceptionFailure(response, requestData, e);
    }

    /**
     * Validates if either S3 Bucket or Stage name is set
     * @param properties the connection properties
     */
    public static void validateProperties(ConnectionProperties properties){
        if (properties.getBucketName().isEmpty() && properties.getStageName().isEmpty()) {
            throw new ConnectorException("Bucket Name and Stage Name empty. Enter a valid Bucket Name/Stage Name!");
        }
    }
}