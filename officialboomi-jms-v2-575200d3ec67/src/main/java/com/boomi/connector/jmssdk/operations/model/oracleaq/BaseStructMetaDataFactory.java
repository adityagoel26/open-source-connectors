// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.OracleTypes;
import com.boomi.connector.api.ConnectorException;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory and base implementation of {@link AQStructMetaDataFactory}
 */
public abstract class BaseStructMetaDataFactory implements AQStructMetaDataFactory {

    private static final String ARRAY_ITEM_NAME = "ITEM";
    private static final String TOPIC_PREFIX = "topic:";
    private static final String QUEUE_PREFIX = "queue:";
    private static final String SPECIAL_CHARACTER_REPLACEMENT = "_";
    private static final String INVALID_FIRST_CHARACTER_REGEX = "[^A-Za-z_]";
    private static final String CHARACTERS_NOT_VALID_IN_XML_TAG_NAME = "[^A-Za-z0-9\\.-]";

    private static final double LEGACY_VERSION_CUTOFF = 11.3;
    public static final int VERSION_DENOMINATOR = 10;
    protected final Connection _conn;

    protected BaseStructMetaDataFactory(Connection conn) {
        this._conn = conn;
    }

    /**
     * Create a new instance of {@link AQStructMetaDataFactory} based on the connection's driver version.
     * <p>
     * Versions prior to {@value #LEGACY_VERSION_CUTOFF} will use the legacy approach to working with structs in oracle
     *
     * @param conn
     * @return StructMetaData instance appropriate for the current connection
     * @throws SQLException
     */
    public static AQStructMetaDataFactory instance(Connection conn) throws SQLException {
        if (conn == null) {
            throw new ConnectorException("null connection");
        }
        float driverVersion = conn.getMetaData().getDriverMajorVersion()
                + (float) conn.getMetaData().getDriverMinorVersion() / VERSION_DENOMINATOR;
        if (driverVersion >= LEGACY_VERSION_CUTOFF) {
            return new CurrentStructMetaDataFactory(conn);
        } else {
            return new LegacyStructMetaDataFactory(conn);
        }
    }

    @Override
    public final AQStructMetaData getTypeMetaData(String name, String typeName) throws SQLException {
        if (typeName == null) {
            throw new ConnectorException("null typeName");
        }
        return createStructMetaData(name, typeName, getOracleMetaData(typeName));
    }

    @Override
    public final AQStructMetaData getStructMetaData(String name, Struct struct) throws SQLException {
        if (struct == null) {
            return new AQComplexStruct(name);
        }
        return createStructMetaData(name, struct.getSQLTypeName(), getOracleMetaData(struct));
    }

    /**
     * Get the {@link ResultSetMetaData} for the specified type
     *
     * @param typeName the SQL name of the type
     * @return metadata for the specified type
     * @throws SQLException
     */
    protected abstract ResultSetMetaData getOracleMetaData(String typeName) throws SQLException;

    /**
     * Get the {@link ResultSetMetaData} for the specified {@link Struct}
     *
     * @param struct the struct instance
     * @return metadata for the specified struct
     * @throws SQLException
     */
    protected abstract ResultSetMetaData getOracleMetaData(Struct struct) throws SQLException;

    /**
     * Get the base type meta data for the specified array. The base type describes the type of elements found in the
     * array, not the type of the array itself.
     *
     * @param typeName the SQL name of the array
     * @return {@link AQStructMetaData} instance representing the array type
     * @throws SQLException
     */
    protected abstract AQStructMetaData getBaseArrayMetaData(String typeName) throws SQLException;

    private AQStructMetaData createStructMetaData(String name, String type, ResultSetMetaData rsMetaData)
            throws SQLException {
        List<AQStructMetaData> attributes = new ArrayList<>();

        if (type != null) {
            Map<String, Integer> columnCounts = new HashMap<>();

            int count = rsMetaData.getColumnCount();
            for (int i = 1; i <= count; i++) {
                String attrName = format(rsMetaData.getColumnName(i), columnCounts);
                String attrTypeName = rsMetaData.getColumnTypeName(i);
                int attrType = rsMetaData.getColumnType(i);

                attributes.add(createStructMetaData(attrName, attrTypeName, attrType));
            }
        }

        return new AQComplexStruct(format(name), type, attributes);
    }

    private AQStructMetaData createStructMetaData(String name, String typeName, int type) throws SQLException {
        AQStructMetaData arrayMetaData;

        if (type == OracleTypes.STRUCT) {
            arrayMetaData = createStructMetaData(name, typeName, getOracleMetaData(typeName));
        } else if (type == OracleTypes.ARRAY) {
            arrayMetaData = createArrayMetaData(name, typeName);
        } else {
            arrayMetaData = new AQSimpleStruct(name, typeName, type);
        }

        return arrayMetaData;
    }

    private AQStructMetaData createArrayMetaData(String attrName, String attrType) throws SQLException {
        AQStructMetaData baseMetaData = getBaseArrayMetaData(attrType);
        AQStructMetaData arrayMetaData = createStructMetaData(ARRAY_ITEM_NAME, baseMetaData.getTypeName(),
                baseMetaData.getType());
        return new AQArrayStruct(attrName, attrType, arrayMetaData);
    }

    private static String format(String name) {
        return format(name, new HashMap<>());
    }

    private static String format(String columnName, Map<String, Integer> columnCounts) {
        if (columnName == null) {
            return null;
        }
        columnName = removePrefix(columnName, QUEUE_PREFIX);
        columnName = removePrefix(columnName, TOPIC_PREFIX);
        columnName = columnName.replaceAll(CHARACTERS_NOT_VALID_IN_XML_TAG_NAME, SPECIAL_CHARACTER_REPLACEMENT);
        columnName = addPrefixIfFirstCharacterIsNotValid(columnName);
        return addSuffixIfElementIsDuplicated(columnName, columnCounts);
    }

    private static String addSuffixIfElementIsDuplicated(String columnName, Map<String, Integer> columnCounts) {
        int duplicateCount = columnCounts.containsKey(columnName) ? columnCounts.get(columnName) : 0;
        columnCounts.put(columnName, duplicateCount + 1);
        if (duplicateCount > 0) {
            return columnName + duplicateCount;
        }
        return columnName;
    }

    private static String addPrefixIfFirstCharacterIsNotValid(String columnName) {
        String first = columnName.substring(0, 1);
        if (first.matches(INVALID_FIRST_CHARACTER_REGEX)) {
            return SPECIAL_CHARACTER_REPLACEMENT + columnName;
        }
        return columnName;
    }

    private static String removePrefix(String name, String prefix) {
        if (name.startsWith(prefix)) {
            return name.replaceFirst(prefix, "");
        }
        return name;
    }
}
