// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.util;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.boomi.connector.api.ConnectorException;

import oracle.sql.ArrayDescriptor;
import oracle.sql.StructDescriptor;

/**
 * The Class MetadataUtil.
 *
 * @author swastik.vn
 */
public class MetadataUtil {

	/** The data types. */
	private Map<String, String> dataTypes = new HashMap<>();

	/** The type names. */
	private Map<String, String> typeNames = new LinkedHashMap<>();

	/**
	 * Instantiates a new metadata util.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 */
	public MetadataUtil(Connection con, String objectTypeId) {
		this.dataTypes = this.getDataTypes(con, objectTypeId);
	}

	/** The Constant VARCHAR. */
	public static final String VARCHAR = "12";

	/** The Constant CLOB. */
	public static final String CLOB = "2005";

	/** The Constant LONGVARCHAR. */
	public static final String LONGVARCHAR = "-1";

	/** The Constant INTEGER. */
	public static final String INTEGER = "4";

	/** The Constant NUMERIC. */
	public static final String NUMERIC = "2";

	/** The Constant TIMESTAMP. */
	public static final String TIMESTAMP = "93";

	/** The Constant DATE. */
	public static final String DATE = "91";

	/** The Constant TINYINT. */
	public static final String TINYINT = "-6";

	/** The Constant BOOLEAN. */
	public static final String BOOLEAN = "16";

	/** The Constant BIT. */
	public static final String BIT = "-7";

	/** The Constant TIME. */
	public static final String TIME = "92";

	/** The Constant CHAR. */
	public static final String CHAR = "1";

	/** Constant NVARCHAR. */
	public static final String NVARCHAR = "-9";

	/** The Constant SMALLINT. */
	public static final String SMALLINT = "5";

	/** The Constant DECIMAL. */
	public static final String DECIMAL = "3";

	/** The Constant DOUBLE. */
	public static final String DOUBLE = "8";

	/** The Constant FLOAT. */
	public static final String FLOAT = "6";

	/** The Constant BIGINT. */
	public static final String BIGINT = "-5";

	/** The Constant BLOB. */
	public static final String BLOB = "2004";

	/** The Constant NCHAR. */
	public static final String NCHAR = "-15";

	/** The Constant REAL. */
	public static final String REAL = "7";

	/** The Constant BINARY. */
	public static final String BINARY = "-2";

	/** The Constant VARBINARY. */
	public static final String VARBINARY = "-3";

	/** The Constant LONGVARBINARY. */
	public static final String LONGVARBINARY = "-4";

	/** The Constant LONGNVARCHAR. */
	public static final String LONGNVARCHAR = "-16";

	/** The Constant ARRAY. */
	public static final String ARRAY_TYPE = "2003";

	/**
	 * This method will fetch the dataTypes of the columns and stores it in a Map.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @return type
	 */
	public Map<String, String> getDataTypes(Connection con, String objectTypeId) {
		Map<String, String> dataType = new HashMap<>();
		try (ResultSet resultSet = con.getMetaData().getColumns(con.getCatalog(), con.getSchema(), objectTypeId, null)) {
			while (resultSet.next()) {
				switch (resultSet.getString(DATA_TYPE)) {
				case VARCHAR:
				case CLOB:
				case LONGVARCHAR:
				case CHAR:
				case NVARCHAR:
				case NCHAR:
					dataType.put(resultSet.getString(COLUMN_NAME), STRING);
					break;
				case INTEGER:
				case TINYINT:
				case SMALLINT:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.INTEGER);
					break;
				case DATE:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DATE);
					break;
				case BOOLEAN:
				case BIT:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.BOOLEAN);
					break;
				case TIME:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.TIME);
					break;
				case BIGINT:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.LONG);
					break;
				case DOUBLE:
				case DECIMAL:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DOUBLE);
					break;
				case FLOAT:
				case REAL:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.FLOAT);
					break;
				case BLOB:
				case BINARY:
				case LONGVARBINARY:
				case VARBINARY:
					dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.BLOB);
					break;
				case TIMESTAMP:
					setTimestampType(dataType, resultSet);
					break;
				case NUMERIC:
					setNumericType(dataType, resultSet);
					break;
				case ARRAY_TYPE:
					ArrayDescriptor array = ArrayDescriptor.createDescriptor(resultSet.getString(TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con) );
					boolean type1 = QueryBuilderUtil.checkArrayDataType(array);
					if (type1) {
						dataType.put(resultSet.getString(COLUMN_NAME), ARRAY);
					} else {
						iterateOverNestedTable(dataType, con, resultSet);
					}
					break;
				default:
					break;
				}
				typeNames.put(resultSet.getString(COLUMN_NAME), resultSet.getString(TYPE_NAME));
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
		return dataType;
	}

	/**
	 * 
	 * @param dataType
	 * @param resultSet
	 * @throws SQLException
	 */
	private void setNumericType(Map<String, String> dataType, ResultSet resultSet) throws SQLException {
		int decimalDigits = resultSet.getInt(DECIMAL_DIGITS);
		if (decimalDigits == 0)
			dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.INTEGER);
		else
			dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DOUBLE);
	}

	/**
	 * 
	 * @param dataType
	 * @param resultSet
	 * @throws SQLException
	 */
	private void setTimestampType(Map<String, String> dataType, ResultSet resultSet) throws SQLException {
		if(resultSet.getString(TYPE_NAME).equals("DATE") || resultSet.getString(TYPE_NAME).equalsIgnoreCase("DATE")) {
			dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DATE);
		}
		else {
		dataType.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.TIMESTAMP);
		}
	}

	/**
	 * This method will fetch the dataTypes of the columns and stores it in a Map.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @return type
	 * @throws SQLException the SQL exception
	 */
	public static Map<String, String> getDataTypesWithTable(Connection con, String objectTypeId) throws SQLException {

		Map<String, String> type = new HashMap<>();
		DatabaseMetaData md = con.getMetaData();
		try (ResultSet resultSet = md.getColumns(null, null, objectTypeId, null);) {
			while (resultSet.next()) {
				switch (resultSet.getString(DATA_TYPE)) {
				case VARCHAR:
				case CLOB:
				case LONGVARCHAR:
				case CHAR:
				case NVARCHAR:
				case NCHAR:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), STRING);
					break;
				case INTEGER:
				case TINYINT:
				case SMALLINT:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.INTEGER);
					break;
				case DATE:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DATE);
					break;
				case BOOLEAN:
				case BIT:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.BOOLEAN);
					break;
				case TIME:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.TIME);
					break;
				case BIGINT:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.LONG);
					break;
				case DOUBLE:
				case DECIMAL:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DOUBLE);
					break;
				case FLOAT:
				case REAL:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.FLOAT);
					break;
				case BLOB:
				case BINARY:
				case LONGVARBINARY:
				case VARBINARY:
					type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.BLOB);
					break;
				case TIMESTAMP:
					setTimestampDatatype(objectTypeId, type, resultSet);
					break;
				case NUMERIC:
					setNumericDatatype(objectTypeId, type, resultSet);
					break;
				case ARRAY_TYPE:
					ArrayDescriptor array = ArrayDescriptor.createDescriptor(resultSet.getString(TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
					boolean type1 = QueryBuilderUtil.checkArrayDataType(array);
					if (type1) {
						type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), ARRAY);
					} else {
						iterateOverNestedTable(type, con, resultSet);
					}
					break;
				default:
					break;
				}
			}
		}
		return type;
	}

	/**
	 * 
	 * @param objectTypeId
	 * @param type
	 * @param resultSet
	 * @throws SQLException
	 */
	private static void setNumericDatatype(String objectTypeId, Map<String, String> type, ResultSet resultSet)
			throws SQLException {
		int decimalDigits = resultSet.getInt(DECIMAL_DIGITS);
		if (decimalDigits == 0)
			type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME),
					OracleDatabaseConstants.INTEGER);
		else
			type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DOUBLE);
	}

	/**
	 * 
	 * @param objectTypeId
	 * @param type
	 * @param resultSet
	 * @throws SQLException
	 */
	private static void setTimestampDatatype(String objectTypeId, Map<String, String> type, ResultSet resultSet)
			throws SQLException {
		if (resultSet.getString(TYPE_NAME).equals("DATE")
				|| resultSet.getString(TYPE_NAME).equalsIgnoreCase("DATE")) {
			type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.DATE);
		} else {
			type.put(objectTypeId + DOT + resultSet.getString(COLUMN_NAME),
					OracleDatabaseConstants.TIMESTAMP);
		}
	}

	/**
	 * This method will iterate over nested table and get the metadata of the inner
	 * tables.
	 *
	 * @param type      the type
	 * @param con       the con
	 * @param resultSet the result set
	 * @throws SQLException the SQL exception
	 */
	private static void iterateOverNestedTable(Map<String, String> type, Connection con, ResultSet resultSet)
			throws SQLException {

		type.put(resultSet.getString(COLUMN_NAME), OracleDatabaseConstants.ARRAY);
		ArrayDescriptor arrayLevel1 = ArrayDescriptor
				.createDescriptor(con.getMetaData().getUserName() + "." + resultSet.getString(TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
		StructDescriptor structLevel1 = StructDescriptor.createDescriptor(arrayLevel1.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData rsmd1 = structLevel1.getMetaData();
		for (int j = 1; j <= rsmd1.getColumnCount(); j++) {

			switch (rsmd1.getColumnType(j)) {
			case 4:
			case -6:
			case 5:
			case 2:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.INTEGER);
				break;
			case 91:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.DATE);
				break;
			case 12:
			case 1:
			case 2005:
			case -1:
			case -9:
			case -15:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.STRING);
				break;
			case 16:
			case -7:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.BOOLEAN);
				break;
			case 92:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.TIME);
				break;
			case -5:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.LONG);
				break;
			case 8:
			case 3:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.DOUBLE);
				break;
			case 6:
			case 7:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.FLOAT);
				break;
			case 2004:
			case -2:
			case -4:
			case -3:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.BLOB);
				break;
			case 93:
				type.put(rsmd1.getColumnName(j), OracleDatabaseConstants.TIMESTAMP);
				break;
			case 2003:
				iterateOverInnerNestedTable(type, con, rsmd1, j);
				break;
			default:
				break;
			}
		}
	}

	/**
	 * 
	 * @param type
	 * @param con
	 * @param rsmd1
	 * @param j
	 * @throws SQLException
	 */
	private static void iterateOverInnerNestedTable(Map<String, String> type, Connection con, ResultSetMetaData rsmd1,
			int j) throws SQLException {
		ArrayDescriptor arrayLevel2 = ArrayDescriptor.createDescriptor(rsmd1.getColumnTypeName(j), SchemaBuilderUtil.getUnwrapConnection(con));
		boolean type1 = QueryBuilderUtil.checkArrayDataType(arrayLevel2);
		if (type1) {
			type.put(rsmd1.getColumnName(j), ARRAY);
		} else {
			type.put(rsmd1.getColumnName(j), ARRAY);
			StructDescriptor structLevel2 = StructDescriptor.createDescriptor(arrayLevel2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
			ResultSetMetaData rsmd2 = structLevel2.getMetaData();
			for (int k = 1; k <= rsmd2.getColumnCount(); k++) {
				switch (rsmd2.getColumnType(k)) {
				case 2:
				case 4:
				case -6:
				case 5:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.INTEGER);
					break;
				case 91:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.DATE);
					break;
				case 12:
				case 1:
				case 2005:
				case -1:
				case -9:
				case -15:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.STRING);
					break;
				case 16:
				case -7:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.BOOLEAN);
					break;
				case 92:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.TIME);
					break;
				case -5:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.LONG);
					break;
				case 8:
				case 3:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.DOUBLE);
					break;
				case 6:
				case 7:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.FLOAT);
					break;
				case 2004:
				case -2:
				case -4:
				case -3:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.BLOB);
					break;
				case 93:
					type.put(rsmd2.getColumnName(k), OracleDatabaseConstants.TIMESTAMP);
					break;
				case 2003:
					ArrayDescriptor arrayLevel3 = ArrayDescriptor.createDescriptor(rsmd2.getColumnTypeName(k),
							SchemaBuilderUtil.getUnwrapConnection(con));
					boolean type3 = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
					if (type3) {
						type.put(rsmd2.getColumnName(k), ARRAY);
					} else {
						throw new ConnectorException("Nested level exhausted!!");
					}
					break;
				default:
					break;
				}
			}

		}
	}

	/**
	 * Gets the list of primary keys.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @return the primary key
	 * @throws SQLException the SQL exception
	 */
	public static List<String> getPrimaryK(Connection con, String objectTypeId) throws SQLException {
		List<String> pk = new ArrayList<>();
		try (ResultSet resultSet = con.getMetaData().getPrimaryKeys(null, con.getSchema(),
				objectTypeId.toUpperCase())) {
			while (resultSet.next()) {
				if (resultSet.getString(COLUMN_NAME) != null)
					pk.add(resultSet.getString(COLUMN_NAME));
			}
		}
		return pk;
	}

	/**
	 * Gets the data type.
	 *
	 * @return the data type
	 */
	public Map<String, String> getDataType() {
		return dataTypes;
	}

	/**
	 * Gets the type names.
	 *
	 * @return the type names
	 */
	public Map<String, String> getTypeNames() {
		return typeNames;
	}
}
