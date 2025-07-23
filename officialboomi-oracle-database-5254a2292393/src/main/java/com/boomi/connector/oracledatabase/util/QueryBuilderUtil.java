// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.oracledatabase.util;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.text.StringEscapeUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;

import oracle.sql.ArrayDescriptor;
import oracle.sql.StructDescriptor;

/**
 * The Class QueryBuilderUtil. This class is used to generate Dynamic Prepared
 * Statements based on the request for all the Dynamic Operations
 *
 * @author swastik.vn
 */
public class QueryBuilderUtil {

	/**
	 * Instantiates a new query builder util.
	 */
	private QueryBuilderUtil() {

	}

	/**
	 * Utility method to build the Query for procedure call.
	 *
	 * @param params     the params
	 * @param objectType the object type
	 * @return the string builder
	 */
	public static StringBuilder buildProcedureQuery(List<String> params, String objectType) {
		StringBuilder query = new StringBuilder("call ");
		query.append(objectType);
		query.append('(');
		if (!params.isEmpty()) {
			for (int i = 1; i <= params.size(); i++) {
				query.append(PARAM);
			}
			query.deleteCharAt(query.length() - 1);
		}
		query.append(')');

		return query;

	}

	/**
	 * Utility method to build the Query for procedure call.
	 *
	 * @param params     the params
	 * @param objectType the object type
	 * @return the string builder
	 */
	public static StringBuilder buildFunctionQuery(List<String> params, String objectType) {
		StringBuilder query = new StringBuilder("select ");
		query.append(objectType);
		query.append('(');
		if (!params.isEmpty()) {
			for (int i = 1; i <= params.size(); i++) {
				query.append(PARAM);
			}
			query.deleteCharAt(query.length() - 1);
		}
		query.append(") from dual");

		return query;

	}
	
	/**
	 * This method will build the initial characters required for the insert query.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @return query
	 * @throws SQLException the SQL exception
	 */
	public static StringBuilder buildInitialQuery(Connection con, String objectTypeId) throws SQLException {
		StringBuilder query = new StringBuilder(QUERY_INITIAL);
		query.append(objectTypeId);
		query.append('(');
		DatabaseMetaData md = con.getMetaData();
		try (ResultSet resultSet = md.getColumns(null, null, objectTypeId, null)) {
			while (resultSet.next()) {
				query.append(resultSet.getString(COLUMN_NAME));
				query.append(',');
			}
		}
		query.deleteCharAt(query.length() - 1);
		query.append(QUERY_VALUES);
		return query;

	}


	/**
	 * Validate the schema name.
	 * 
	 * @param connection               the connection
	 * @param OracleDatabaseConnection the object type
	 * @param schemaName
	 */

	public static void setSchemaName(Connection con, OracleDatabaseConnection conn, String schemaName)
			throws SQLException {
		if (schemaName != null && schemaName.isEmpty() && !conn.getSchemaName().isEmpty()) {
			schemaName = conn.getSchemaName();
		}
		if (!StringUtil.isEmpty(schemaName)) {
			con.setSchema(schemaName);
		}

	}

	/**
	 * Builds the Insert Query Required of Prepared Statement in Dynamic Insert.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 */
	@SuppressWarnings({"java:S3776"})
	public static StringBuilder buildInitialPstmntInsert(Connection con, String objectTypeId) throws SQLException {
		StringBuilder query = new StringBuilder(QUERY_INITIAL);
		query.append(objectTypeId).append('(');
		DatabaseMetaData md = con.getMetaData();
		boolean nestedTable = false;
		int paramCount = 0;
		try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), objectTypeId, null)) {
			while (resultSet.next()) {
				paramCount++;
				query.append(resultSet.getString(COLUMN_NAME));
				query.append(',');
				if (resultSet.getString(DATA_TYPE).equals(ARRAY_TYPE)
						|| resultSet.getString(DATA_TYPE).equals(NESTED_TABLE)) {
					nestedTable = true;
				}
			}
		}
		query.deleteCharAt(query.length() - 1);
		query.append(QUERY_VALUES);
		boolean removeComma = true;
		boolean removeVarrayComma = true;
		boolean varray = false;
		if (nestedTable) {
			removeComma = false;
			boolean removeCommaInnerTable = true;
			try (ResultSet resultSet = md.getColumns(null, null, objectTypeId, null)) {
				while (resultSet.next()) {
					if (resultSet.getString(DATA_TYPE).equals(ARRAY_TYPE)
							|| resultSet.getString(DATA_TYPE).equals(NESTED_TABLE)) {
						ArrayDescriptor arraydes1 = ArrayDescriptor.createDescriptor(
								con.getMetaData().getUserName() + DOT + resultSet.getString(TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
						boolean type1 = QueryBuilderUtil.checkArrayDataType(arraydes1);
						if(type1) {
							varray = true;
							query.append(PARAM);
						} else {
							varray = false;
							query.append(resultSet.getString(TYPE_NAME));
							query.append('(').append(
									arraydes1.getBaseName().replaceAll(con.getMetaData().getUserName() + DOT, ""))
									.append('(');
							StructDescriptor struct1 = StructDescriptor.createDescriptor(arraydes1.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
							ResultSetMetaData rsmd1 = struct1.getMetaData();
							for (int j = 1; j <= rsmd1.getColumnCount(); j++) {
								if (rsmd1.getColumnType(j) == 2003) {
									removeCommaInnerTable = false;

									ArrayDescriptor arraydes2 = ArrayDescriptor
											.createDescriptor(rsmd1.getColumnTypeName(j), SchemaBuilderUtil.getUnwrapConnection(con));
									boolean type2 = QueryBuilderUtil.checkArrayDataType(arraydes2);
									if(type2) {
										query.append(PARAM);
										removeCommaInnerTable = true;
										removeVarrayComma = true;
									}

									else {
										query.append(rsmd1.getColumnTypeName(j)
												.replaceAll(con.getMetaData().getUserName() + DOT, "") + '(');
										StructDescriptor struct2 = StructDescriptor
												.createDescriptor(arraydes2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
										query.append(arraydes2.getBaseName()
												.replaceAll(con.getMetaData().getUserName() + DOT, "") + '(');
										ResultSetMetaData rsmd2 = struct2.getMetaData();

										for (int k = 1; k <= rsmd2.getColumnCount(); k++) {
											if (rsmd2.getColumnType(k) == 2003) {
												ArrayDescriptor arraydes3 = ArrayDescriptor
														.createDescriptor(rsmd2.getColumnTypeName(k), SchemaBuilderUtil.getUnwrapConnection(con));
												boolean type3 = QueryBuilderUtil.checkArrayDataType(arraydes3);
												if(type3){
													query.append(PARAM);
												} else {
													throw new ConnectorException("Nested table level exhasted!!!");
												}
											} else {
												query.append(PARAM);
											}
										}

										query.deleteCharAt(query.length() - 1);
										query.append("))");
									}
								} else {
									query.append(PARAM);
								}
							}
							if (removeCommaInnerTable) {
								query.deleteCharAt(query.length() - 1);
							}
							query.append("))");
							query.append(",");
						}
					} else {
						query.append(PARAM);
					}
					if (removeComma) {
						query.deleteCharAt(query.length() - 1);
					}

				}
			}
		} else {
			for (int i = 1; i <= paramCount; i++) {
				query.append(PARAM);
			}
		}
		if (removeComma || removeVarrayComma || varray) {
			query.deleteCharAt(query.length() - 1);
		}
		query.append(')');
		return query;

	}

	/**
	 * Gets the type name of the given parameter of the procedure.
	 *
	 * @param con           the con
	 * @param procedureName the procedure name
	 * @param aurgumentName the aurgument name
	 * @return the type name
	 * @throws SQLException the SQL exception
	 */
	public static String getTypeName(Connection con, String procedureName1, String aurgumentName) throws SQLException {
		String procedureName = SchemaBuilderUtil.getProcedureName(procedureName1);
		String typeName = null;
		StringBuilder query = new StringBuilder(
				"SELECT TYPE_NAME, TYPE_OWNER, DATA_TYPE FROM SYS.ALL_ARGUMENTS WHERE OBJECT_NAME = ? AND ARGUMENT_NAME = ?");
		try (PreparedStatement psmnt = con.prepareStatement(query.toString())) {
			psmnt.setString(1, procedureName.toUpperCase());
			psmnt.setString(2, aurgumentName.toUpperCase());
			try(ResultSet rs = psmnt.executeQuery();){
			rs.next();
			typeName = rs.getString(OracleDatabaseConstants.TYPE_OWNER).toUpperCase() + "."+rs.getString(OracleDatabaseConstants.TYPE_NAME);
			}
		}
		return typeName;
	}
	
	/**
	 * This method will build the initial characters required for the insert query.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @param schemaName the schema name
	 * @return query
	 * @throws SQLException the SQL exception
	 */
	public static StringBuilder buildInitialQuery(Connection con, String objectTypeId, String schemaName) throws SQLException {
		StringBuilder query = new StringBuilder(QUERY_INITIAL + objectTypeId + "(");
		DatabaseMetaData md = con.getMetaData();
		try (ResultSet resultSet = md.getColumns(con.getCatalog(), schemaName,
				objectTypeId, null);) {
			while (resultSet.next()) {
				query.append(resultSet.getString(COLUMN_NAME));
				query.append(",");
			}
		}
		query.deleteCharAt(query.length() - 1);
		query.append(QUERY_VALUES);
		return query;

	}

	/**
	 * This method will check the Datatype of the key and append the JsonNode values
	 * to the prepared statement accordingly.
	 *
	 * @param preparedStatement    the preparedStatement
	 * @param dataTypes the data types
	 * @param key       the key
	 * @param fieldName the field name
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 * @throws ParseException 
	 */
	public static void checkDataType(PreparedStatement preparedStatement, Map<String, String> dataTypes, String key,
			JsonNode fieldName, int i) throws SQLException, IOException {

		switch (dataTypes.get(key)) {
		case INTEGER:
			setIntegerDatatype(preparedStatement, fieldName, i);
			break;
		case DATE:
			setDateDatatype(preparedStatement, fieldName, i);
			break;
		case STRING:
			setStringDatatype(preparedStatement, fieldName, i);
			break;
		case NVARCHAR:
			setNvarcharDatatype(preparedStatement, fieldName, i);
			break;
		case TIME:
			setTimeDatatype(preparedStatement, fieldName, i);
			break;
		case BOOLEAN:
			setBooleanDatatype(preparedStatement, fieldName, i);
			break;
		case LONG:
			setLongDatatype(preparedStatement, fieldName, i);
			break;
		case FLOAT:
			setFloatDatatype(preparedStatement, fieldName, i);
			break;
		case DOUBLE:
			setDoubleDatatype(preparedStatement, fieldName, i);
			break;
		case BLOB:
			setBlobDatatype(preparedStatement, fieldName, i);
			break;
		case TIMESTAMP:
			setTimestampDataTypes(preparedStatement, fieldName, i);
			break;
		default:
			break;
		}

	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimestampDataTypes(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			timeStampDataType(preparedStatement, i, fieldName);
		} else {
			preparedStatement.setNull(i, Types.TIMESTAMP);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void setBlobDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i)
			throws SQLException, IOException {
		if (fieldName != null && !fieldName.isNull()) {
			try (InputStream stream = new ByteArrayInputStream(
					fieldName.toString().replace(BACKSLASH, "").getBytes());) {
				preparedStatement.setBlob(i, stream);
			}
		} else {
			preparedStatement.setNull(i, Types.BLOB);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setDouble(i, Double.parseDouble(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			preparedStatement.setNull(i, Types.DECIMAL);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setFloat(i, Float.parseFloat(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			preparedStatement.setNull(i, Types.FLOAT);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setLong(i, Long.parseLong(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			preparedStatement.setNull(i, Types.BIGINT);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			boolean flag = Boolean.parseBoolean(fieldName.toString().replace(BACKSLASH, ""));
			preparedStatement.setBoolean(i, flag);
		} else {
			preparedStatement.setNull(i, Types.BOOLEAN);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setTime(i, Time.valueOf(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			preparedStatement.setNull(i, Types.TIME);
		}
	}
	
/**
 * 
 * @param preparedStatement
 * @param fieldName
 * @param i
 * @throws SQLException
 */
	private static void setNvarcharDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setString(i, StringEscapeUtils.unescapeJava(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			preparedStatement.setNull(i, Types.NVARCHAR);
		}
	}

	/**
	 * Sets a string value to the provided PreparedStatement at the specified index.
	 * If the fieldName is not null and not a null JSON node, it unescapes the string
	 * value from the node and sets it as a parameter. If the fieldName is null or is
	 * a null JSON node, it sets a NULL value for the parameter.
	 *
	 * @param preparedStatement the PreparedStatement to which the value is set
	 * @param fieldName the JSON node containing the string value to be set
	 * @param i the index of the parameter in the PreparedStatement
	 * @throws SQLException if a database access error occurs
	 */
	private static void setStringDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setString(i, fieldName.asText());
		} else {
			preparedStatement.setNull(i, Types.VARCHAR);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			preparedStatement.setString(i, fieldName.toString().replace(BACKSLASH, ""));
		} else {
			preparedStatement.setNull(i, Types.DATE);
		}
	}

	/**
	 * 
	 * @param preparedStatement
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerDatatype(PreparedStatement preparedStatement, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null && !fieldName.isNull()) {
			BigDecimal num = new BigDecimal(fieldName.toString().replace(BACKSLASH, ""));
			preparedStatement.setBigDecimal(i, num);
		} else {
			preparedStatement.setNull(i, Types.INTEGER);
		}
	}

	/**
	 * This method is to replace '*' with SQL wildcard '%' if the given pattern is neither empty nor null
	 * @param pattern Store Procedure Pattern
	 * @return updated string
	 */
	public static String replaceWithSqlWildCards(String pattern) {
		return StringUtil.fastReplace(StringUtil.trimToNull(pattern), "*", "%");
	}

	/**
	 * 
	 * @param arrayLevel
	 * @return
	 * @throws SQLException
	 */
	public static boolean checkArrayDataType(ArrayDescriptor arrayLevel) throws SQLException{
	return arrayLevel.getBaseName().equals(NUMBER) || arrayLevel.getBaseName().equals(VARCHAR)
			|| arrayLevel.getBaseName().equalsIgnoreCase(DATE)
			|| arrayLevel.getBaseName().equalsIgnoreCase(CHAR)
			|| arrayLevel.getBaseName().equalsIgnoreCase(TIME)
			|| arrayLevel.getBaseName().equalsIgnoreCase(FLOAT)
			|| arrayLevel.getBaseName().equalsIgnoreCase(NCHAR)
			|| arrayLevel.getBaseName().equalsIgnoreCase(NVARCHAR)
			|| arrayLevel.getBaseName().equalsIgnoreCase(TIMESTAMP)
			|| arrayLevel.getBaseName().equalsIgnoreCase(LONGVARCHAR)
			|| arrayLevel.getBaseName().equalsIgnoreCase(INTEGER)
			|| arrayLevel.getBaseName().equalsIgnoreCase(TINYINT)
			|| arrayLevel.getBaseName().equalsIgnoreCase(SMALLINT)
			|| arrayLevel.getBaseName().equalsIgnoreCase(DOUBLE)
			|| arrayLevel.getBaseName().equalsIgnoreCase(DECIMAL)
			|| arrayLevel.getBaseName().equalsIgnoreCase(NUMERIC);
		}
	
	
	/**
	 * This method will check for the valid timestamp format.
	 *
	 * @param bstmnt    the bstmnt
	 * @param i the 9
	 * @param fieldName       the fieldName
	 * @throws SQLException 
	 */
	public static void timeStampDataType(PreparedStatement bstmnt,int i, JsonNode fieldName) throws SQLException {
		   SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
		   SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
		   String reformattedStr = null;

		   try {
			   reformattedStr  = myFormat.format(fromUser.parse(fieldName.toString().replace(BACKSLASH, "")));
		   } catch (ParseException e) {
			   SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
			   SimpleDateFormat myFormat2 = new SimpleDateFormat(DATETIME_FORMAT);
			   try {
				   reformattedStr  = myFormat2.format(fromUser1.parse(fieldName.toString().replace(BACKSLASH, "")));
			   } catch (ParseException e1) {
			   reformattedStr = fieldName.toString().replace(BACKSLASH, "");
			   }
		   }
		   bstmnt.setString(i,reformattedStr);
}
	
	
	
	/**
	 * This method will check for the valid timestamp format.
	 *
	 * @param bstmnt    the bstmnt
	 * @param i the i
	 * @param fieldName       the fieldName
	 * @throws SQLException 
	 */
	public static void timeStampDataType(PreparedStatement bstmnt,int i, String fieldName) throws  SQLException {
		   SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
		   SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
		   String reformattedStr = null;

		   try {
			   reformattedStr  = myFormat.format(fromUser.parse(fieldName.replace(BACKSLASH, "")));
		   } catch (ParseException e) {
			   SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
			   SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME_FORMAT);
			   fromUser1.setTimeZone(TimeZone.getTimeZone("GMT"));
			   try {
				   reformattedStr  = myFormat1.format(fromUser1.parse(fieldName.replace(BACKSLASH, "")));
			   } catch (ParseException e1) {
			   reformattedStr = fieldName.replace(BACKSLASH, "");
			   }
		   }
		   bstmnt.setString(i, reformattedStr);
}
	
	
	/**
	 * This method will check for the valid timestamp format.
	 *
	 * @param bstmnt    the bstmnt
	 * @param i the 9
	 * @param fieldName       the fieldName
	 * @throws SQLException 
	 */
	public static void timeStampDataType(PreparedStatement bstmnt,int i, Map.Entry<String, Object> fieldName) throws  SQLException {

		   SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
		   SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
		   String reformattedStr = null;

		   try {
			   reformattedStr  = myFormat.format(fromUser.parse(fieldName.getValue().toString().replace(BACKSLASH, "")));
		   } catch (ParseException e) {
			   SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
			   SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME_FORMAT);
			   try {
				   reformattedStr  = myFormat1.format(fromUser1.parse(fieldName.getValue().toString().replace(BACKSLASH, "")));
			   } catch (ParseException e1) {
			   reformattedStr = fieldName.getValue().toString().replace(BACKSLASH, "");
			   }
		   }
		   bstmnt.setString(i, reformattedStr);

	}
	
	/**
	 * This method will check for the valid timestamp format.
	 * @param node    the node
	 *return String
	 */
	public static String timeStampNestedType(JsonNode node) {

		   SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
		   SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
		   String reformattedStr = null;

		   try {
			   reformattedStr  = myFormat.format(fromUser.parse(node.toString().replace(BACKSLASH, "")));
		   } catch (ParseException e) {
			   SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
			   SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME_FORMAT);
			   try {
				   reformattedStr  = myFormat1.format(fromUser1.parse(node.toString().replace(BACKSLASH, "")));
			   } catch (ParseException e1) {
			   reformattedStr = node.toString().replace(BACKSLASH, "");
			   }
		   }
		   return reformattedStr;

	}
	
	
	/**
	 * This method will check for the valid timestamp format.
	 *
	 * @param node    the node
	 * @param i the i
	 * @param csmt  the csmt
	 * @param type the type
	 */
	public static String setDateTimeStampVarrayType(JsonNode node) {
	String reformattedStr = null;
		try {
				 reformattedStr = setDateTimeStampVarrayType3(node);
		} catch (Exception e) {
			return node.toString().replace(BACKSLASH, "");
		}
		return reformattedStr;
	}

	/**
	 * 
	 * @param node
	 * @return
	 */
	private static String setDateTimeStampVarrayType3(JsonNode node) {
		String reformattedStr;
		SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT);
		   SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME);
		   try {
			   reformattedStr  = myFormat1.format(fromUser1.parse(node.toString().replace(BACKSLASH, "")));
		   } catch (ParseException e) {
			   SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME_FORMAT1);
			   SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME);
			   reformattedStr = setDateTimStampVarrayType1(node, fromUser, myFormat);
		   }
		return reformattedStr;
	}

	
	/**
	 * 
	 * @param node
	 * @param fromUser
	 * @param myFormat
	 * @return
	 */
	private static String setDateTimStampVarrayType1(JsonNode node, SimpleDateFormat fromUser,
			SimpleDateFormat myFormat) {
		String reformattedStr;
		try {
			   reformattedStr  = myFormat.format(fromUser.parse(node.toString().replace(BACKSLASH, "")));
		   } catch (ParseException e1) {
			   reformattedStr = setDateTimStampVarrayType2(node);
		   }
		return reformattedStr;
	}

	
	/**
	 * 
	 * @param node
	 * @return
	 */
	private static String setDateTimStampVarrayType2(JsonNode node) {
		String reformattedStr;
		SimpleDateFormat fromUser2 = new SimpleDateFormat(DATETIME_FORMAT2);
		   SimpleDateFormat myFormat2 = new SimpleDateFormat(DATETIME);
		   try {
			   reformattedStr  = myFormat2.format(fromUser2.parse(node.toString().replace(BACKSLASH, "")));
		   } catch (ParseException e3) {
			   reformattedStr = node.toString().replace(BACKSLASH, "");
		   }
		return reformattedStr;
	}
	

	/**
	 * This method will check the Datatype of the key and append the values to the
	 * prepared statement accordingly.
	 *
	 * @param bstmnt    the bstmnt
	 * @param dataTypes the data types
	 * @param key       the key
	 * @param fieldName the field name
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	public static void checkDataType(PreparedStatement bstmnt, Map<String, String> dataTypes, String key,
			String fieldName, int i) throws SQLException, IOException {

		switch (dataTypes.get(key)) {
		case INTEGER:
			setIntegerDatatype(bstmnt, fieldName, i);
			break;
		case DATE:
			setDateDatatype(bstmnt, fieldName, i);
			break;
		case STRING:
			setStringDatatype(bstmnt, fieldName, i);
			break;
		case NVARCHAR:
			setNvarcharDatatype(bstmnt, fieldName, i);
			break;
		case TIME:
			setTimeDatatype(bstmnt, fieldName, i);
			break;
		case BOOLEAN:
			setBooleanDatatype(bstmnt, fieldName, i);
			break;
		case LONG:
			setLongDaatype(bstmnt, fieldName, i);
			break;
		case FLOAT:
			setFloatDatatype(bstmnt, fieldName, i);
			break;
		case DOUBLE:
			setDoubleDatatype(bstmnt, fieldName, i);
			break;
		case BLOB:
			setBlobDatatype(bstmnt, fieldName, i);
			break;
		case TIMESTAMP:
			setTimestampDatatype(bstmnt, fieldName, i);
			break;
		default:
			break;
		}

	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimestampDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
				timeStampDataType(bstmnt,i,fieldName);
			
		} else {
			bstmnt.setNull(i, Types.TIMESTAMP);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void setBlobDatatype(PreparedStatement bstmnt, String fieldName, int i)
			throws SQLException, IOException {
		if (fieldName != null) {
			String value = fieldName.replace(BACKSLASH, "");
			try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
				bstmnt.setBlob(i, stream);
			}
		} else {
			bstmnt.setNull(i, Types.BLOB);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			double value = Double.parseDouble(fieldName.replace(BACKSLASH, ""));
			bstmnt.setDouble(i, value);
		} else {
			bstmnt.setNull(i, Types.DECIMAL);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			float value = Float.parseFloat(fieldName.replace(BACKSLASH, ""));
			bstmnt.setFloat(i, value);
		} else {
			bstmnt.setNull(i, Types.FLOAT);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongDaatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			long value = Long.parseLong(fieldName.replace(BACKSLASH, ""));
			bstmnt.setLong(i, value);
		} else {
			bstmnt.setNull(i, Types.BIGINT);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			boolean flag = Boolean.parseBoolean(fieldName);
			bstmnt.setBoolean(i, flag);
		} else {
			bstmnt.setNull(i, Types.BOOLEAN);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setTime(i, Time.valueOf(fieldName));
		} else {
			bstmnt.setNull(i, Types.TIME);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setNvarcharDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldName));
		} else {
			bstmnt.setNull(i, Types.VARCHAR);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setStringDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setString(i, fieldName);
		} else {
			bstmnt.setNull(i, Types.VARCHAR);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setString(i, fieldName);
		} else {
			bstmnt.setNull(i, Types.DATE);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerDatatype(PreparedStatement bstmnt, String fieldName, int i) throws SQLException {
		if (fieldName != null) {
			BigDecimal num = new BigDecimal(fieldName.replace(BACKSLASH, ""));
			bstmnt.setBigDecimal(i, num);
		} else {
			bstmnt.setNull(i, Types.INTEGER);
		}
	}

	/**
	 * This method will check the Datatype of the key and append the values to the
	 * prepared statement accordingly.
	 *
	 * @param pstmnt    the pstmnt
	 * @param dataTypes the data types
	 * @param key       the key
	 * @param entry     the entry
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	public static void checkDataType(PreparedStatement pstmnt, Map<String, String> dataTypes, String key,
			Map.Entry<String, Object> entry, int i) throws SQLException, IOException {
		switch (dataTypes.get(key)) {
		case INTEGER:
			BigDecimal num = new BigDecimal(entry.getValue().toString());
			pstmnt.setBigDecimal(i, num);
			break;
		case STRING:
			String varchar = (String) entry.getValue();
			pstmnt.setString(i, varchar);
			break;
		case NVARCHAR:
			String nVarchar = (String) entry.getValue();
			pstmnt.setString(i, StringEscapeUtils.unescapeJava(nVarchar));
			break;
		case DATE:
			String date = (String) entry.getValue();
			pstmnt.setString(i, date);
			break;
		case TIME:
			String time = (String) entry.getValue();
			pstmnt.setTime(i, Time.valueOf(time));
			break;
		case BOOLEAN:
			Boolean flag = (Boolean) entry.getValue();
			pstmnt.setBoolean(i, flag);
			break;
		case LONG:
			long num1 = Long.parseLong(entry.getValue().toString());
			pstmnt.setLong(i, num1);
			break;
		case FLOAT:
			float flt = Float.parseFloat(entry.getValue().toString());
			pstmnt.setFloat(i, flt);
			break;
		case DOUBLE:
			double dbl = Double.parseDouble(entry.getValue().toString());
			pstmnt.setDouble(i, dbl);
			break;
		case TIMESTAMP:
				timeStampDataType(pstmnt,i,entry);
			break;
		case BLOB:
			String value = entry.getValue().toString().replace(BACKSLASH, "");
			try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
				pstmnt.setBlob(i, stream);
			}

			break;
		default:
			break;
		}
	}

	/**
	 * Validate the table name.
	 *
	 * @param finalQuery the final query
	 * @return the string
	 * @throws IndexOutOfBoundsException the index out of bounds exception
	 */
	public static String validateTheTableName(String finalQuery) {
		int findIndex = finalQuery.lastIndexOf("FROM");
		String tep = finalQuery.substring(findIndex);
		if (tep.indexOf(" ") >= 0) {
			tep = tep.substring(tep.indexOf(" ") + 1);
			if (tep.indexOf(" ") >= 0) {
				finalQuery = tep.substring(0, tep.indexOf(" "));
			}
			else {
				finalQuery = tep;
			}
		}
		if (finalQuery.contains(".")) {
			finalQuery = finalQuery.substring(finalQuery.indexOf(DOT) + 1, finalQuery.length());
		}
		return finalQuery;
	}
	
	/**
	 * This method will convert time out in seconds
	 * @param readTimeout
	 * @return int
	 */
	public static int convertMsToSeconds(int timeOut) {
		if(timeOut == 0) {
			return 0;
		}
		else if(timeOut<1000) {
			return 1;
		}
		else {
			float seconds = (float) (timeOut/1000.0);
			return Math.round(seconds);
		}
	}

}
