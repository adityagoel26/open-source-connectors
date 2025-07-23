// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.get;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.text.StringEscapeUtils;
import org.json.simple.parser.ParseException;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.boomi.connector.oracledatabase.util.CustomPayloadUtil;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.util.RequestUtil;
import com.boomi.connector.oracledatabase.util.SchemaBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * The Class StandardGetOperation.
 *
 * @author swastik.vn
 */
public class StandardGetOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(StandardGetOperation.class.getName());

	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	/**
	 * Instantiates a new standard get operation.
	 *
	 * @param connection the connection
	 */
	public StandardGetOperation(OracleDatabaseConnection connection) {
		super(connection);
	}

	/**
	 * Execute update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		OracleDatabaseConnection conn = getConnection();
		String query = getContext().getOperationProperties().getProperty(OracleDatabaseConstants.QUERY, "");
		Long maxRows = getContext().getOperationProperties().getLongProperty(OracleDatabaseConstants.MAX_ROWS);
		Long maxFieldSize = getContext().getOperationProperties().getLongProperty("maxFieldSize");
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			this.executeStatements(con, request, response, maxRows, query, maxFieldSize);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * This method will process the Prepared Statements based on the requests and
	 * query provided in the Sql Query field.
	 *
	 * @param con          the con
	 * @param trackedData  the tracked data
	 * @param response     the response
	 * @param maxRows      the max rows
	 * @param query        the query
	 * @param maxFieldSize the max field size
	 * @throws SQLException the SQL exception
	 */
	private void executeStatements(Connection con, UpdateRequest trackedData, OperationResponse response, Long maxRows,
			String query, Long maxFieldSize) throws SQLException {
		DatabaseMetaData databaseMetaData = con.getMetaData();

		boolean checkInClause = getContext().getOperationProperties().getBooleanProperty("INClause", false);
		for (ObjectData objdata : trackedData) {
			try (InputStream is = objdata.getData()) {
				// Here we are storing the Object data in MAP, Since the input request is not
				// having the fixed number of fields and Keys are unknown to extract the Json
				// Values.
				JsonNode userData = RequestUtil.getJsonData(is);
				if (userData != null) {
					ExecutionParameters executionParameters = new ExecutionParameters(con, response, objdata);
					executeIfUserData(executionParameters, maxRows, query, maxFieldSize, databaseMetaData, checkInClause, userData);
				} else if (query != null) {
					try (NamedParameterStatement pstmnt = new NamedParameterStatement(con, query)) {
						pstmnt.setReadTimeout(QueryBuilderUtil.convertMsToSeconds(getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
						executeQuery(pstmnt, maxRows, objdata, response, maxFieldSize, con);
					}
				} else {
					throw new ConnectorException("Please enter SQL Statement");
				}
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (Exception e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			}
		}
		logger.info("Statements processed Successfully!!");

	}

	private void executeIfUserData(ExecutionParameters executionParameters, Long maxRows, String query, Long maxFieldSize, DatabaseMetaData databaseMetaData, boolean checkInClause, JsonNode userData) throws SQLException, IOException {
		Connection con = executionParameters.getCon();
		OperationResponse response = executionParameters.getResponse();
		ObjectData objdata = executionParameters.getObjdata();

		String finalQuery = this.getQuery(query, userData);
		if (finalQuery != null && !finalQuery.trim().isEmpty()) {
			Map<String, String> dataTypes = this.getDataTypes(con, getContext().getObjectTypeId(),
					finalQuery);
			if (checkInClause) {
				executeQueryForINClause(finalQuery, con, objdata, dataTypes, maxFieldSize, maxRows,
						response);
			} else if (finalQuery.toUpperCase().contains("IN(") || finalQuery.toUpperCase().contains("IN (")) {
				throw new ConnectorException("Kindly select the IN clause check box!");
			} else {
				ExecutionParameters executionParametersToCheckForStatements = new ExecutionParameters(con, response, objdata, dataTypes);
				checkForStatement(executionParametersToCheckForStatements, maxRows, maxFieldSize, databaseMetaData, userData,
						finalQuery);
			}
		} else {
			throw new ConnectorException("Please enter SQL Statement");
		}
	}

	private void checkForStatement(ExecutionParameters executionParameters, Long maxRows, Long maxFieldSize,
			DatabaseMetaData databaseMetaData, JsonNode userData, String finalQuery) throws SQLException, IOException {
		Connection con =executionParameters.getCon();
		OperationResponse response = executionParameters.getResponse();
		ObjectData objdata = executionParameters.getObjdata();
		Map<String, String> dataTypes = executionParameters.getDataTypes();

		if (dataTypes.containsKey("NVARCHAR") && databaseMetaData.getDatabaseProductName()
				.equals(OracleDatabaseConstants.MSSQL)) {
			StringBuilder finalQueryString = new StringBuilder(finalQuery.toUpperCase());
			finalQueryString.append(" FOR JSON AUTO");
			finalQuery = finalQueryString.toString();
		}
		try (NamedParameterStatement pstmnt = new NamedParameterStatement(con, finalQuery)) {
			this.prepareStatement(con, userData, dataTypes, pstmnt);
			pstmnt.setReadTimeout(QueryBuilderUtil.convertMsToSeconds(getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
			executeQuery(pstmnt, maxRows, objdata, response, maxFieldSize, con);
		}
	}

	/**
	 * Builds the final query based on the EXEC(
	 *
	 * @param query    the query
	 * @param userData the user data
	 * @return the query
	 */
	private String getQuery(String query, JsonNode userData) {
		return userData.get(OracleDatabaseConstants.SQL_QUERY) == null ? query
				: userData.get(OracleDatabaseConstants.SQL_QUERY).toString().replace(BACKSLASH, "");

	}

	/**
	 * Gets the data types for each columns associated with the tables/table.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @param finalQuery   the final query
	 * @return the data types
	 * @throws SQLException the SQL exception
	 */
	private Map<String, String> getDataTypes(Connection con, String objectTypeId, String finalQuery)
			throws SQLException {
		Map<String, String> dataTypes = new HashMap<>();
		if (objectTypeId.contains(",")) {
			String[] tableNames = SchemaBuilderUtil.getTableNames(objectTypeId);
			for (String tableName : tableNames) {
				dataTypes.putAll(MetadataUtil.getDataTypesWithTable(con, tableName.trim()));
			}
		} else {
			if (!finalQuery.toUpperCase().contains(OPEN_EXEC)) {
				String tableNameInQuery = QueryBuilderUtil.validateTheTableName(finalQuery.toUpperCase());
				if (!tableNameInQuery.equalsIgnoreCase(getContext().getObjectTypeId())) {
					throw new ConnectorException(
							"The table name used in the query does not match with Object Type selected!");
				}
			}
			dataTypes.putAll(new MetadataUtil(con, getContext().getObjectTypeId()).getDataType());

		}
		return dataTypes;
	}

	/**
	 * Execute query for IN clause.
	 *
	 * @param finalINQuery the final IN query
	 * @param con          the con
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @param maxFieldSize the max field size
	 * @param maxRows      the max rows
	 * @param response     the response
	 * @param query
	 * @throws SQLException   the SQL exception
	 * @throws IOException    Signals that an I/O exception has occurred.
	 * @throws ParseException the parse exception
	 */
	private void executeQueryForINClause(String finalINQuery, Connection con, ObjectData objdata,
			Map<String, String> dataTypes, Long maxFieldSize, Long maxRows, OperationResponse response)
			throws SQLException, IOException {
		DatabaseMetaData databaseMetaData = con.getMetaData();
		if (finalINQuery != null) {
			if (dataTypes.containsKey("NVARCHAR")
					&& databaseMetaData.getDatabaseProductName().equals(OracleDatabaseConstants.MSSQL)) {
				finalINQuery = finalINQuery+" FOR JSON AUTO";
			}
			try (NamedParameterStatement pstmnt = new NamedParameterStatement(con, finalINQuery, objdata)) {
				pstmnt.setReadTimeout(QueryBuilderUtil.convertMsToSeconds(getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
				this.inClausePreparedStatement(objdata, dataTypes, pstmnt, con);
				executeQuery(pstmnt, maxRows, objdata, response, maxFieldSize, con);
			}

		}

	}

	/**
	 * In clause prepared statement.
	 *
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param bstmnt    the bstmnt
	 * @param con       the con
	 * @param query
	 * @throws IOException    Signals that an I/O exception has occurred.
	 * @throws SQLException   the SQL exception
	 * @throws ParseException the parse exception
	 */
	private void inClausePreparedStatement(ObjectData objdata, Map<String, String> dataTypes,
			NamedParameterStatement bstmnt, Connection con)
			throws IOException, SQLException {
		try (InputStream is = objdata.getData()) {
			JsonNode json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				String databaseName = md.getDatabaseProductName();
				int i = 0;
				Iterator<String> inputKeys = json.fieldNames();
				while (inputKeys.hasNext()) {
					String key = inputKeys.next();
					if (!key.equalsIgnoreCase(SQL_QUERY)) {
						i++;
						this.validateDataType(dataTypes, key);
						JsonNode value = json.get(key);
						switch (dataTypes.get(key)) {
						case INTEGER:
							setIntegerArray(bstmnt, json, i, key, value);
							break;
						case DATE:
							setDateArray(bstmnt, json, databaseName, i, key, value);
							break;
						case STRING:
							setStringArray(bstmnt, json, i, key, value);
							break;
						case TIME:
							setTimeArray(bstmnt, json, databaseName, i, key, value);
							break;
						case NVARCHAR:
							setNvarcharArray(bstmnt, json, i, key, value);
							break;
						case LONG:
							setLongArray(bstmnt, json, i, key, value);
							break;
						case FLOAT:
							setFloatArray(bstmnt, json, i, key, value);
							break;
						case DOUBLE:
							setDoubleArray(bstmnt, json, i, key, value);
							break;
						case BLOB:
							setBlobArray(bstmnt, json, i, key, value);
							break;
						case TIMESTAMP:
							setTimestampArray(bstmnt, json, i, key, value);
							break;
						case BOOLEAN:
							setBooleanArray(bstmnt, json, i, key, value);
							break;
							
						default:
							break;
						}
					}
				}
			}
		}

	}

	/**
	 * This method will set Array of booleans for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setBooleanArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		Boolean flag = Boolean.valueOf(json.get(key).toString().replace(BACKSLASH, ""));
		if (value == null) {
			bstmnt.setNull(i, Types.BOOLEAN);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setBooleanArray(key, (ArrayNode) json.get(key));
			} else {
				bstmnt.setBoolean(key, flag);
			}
		}
	}

	/**
	 * This method will set Array of time data for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param databaseName
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setTimeArray(NamedParameterStatement bstmnt, JsonNode json, String databaseName, int i, String key,
			JsonNode value) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.VARCHAR);
		} else {
			String varchar = json.get(key).toString().replace(BACKSLASH, "");
			if (databaseName.equals(MYSQL)) {
				if (json.get(key) instanceof ArrayNode) {
					bstmnt.setStringArray(key, (ArrayNode) json.get(key));
				} else {
					bstmnt.setString(key, varchar);
				}
			} else {
				if (json.get(key) instanceof ArrayNode) {
					bstmnt.setTimeArray(key, (ArrayNode) json.get(key));
				} else {
					bstmnt.setTime(key, Time.valueOf(varchar));
				}
			}
		}
	}

	/**
	 * This method will set Array of strings for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setStringArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.VARCHAR);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setStringArray(key, (ArrayNode) json.get(key));
			} else {
				String varchar = json.get(key).toString().replace(BACKSLASH, "");
				bstmnt.setString(key, varchar);
			}
		}
	}
	
	/**
	 * This method will set Array of Long for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setLongArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.BIGINT);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setLongArray(key, (ArrayNode) json.get(key));
			} else {
				long longValue =  Long.parseLong(json.get(key).toString().replace(BACKSLASH, ""));
				bstmnt.setLong(key, longValue);
			}
		}
	}

	/**
	 * This method will set Array of float for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setFloatArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.FLOAT);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setFloatArray(key, (ArrayNode) json.get(key));
			} else {
				float floatValue = Float.parseFloat(json.get(key).toString().replace(BACKSLASH, ""));
				bstmnt.setFloat(key, floatValue);
			}
		}
	}
	/**
	 * This method will set Array of Double for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setDoubleArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.DOUBLE);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setDoubleArray(key, (ArrayNode) json.get(key));
			} else {
				double doubleValue = Double.parseDouble(json.get(key).toString().replace(BACKSLASH, ""));
				bstmnt.setDouble(key, doubleValue);
			}
		}
	}
	
	/**
	 * This method will set Array of Blob for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 * @throws IOException 
	 */
	private void setBlobArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException, IOException {
		if (value == null) {
			bstmnt.setNull(i, Types.BLOB);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setBlobArray(key, (ArrayNode) json.get(key));
			} else {
				String blobData =json.get(key).toString().replace(BACKSLASH, "");
				bstmnt.setBlob(key, blobData);
			}
		}
	}
	/**
	 * This method will set Array of dates for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param databaseName
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setDateArray(NamedParameterStatement bstmnt, JsonNode json, String databaseName, int i, String key,
			JsonNode value) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.DATE);
		} else {
			if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
				if (json.get(key) instanceof ArrayNode) {
					bstmnt.setStringArray(key, (ArrayNode) json.get(key));
				} else {
					bstmnt.setString(key, json.get(key).toString().replace(BACKSLASH, ""));
				}
			} else {
				if (json.get(key) instanceof ArrayNode) {
					bstmnt.setDateArray(key, (ArrayNode) json.get(key));
				} else {
					bstmnt.setDate(key, Date.valueOf(json.get(key).toString().replace(BACKSLASH, "")));
				}
			}
		}
	}

	/**
	 * This method will set Array of NVarchar for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setNvarcharArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.NVARCHAR);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setNvarcharArray(key, (ArrayNode) json.get(key));
			} else {
				String nvarchar = StringEscapeUtils
						.unescapeJava(json.get(key).toString().replace(BACKSLASH, ""));
				bstmnt.setString(key, nvarchar);
			}
		}
	}
	/**
	 * This method will set Array of T for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setTimestampArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		ArrayNode ar1 = null; 
		if (value == null) {
			bstmnt.setNull(i, Types.TIMESTAMP);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				ar1 = this.timeStampDataType(json,key);
				bstmnt.setStringArray(key, ar1);
			} else {
				String timestamp = json.get(key).toString().replace(BACKSLASH, "");
				bstmnt.setString(key, timestamp);
			}
		}
	}
	
	/**
	 * This method will check for the valid timestamp format.
	 * @param json       the json
	 * @param key       the key
	 * @return ArrayNode
	 */
	public ArrayNode timeStampDataType(JsonNode json, String key) {

		ArrayNode ar = (ArrayNode) json.get(key);
		ArrayNode ar1 = mapper.createArrayNode();
		for (int i = 0; i < ar.size(); i++) {

			SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
			SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
			String reformattedStr = null;
			try {
				reformattedStr = myFormat.format(fromUser.parse(ar.get(i).toString().replace(BACKSLASH, "")));
			} catch (java.text.ParseException e) {
				SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
				SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME_FORMAT);
				try {
					reformattedStr = myFormat1.format(fromUser1.parse(ar.get(i).toString().replace(BACKSLASH, "")));
				} catch (java.text.ParseException e1) {
					reformattedStr = ar.get(i).toString().replace(BACKSLASH, "");
				}

			}
			ar1.add(reformattedStr);
		}
		return ar1;

	}
	
	
	/**
	 * This method will set Array of integers for IN Clause
	 * 
	 * @param bstmnt
	 * @param json
	 * @param i
	 * @param key
	 * @param value
	 * @throws SQLException
	 */
	private void setIntegerArray(NamedParameterStatement bstmnt, JsonNode json, int i, String key, JsonNode value)
			throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.INTEGER);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setIntArray(key, (ArrayNode) json.get(key));
			} else {
				int num = Integer.parseInt(json.get(key).toString().replace(BACKSLASH, ""));
				bstmnt.setInt(key, num);
			}
		}
	}

	/**
	 * This method will validate the Datatypes of the column before setting the
	 * values to Prepared Statement.
	 *
	 * @param dataTypes the data types
	 * @param key       the key
	 */
	private void validateDataType(Map<String, String> dataTypes, String key) {
		if (!dataTypes.containsKey(key)) {
			throw new ConnectorException("Keys Provided in Input Request is not matching the Request Profile");
		}

	}

	/**
	 * This method will set the MaxRows and MaxFieldSize from the operation UI to
	 * the Preparedstatement and it will call processResultSet.
	 *
	 * @param pstmnt       the pstmnt
	 * @param maxRows      the max rows
	 * @param objdata      the objdata
	 * @param response     the response
	 * @param maxFieldSize the max field size
	 * @param con
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private void executeQuery(NamedParameterStatement pstmnt, Long maxRows, ObjectData objdata,
			OperationResponse response, Long maxFieldSize, Connection con) throws SQLException, IOException {
		Long fetchSize = getContext().getOperationProperties().getLongProperty(FETCH_SIZE);
		if (maxRows != null && maxRows > 0) {
			pstmnt.setMaxRows(maxRows.intValue());
		}
		if (maxFieldSize != null && maxFieldSize > 0) {
			pstmnt.setMaxFieldSize(maxFieldSize.intValue());
		}
		if (fetchSize != null && fetchSize > 0) {
			pstmnt.setFetchSize(fetchSize.intValue());
		}
		this.processResultSet(pstmnt, objdata, response, con);

	}

	/**
	 * This method will add the parameters to the Prepared Statements based on the
	 * incoming requests.
	 *
	 * @param con       the con
	 * @param jsonNode  the json node
	 * @param dataTypes the data types
	 * @param pstmnt    the pstmnt
	 * @param query
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private void prepareStatement(Connection con, JsonNode jsonNode, Map<String, String> dataTypes,
			NamedParameterStatement pstmnt) throws SQLException, IOException {
		int i = 0;
		String databasename = con.getMetaData().getDatabaseProductName();
		if (jsonNode != null) {
			for (Iterator<String> fieldName = jsonNode.fieldNames(); fieldName.hasNext();) {
				String key = fieldName.next().trim();
				JsonNode fieldValue = jsonNode.get(key);
				if (!key.equals(SQL_QUERY)) {
					this.validateDataType(dataTypes, key);
					i++;
					ifIntegerDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifStringDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifDateDataType(dataTypes, pstmnt, i, databasename, key, fieldValue);
					ifTimeDataType(dataTypes, pstmnt, i, databasename, key, fieldValue);
					ifBooleanDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifLongDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifFloatDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifDoubleDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifBlobDataType(dataTypes, pstmnt, i, key, fieldValue);
					ifTimestampDataType(dataTypes, pstmnt, i, key, fieldValue);
				}

			}
		}

		logger.log(Level.INFO, "Values appeneded for prepared statement");
	}

	/**
	 * This method will set the boolean to the prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifBooleanDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(BOOLEAN)) {
			if (fieldValue != null) {
				boolean flag = Boolean.parseBoolean(fieldValue.toString().replace(BACKSLASH, ""));
				pstmnt.setBoolean(key, flag);
			} else {
				pstmnt.setNull(i, Types.BOOLEAN);
			}
		}
	}

	/**
	 * This method will set the time to the prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param databasename
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifTimeDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i,
			String databasename, String key, JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(TIME)) {
			if (fieldValue != null) {
				String time = fieldValue.toString().replace(BACKSLASH, "");
				if (databasename.equals(ORACLE) || databasename.equals(MYSQL)) {
					pstmnt.setString(key, time);
				} else {
					pstmnt.setTime(key, Time.valueOf(time));
				}
			} else {
				pstmnt.setNull(i, Types.TIME);
			}

		}
	}

	/**
	 * This method will set the date to the prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param databasename
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifDateDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i,
			String databasename, String key, JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(DATE)) {
			if (fieldValue != null) {
				if (databasename.equals(ORACLE) || databasename.equals(MYSQL)) {
					pstmnt.setString(key, fieldValue.toString().replace(BACKSLASH, ""));
				} else {
					pstmnt.setDate(key, Date.valueOf(fieldValue.toString().replace(BACKSLASH, "")));
				}
			} else {
				pstmnt.setNull(i, Types.DATE);
			}
		}
	}

	/**
	 * This method will set the string to the prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifStringDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(STRING)) {
			if (fieldValue != null) {
				String varchar = fieldValue.toString().replace(BACKSLASH, "");
				pstmnt.setString(key, varchar);
			} else {
				pstmnt.setNull(i, Types.VARCHAR);
			}
		}
	}

	/**
	 * This method will set the integer to prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifIntegerDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(INTEGER)) {
			if (fieldValue != null) {
				int num = Integer.parseInt(fieldValue.toString().replace(BACKSLASH, ""));
				pstmnt.setInt(key, num);
			} else {
				pstmnt.setNull(i, Types.INTEGER);
			}
		}
	}

	
	/**
	 * This method will set the long to prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifLongDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(LONG)) {
			if (fieldValue != null) {
				long longValue = Long.parseLong(fieldValue.toString().replace(BACKSLASH, ""));
				pstmnt.setLong(key, longValue);
			} else {
				pstmnt.setNull(i, Types.BIGINT);
			}
		}
	}
	
	/**
	 * This method will set the float to prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifFloatDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(FLOAT)) {
			if (fieldValue != null) {
				float floatValue = Float.parseFloat(fieldValue.toString().replace(BACKSLASH, ""));
				pstmnt.setFloat(key, floatValue);
			} else {
				pstmnt.setNull(i, Types.FLOAT);
			}
		}
	}
	
	/**
	 * This method will set the double to prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void ifDoubleDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(DOUBLE)) {
			if (fieldValue != null) {
				double doubleValue = Double.parseDouble(fieldValue.toString().replace(BACKSLASH, ""));
				pstmnt.setDouble(key, doubleValue);
			} else {
				pstmnt.setNull(i, Types.DOUBLE);
			}
		}
	}
	
	/**
	 * This method will set the blob to prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 * @throws IOException 
	 */
	private void ifBlobDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException, IOException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(BLOB)) {
			if (fieldValue != null) {
				String blobData = fieldValue.toString().replace(BACKSLASH, "");
					pstmnt.setBlob(key, blobData);
			} else {
				pstmnt.setNull(i, Types.BLOB);
			}
		}
	}
	
	/**
	 * This method will set the Timestamp to prepared statement
	 * 
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param key
	 * @param fieldValue
	 * @throws SQLException 
	 */
	private void ifTimestampDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i, String key,
			JsonNode fieldValue) throws SQLException {
		if (dataTypes.containsKey(key) && dataTypes.get(key).equals(TIMESTAMP)) {
			if (fieldValue != null) {
					this.timeStampDataType(pstmnt,key,fieldValue);
			} else {
				pstmnt.setNull(i, Types.TIMESTAMP);
			}
		}
	}
	
	/**
	 * This method will check for the valid timestamp format.
	 * @param pstmnt       the pstmnt
	 * @param key       the key
	 * @param fieldValue       the fieldValue 
	 * @throws SQLException 
	 */
	public void timeStampDataType(NamedParameterStatement pstmnt, String key, JsonNode fieldValue) throws SQLException {

		SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
		SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
		String reformattedStr = null;
		try {
			reformattedStr = myFormat.format(fromUser.parse(fieldValue.toString().replace(BACKSLASH, "")));
		} catch (java.text.ParseException e) {
			SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
			SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME_FORMAT);
			try {
				reformattedStr = myFormat1.format(fromUser1.parse(fieldValue.toString().replace(BACKSLASH, "")));
			} catch (java.text.ParseException e1) {
				reformattedStr = fieldValue.toString().replace(BACKSLASH, "");
			}
		}
		pstmnt.setString(key, reformattedStr);

	}
	
	
	/**
	 * This method will process the result set and creates the Payload for the
	 * Operation Response.
	 *
	 * @param pstmnt   the pstmnt
	 * @param objdata  the objdata
	 * @param response the response
	 * @param con
	 * @throws IOException 
	 */
	private void processResultSet(NamedParameterStatement pstmnt, ObjectData objdata, OperationResponse response,
			Connection con) throws IOException {
		CustomPayloadUtil load = null;
		try (ResultSet rs = pstmnt.executeQuery()) {
			boolean isBatching = false;
			String cookie = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
			if (cookie != null && !cookie.isEmpty()) {
				JsonNode json = mapper.readTree(cookie);
				JsonNode cookieMap = json.get("documentBatching");
				if (cookieMap != null)
					isBatching = cookieMap.asBoolean();
			}
			Long batchCount = getContext().getOperationProperties().getLongProperty(BATCH_COUNT);
			while (rs.next()) {
				if ((batchCount == null || batchCount == 0) && !isBatching) {
					load = new CustomPayloadUtil(rs, con);
				} else if (batchCount != null && isBatching && batchCount > 0) {
					load = new CustomPayloadUtil(rs, con, batchCount);

				} else {
					throw new ConnectorException("Kindly check the profile details!!");
				}
				response.addPartialResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
						SUCCESS_RESPONSE_MESSAGE, load);
			}
			response.finishPartialResult(objdata);

		} catch (SQLException | IOException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} finally {
			IOUtil.closeQuietly(load);
		}
	}

	/**
	 * Gets the Connection instance.
	 *
	 * @return the connection
	 */
	@Override
	public OracleDatabaseConnection getConnection() {
		return (OracleDatabaseConnection) super.getConnection();
	}

}
