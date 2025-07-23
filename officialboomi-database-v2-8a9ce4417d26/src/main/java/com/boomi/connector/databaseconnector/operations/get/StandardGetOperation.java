// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.get;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.CustomPayloadUtil;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.databaseconnector.util.RequestUtil;
import com.boomi.connector.databaseconnector.util.SchemaBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.BATCH_COUNT;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.BLOB;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.BOOLEAN;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.CLOB;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DATE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DOUBLE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DOUBLE_QUOTE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.EXEC;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.FLOAT;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.INTEGER;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.JSON;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.LONG;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.MYSQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.NVARCHAR;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.OPEN_EXEC;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.ORACLE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.POSTGRESQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SCHEMA_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SQL_QUERY;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.STRING;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TIME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TIMESTAMP;

/**
 * The Class StandardGetOperation.
 *
 * @author swastik.vn
 */
public class StandardGetOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger LOG = Logger.getLogger(StandardGetOperation.class.getName());
	
	private static final String PARAM_REPLACE_REGEX = "'[ ]{0,}\\$%s[ ]{0,}'|\\$%s|\"[ ]{0,}\\$%s[ ]{0,}\"";


	/**
	 * Instantiates a new standard get operation.
	 *
	 * @param databaseConnectorConnection the databaseConnectorConnection
	 */
	public StandardGetOperation(DatabaseConnectorConnection databaseConnectorConnection) {
		super(databaseConnectorConnection);
	}

	/**
	 * Execute update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		String query = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.QUERY, "");
		Long maxRows = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.MAX_ROWS);
		Long maxFieldSize = getContext().getOperationProperties().getLongProperty(
				DatabaseConnectorConstants.MAX_FIELD_SIZE);
		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if (sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			String schemaName = (String) getContext().getOperationProperties()
					.get(DatabaseConnectorConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, databaseConnectorConnection.getSchemaName());
			this.executeStatements(sqlConnection, request, response, maxRows, query, maxFieldSize);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * This method will process the Prepared Statements based on the requests and
	 * query provided in the Sql Query field.
	 *
	 * @param sqlConnection          the sqlConnection
	 * @param trackedData  the tracked data
	 * @param response     the response
	 * @param maxRows      the max rows
	 * @param query        the query
	 * @param maxFieldSize the max field size
	 * @throws SQLException the SQL exception
	 */
    private void executeStatements(Connection sqlConnection, UpdateRequest trackedData, OperationResponse response,
            Long maxRows, String query, Long maxFieldSize) throws SQLException {
		DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
		boolean checkInClause = getContext().getOperationProperties().getBooleanProperty(
				DatabaseConnectorConstants.IN_CLAUSE, false);
		String schemaName = getContext().getOperationProperties()
				.getProperty(SCHEMA_NAME);
		String schema = QueryBuilderUtil.getSchemaFromConnection(databaseMetaData.getDatabaseProductName(),
				sqlConnection,
				schemaName, getConnection().getSchemaName());
		for (ObjectData objdata : trackedData) {
			try (InputStream is = objdata.getData();) {
				// Here we are storing the Object data in MAP, Since the input request is not
				// having the fixed number of fields and Keys are unknown to extract the Json
				// Values.
				JsonNode userData = RequestUtil.getJsonData(is);
				query = setWhereAndOrderBy(query, objdata);
				if (userData != null) {
					String finalQuery = constructFinalQuery(query, objdata, userData);
					if (finalQuery != null && !finalQuery.trim().isEmpty()) {
						Map<String, String> dataTypes = this.getDataTypes(sqlConnection, getContext().getObjectTypeId(),
								finalQuery, schema);
						if ((!checkInClause && inCheck(finalQuery.toUpperCase()))
								|| (checkInClause && !inCheck(finalQuery.toUpperCase()))) {
							throw new ConnectorException(
									"Kindly select the IN clause check box only if Sql Query contains IN Clause !");
						} else if (checkInClause) {
							executeQueryForINClause(finalQuery, sqlConnection, objdata, dataTypes, maxFieldSize,
									response, query);
						} else {
							if (dataTypes.containsKey(NVARCHAR) && databaseMetaData.getDatabaseProductName()
									.equals(DatabaseConnectorConstants.MSSQL)) {
								finalQuery = finalQuery.toUpperCase() + " FOR JSON AUTO";
							}
							try (NamedParameterStatement pstmnt = new NamedParameterStatement(sqlConnection, finalQuery)) {
								prepareStatement(sqlConnection, userData, dataTypes, pstmnt, query);
								pstmnt.setReadTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
										getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut()
												.intValue() : 0));
								executeQuery(pstmnt, maxRows, objdata, response, maxFieldSize);
							}
						}
					} else {
						throw new ConnectorException("Please enter SQL Statement");
					}
				} else if (query != null) {
					try (NamedParameterStatement pstmnt = new NamedParameterStatement(sqlConnection, query)) {
						pstmnt.setReadTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
                                (getConnection().getReadTimeOut() != null) ? getConnection().getReadTimeOut().intValue()
                                        : 0));
						executeQuery(pstmnt, maxRows, objdata, response, maxFieldSize);
					}
				} else {
					throw new ConnectorException("Please enter SQL Statement");
				}
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IllegalArgumentException e) {
				CustomResponseUtil.writeInvalidInputResponse(e, objdata, response);
			} catch (Exception e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			}
		}
		LOG.info("Statements proccessed Successfully!!");

	}

	/**
	 * Builds the final query based on the EXEC(
	 *
	 * @param query    the query
	 * @param userData the user data
	 * @return the query
	 */
	private static String getQuery(String query, JsonNode userData) {
		if (query != null && !query.toUpperCase().contains(OPEN_EXEC)) {
			return userData.get(DatabaseConnectorConstants.SQL_QUERY) == null ? query
					: userData.get(DatabaseConnectorConstants.SQL_QUERY).toString().replace(DOUBLE_QUOTE, "");
		} else {
			return query;
		}

	}

	/**
	 * Returns true if entered Query has IN CLAUSE.
	 *
	 * @param query the query
	 * @return the in check
	 */
	private static boolean inCheck(String query) {
		return (Pattern.compile(DatabaseConnectorConstants.CAPS_IN).matcher(query).find() ||
				Pattern.compile(DatabaseConnectorConstants.SMALL_IN).matcher(query).find());
	}

	/**
	 * Gets the data types for each columns associated with the tables/table.
	 *
	 * @param sqlConnection          the sqlConnection
	 * @param objectTypeId the object type id
	 * @param finalQuery   the final query
	 * @param schemaName the schema Name
	 * @return the data types
	 * @throws SQLException the SQL exception
	 */
	private Map<String, String> getDataTypes(Connection sqlConnection, String objectTypeId, String finalQuery,
			String schemaName) throws SQLException {
		Map<String, String> dataTypes = new HashMap<>();
		if (objectTypeId.contains(",")) {
			String[] tableNames = SchemaBuilderUtil.getTableNames(objectTypeId);
			for (String tableName : tableNames) {
				dataTypes.putAll(MetadataExtractor.getDataTypesWithTable(sqlConnection, tableName.trim()));
			}
		} else {
			if (!finalQuery.toUpperCase().contains(OPEN_EXEC)) {
				String tableNameInQuery = QueryBuilderUtil.validateTheTableName(finalQuery.toUpperCase());
				if (!tableNameInQuery.equalsIgnoreCase(getContext().getObjectTypeId())) {
					throw new ConnectorException(
							"The table name used in the query does not match with Object Type selected!");
				}
			}
			dataTypes.putAll(new MetadataExtractor(sqlConnection, objectTypeId, schemaName).getDataType());

		}
		return dataTypes;
	}

	/**
	 * Execute query for IN clause.
	 *
	 * @param finalINQuery the final IN query
	 * @param sqlConnection          the sqlConnection
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @param maxFieldSize the max field size
	 * @param response     the response
	 * @param query
	 * @throws SQLException
	 * @throws IOException
	 * @throws ParseException
	 * @throws Exception
	 */
	private void executeQueryForINClause(String finalINQuery, Connection sqlConnection, ObjectData objdata,
			Map<String, String> dataTypes, Long maxFieldSize, OperationResponse response, String query)
			throws SQLException, IOException, ParseException {
		Long maxRows = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.MAX_ROWS, (long) 0);
		DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
		if (finalINQuery != null) {
			if (dataTypes.containsKey(NVARCHAR)
					&& databaseMetaData.getDatabaseProductName().equals(DatabaseConnectorConstants.MSSQL)) {
				finalINQuery = finalINQuery + " FOR JSON AUTO";
			}
			try (NamedParameterStatement pstmnt = new NamedParameterStatement(sqlConnection, finalINQuery, objdata)) {
				pstmnt.setReadTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
						getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
				inClausePreparedStatement(objdata, dataTypes, pstmnt, sqlConnection, query);
				executeQuery(pstmnt, maxRows, objdata, response, maxFieldSize);
			}

		}

	}

	/**
	 * In clause prepared statement.
	 *
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param bstmnt    the bstmnt
	 * @param sqlConnection       the sqlConnection
	 * @param query
	 * @throws IOException    Signals that an I/O exception has occurred.
	 * @throws SQLException   the SQL exception
	 * @throws ParseException the parse exception
	 */
	private static void inClausePreparedStatement(ObjectData objdata, Map<String, String> dataTypes,
			NamedParameterStatement bstmnt, Connection sqlConnection, String query)
			throws IOException, SQLException, ParseException {
		ObjectReader reader = DBv2JsonUtil.getBigDecimalObjectMapper().reader();
		try (InputStream is = objdata.getData()) {
			JsonNode json = reader.readTree(is);
			if (json != null) {
				DatabaseMetaData md = sqlConnection.getMetaData();
				String databaseName = md.getDatabaseProductName();
				int i = 0;
				Iterator<String> inputKeys = json.fieldNames();
				while (inputKeys.hasNext()) {
					String key = inputKeys.next();
					if (!SQL_QUERY.equalsIgnoreCase(key)) {
						i++;
						validateDataType(dataTypes, key);
						JsonNode value = json.get(key);
						switch (dataTypes.get(key)) {
						case INTEGER:
							setIntegerDataType(value, bstmnt, i, json, key);
							break;
						case DATE:
							setDateDataType(value, bstmnt, i, json, key,databaseName);
							break;
						case JSON:
							extractJsonInClause(bstmnt, json, databaseName, i, key);
							break;
						case NVARCHAR:
							setNvarcharDataType(value, bstmnt, i, json, key);
							break;
						case STRING:
							setStringDataType(value, bstmnt, i, json, key);
							break;
						case TIME:
							setTimeDataType(value, bstmnt, i, json, key,databaseName);
							break;
						case BOOLEAN:
							setBooleanDataType(value, bstmnt, i, json, key);
							break;
						case LONG:
							setLongDataType(value, bstmnt, i, json, key);
							break;
						case FLOAT:
							setFloatDataType(value, bstmnt, i, json, key);
							break;
						case DOUBLE:
							setDoubleDataType(value, bstmnt, i, json, key);
							break;
						case BLOB:
							setBLOBDataType(value, bstmnt, i, json, key);
							break;
						case TIMESTAMP:
							setTimestampDataType(value, bstmnt, i, json, key);
							break;
						default:
							break;
						}
					} else if (query.contains(EXEC)) {
						bstmnt.setExec(1, json.get(key).toString().replace(DOUBLE_QUOTE, ""));
					}
				}
			}
		}

	}

	/**
	 * @param bstmnt
	 * @param json
	 * @param databaseName
	 * @param i
	 * @param key
	 * @throws SQLException
	 * @throws ParseException
	 */
	private static void extractJsonInClause(NamedParameterStatement bstmnt, JsonNode json, String databaseName, int i,
			String key) throws SQLException, ParseException {
		if (databaseName.equals(POSTGRESQL)) {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			ArrayNode node = (ArrayNode) json.get(key);
			jsonObject.setValue(node.toString());
			bstmnt.setObject(i, jsonObject);
		} else if (databaseName.equals(ORACLE)) {
			String finalString = json.get(key).toString().replace("\\\"", DOUBLE_QUOTE);
			JSONParser parser = new JSONParser();
			JSONObject jsonString = (JSONObject) parser.parse(finalString);
			JSONObject jsonObject = new JSONObject(jsonString);
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String jsonKey = keys.next();
				OracleJsonFactory factory = new OracleJsonFactory();
				OracleJsonObject object = factory.createObject();
				object.put(jsonKey, (String) jsonObject.get(jsonKey));
				bstmnt.setObject(i, object);
			}
		} else {
			bstmnt.setString(key, StringEscapeUtils.unescapeJava(json.get(key).toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * This method will validate the Datatypes of the column before setting the
	 * values to Prepared Statement.
	 *
	 * @param dataTypes the data types
	 * @param key       the key
	 */
	private static void validateDataType(Map<String, String> dataTypes, String key) {
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
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private void executeQuery(NamedParameterStatement pstmnt, Long maxRows, ObjectData objdata,
			OperationResponse response, Long maxFieldSize) throws SQLException, IOException {
		Long fetchSize = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.FETCH_SIZE);
		//check if "maxRow" is configured as DOP and override the value
		maxRows = QueryBuilderUtil.getMaxRowIfConfigureAsDOP(objdata, maxRows);
		if (maxRows != null && maxRows > 0) {
			pstmnt.setMaxRows(maxRows.intValue());
		}
		if (maxFieldSize != null && maxFieldSize > 0) {
			pstmnt.setMaxFieldSize(maxFieldSize.intValue());
		}
		if (fetchSize != null && fetchSize > 0) {
			pstmnt.setFetchSize(fetchSize.intValue());
		}
		this.processResultSet(pstmnt, objdata, response);
	}

	/**
	 * This method will add the parameters to the Prepared Statements based on the
	 * incoming requests.
	 *
	 * @param sqlConnection       the sqlConnection
	 * @param jsonNode  the json node
	 * @param dataTypes the data types
	 * @param pstmnt    the pstmnt
	 * @param query
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private static void prepareStatement(Connection sqlConnection, JsonNode jsonNode, Map<String, String> dataTypes,
			NamedParameterStatement pstmnt, String query) throws SQLException, IOException {
		int i = 0;
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		if (jsonNode != null) {
			for (Iterator<String> fieldName = jsonNode.fieldNames(); fieldName.hasNext();) {
				String key = fieldName.next().trim();
				JsonNode fieldValue = jsonNode.get(key);
				if (!key.equals(SQL_QUERY)) {
					validateDataType(dataTypes, key);
					i++;
					if (dataTypes.containsKey(key)) {
						checkDataType(dataTypes, pstmnt, i, databaseName, key, fieldValue);
					}

				} else if (query.toUpperCase().contains(OPEN_EXEC)) {
					pstmnt.setExec(1, fieldValue.toString().replace(DOUBLE_QUOTE, ""));
				}

			}
		}
	}

	/**
	 * @param dataTypes
	 * @param pstmnt
	 * @param i
	 * @param databaseName
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void checkDataType(Map<String, String> dataTypes, NamedParameterStatement pstmnt, int i,
			String databaseName, String key, JsonNode fieldValue) throws SQLException, IOException {

		switch (dataTypes.get(key)) {
		case INTEGER:
			setIntegerData(fieldValue, pstmnt, i, key);
			break;
		case STRING:
			setStringData(fieldValue, pstmnt, i, key);
			break;
		case NVARCHAR:
			setNvarcharData(fieldValue, pstmnt, i, key);
			break;
		case JSON:
			if (fieldValue != null) {
				extractJson(pstmnt, i, databaseName, key, fieldValue);
			} else {
				pstmnt.setNull(i, Types.NULL);
			}
			break;
		case DATE:
			setDateData(fieldValue, pstmnt, i, key, databaseName);
			break;
		case CLOB:
			setCLOBData(fieldValue, pstmnt, key, databaseName);
			break;
		case TIME:
			setTimeData(fieldValue, pstmnt, i, key, databaseName);
			break;
		case BOOLEAN:
			setBooleanData(fieldValue, pstmnt, i, key);
			break;
		case LONG:
			setLongData(fieldValue, pstmnt, i, key);
			break;
		case FLOAT:
			setFloatData(fieldValue, pstmnt, i, key);
			break;
		case DOUBLE:
			setDoubleData(fieldValue, pstmnt, i, key);
			break;
		case BLOB:
			setBLOBData(fieldValue, pstmnt, i, key);
			break;
		case TIMESTAMP:
			setTimestampData(fieldValue, pstmnt, i, key);
			break;
		default:
			break;
		}

	}

	/**
	 * This method will extract the Json from Json field value and sets it to
	 * prepared statement based on the database name
	 * 
	 * @param pstmnt
	 * @param i
	 * @param databaseName
	 * @param key
	 * @param fieldValue
	 * @throws SQLException
	 */
	private static void extractJson(NamedParameterStatement pstmnt, int i, String databaseName, String key,
			JsonNode fieldValue) throws SQLException {
		if (databaseName.equals(POSTGRESQL)) {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(fieldValue.toString());
			pstmnt.setObject(i, jsonObject);
		} else if (databaseName.equals(ORACLE)) {
			OracleJsonFactory factory = new OracleJsonFactory();
			OracleJsonObject object = factory.createObject();
			JSONObject jsonObject = new JSONObject(fieldValue.toString());
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String jsonKeys = keys.next();
				object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
			}
			pstmnt.setObject(i, object);
		} else {
			pstmnt.setString(key, StringEscapeUtils.unescapeJava(fieldValue.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * This method will process the result set and creates the Payload for the
	 * Operation Response.
	 *
	 * @param pstmnt   the pstmnt
	 * @param objdata  the objdata
	 * @param response the response
	 * @throws IOException 
	 */
	private void processResultSet(NamedParameterStatement pstmnt, ObjectData objdata, OperationResponse response)
			throws IOException {
		CustomPayloadUtil load = null;
		try (ResultSet rs = pstmnt.executeQuery();) {
			boolean isBatching = false;
			String cookie = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
			if (cookie != null && !cookie.isEmpty()) {
				ObjectReader reader = DBv2JsonUtil.getObjectReader();
				JsonNode     json   = reader.readTree(cookie);
				JsonNode cookieMap = json.get("documentBatching");
				if (cookieMap != null) {
					isBatching = cookieMap.asBoolean();
				}
			}
			Long batchCount = getContext().getOperationProperties().getLongProperty(BATCH_COUNT);
				while (rs.next()) {
					if ((batchCount == null || batchCount == 0) && !isBatching ) {
						load = new CustomPayloadUtil(rs);
				}
					else if(batchCount != null && isBatching && batchCount>0){
						load = new CustomPayloadUtil(rs, batchCount);
						
					}
					else {
						throw new ConnectorException("Kindly check the profile details!!");
					}
					response.addPartialResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
							SUCCESS_RESPONSE_MESSAGE, load);
			} 
			response.finishPartialResult(objdata);

		} catch (SQLException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} finally {
			IOUtil.closeQuietly(load);
		}
	}
	
	/**
	 * Builds the final query based on the EXEC(
	 *
	 * @param query    the query
	 * @param userData the user data
	 * @return the query
	 */
	private static String setWhereAndOrderBy(String query, ObjectData objdata) {
		
		HashMap<String,String> paramValues = new HashMap<>(); 
		List<String> paramNames = QueryBuilderUtil.getParamNames(query, "standard");
		if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.WHERE_PARAM, null) != null) {
			paramValues.putIfAbsent(DatabaseConnectorConstants.WHERE_PARAM,
					"WHERE " + objdata.getDynamicProperties().get(DatabaseConnectorConstants.WHERE_PARAM));
		}
		if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.ORDER_BY_PARAM, null) != null) {
			paramValues.putIfAbsent(DatabaseConnectorConstants.ORDER_BY_PARAM,
					"ORDER BY " + objdata.getDynamicProperties().get(DatabaseConnectorConstants.ORDER_BY_PARAM));
		}
		for(String param : paramNames) {
			String curParamRegex = String.format(PARAM_REPLACE_REGEX, param, param, param);
			if (paramValues.get(param) != null) {
				query = query.replaceAll(curParamRegex, Matcher.quoteReplacement(paramValues.get(param)));
			}
		}
	
		return query;

	}
	
	/**
	 * Sets the Integer value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerDataType(JsonNode value, NamedParameterStatement bstmnt, int i, JsonNode json,
			String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.INTEGER);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setIntArray(key, (ArrayNode) json.get(key));
			} else {
				BigDecimal bigDecimalValue = new BigDecimal(json.get(key).toString().replace(DOUBLE_QUOTE, ""));
				bstmnt.setBigDecimal(key, bigDecimalValue);
			}
		}
	}
	
	/**
	 * Sets the Date value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateDataType(JsonNode value, NamedParameterStatement bstmnt, int i, JsonNode json,
			String key, String databaseName) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.DATE);
		} else {
			if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
				if (json.get(key) instanceof ArrayNode) {
					bstmnt.setStringArray(key, (ArrayNode) json.get(key));
				} else {
					bstmnt.setString(key, json.get(key).toString().replace(DOUBLE_QUOTE, ""));
				}
			} else {
				if (json.get(key) instanceof ArrayNode) {
					bstmnt.setDateArray(key, (ArrayNode) json.get(key));
				} else {
					try {
						bstmnt.setDate(key,
							Date.valueOf(json.get(key).toString().replace(DOUBLE_QUOTE, "")));
					}catch(IllegalArgumentException e) {
						throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
					}
				}
			}
		}
	}
	
	/**
	 * Sets the NVARCHAR value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setNvarcharDataType(JsonNode value, NamedParameterStatement bstmnt, int i, JsonNode json,
			String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.NVARCHAR);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setNvarcharArray(key, (ArrayNode) json.get(key));
			} else {
				bstmnt.setNString(key, StringEscapeUtils
						.unescapeJava(json.get(key).toString().replace(DOUBLE_QUOTE, "")));
			}
		}
	}
	
	
	/**
	 * Sets the String value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setStringDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.VARCHAR);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setStringArray(key, (ArrayNode) json.get(key));
			} else {
				String varchar = json.get(key).toString().replace(DOUBLE_QUOTE, "");
				bstmnt.setString(key, varchar);
			}
		}
	}

	/**
	 * Sets the Time value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key, String databaseName) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.VARCHAR);
		} else {
			String varchar = json.get(key).toString().replace(DOUBLE_QUOTE, "");
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
	 * Sets the Boolean value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key) throws SQLException {
		boolean flag = Boolean.parseBoolean(json.get(key).toString().replace(DOUBLE_QUOTE, ""));
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
	 * Sets the Long value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.BIGINT);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setLongArray(key, (ArrayNode) json.get(key));
			} else {
				long longValue = Long.parseLong(json.get(key).toString().replace(DOUBLE_QUOTE, ""));
				bstmnt.setLong(key, longValue);
			}
		}
	}
	
	/**
	 * Sets the Float value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.FLOAT);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setFloatArray(key, (ArrayNode) json.get(key));
			} else {
				float floatValue = Float.parseFloat(json.get(key).toString().replace(DOUBLE_QUOTE, ""));
				bstmnt.setFloat(key, floatValue);
			}
		}
	}
	
	/**
	 * Sets the Double value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleDataType(JsonNode value, NamedParameterStatement bstmnt, int i, JsonNode json,
			String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.DECIMAL);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setDoubleArray(key, (ArrayNode) json.get(key));
			} else {
				BigDecimal bigDecimalValue = new BigDecimal(json.get(key).toString().replace(DOUBLE_QUOTE, ""));
				bstmnt.setBigDecimal(key, bigDecimalValue);
			}
		}
	}
	
	/**
	 * Sets the BLOB value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void setBLOBDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key) throws SQLException, IOException {
		if (value == null) {
			bstmnt.setNull(i, Types.BLOB);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setBlobArray(key, (ArrayNode) json.get(key));
			} else {
				String blobData = json.get(key).toString().replace(DOUBLE_QUOTE, "");
				bstmnt.setBlob(key, blobData);
			}
		}
	}
	
	/**
	 * Sets the Timestamp value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void setTimestampDataType(JsonNode value, NamedParameterStatement bstmnt, int i,
			JsonNode json, String key) throws SQLException {
		if (value == null) {
			bstmnt.setNull(i, Types.TIMESTAMP);
		} else {
			if (json.get(key) instanceof ArrayNode) {
				bstmnt.setTimeStampArray(key, (ArrayNode) json.get(key));
			} else {
				Timestamp timestamp = Timestamp.valueOf(json.get(key).toString().replace(DOUBLE_QUOTE, ""));
				bstmnt.setTimestamp(key, timestamp);
			}
		}
	}

	/**
	 * Sets the Integer value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerData(JsonNode fieldValue, NamedParameterStatement pstmnt, int i,
			String key) throws SQLException {
		if (fieldValue != null) {
			BigDecimal value   = new BigDecimal(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			pstmnt.setBigDecimal(key, value);
		} else {
			pstmnt.setNull(i, Types.INTEGER);
		}
	}

	/**
	 * Sets the String value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setStringData(JsonNode fieldValue, NamedParameterStatement pstmnt, int i,
			String key) throws SQLException {
		if (fieldValue != null) {
			String varchar = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			pstmnt.setString(key, varchar);
		} else {
			pstmnt.setNull(i, Types.VARCHAR);
		}
	}

	/**
	 * Sets the NVARCHAR parameter value in the prepared statement based on the provided JSON field value.
	 *
	 * @param fieldValue The JSON field value to set in the prepared statement.
	 * @param pstmnt      The {@code NamedParameterStatement} instance representing the prepared statement.
	 * @param i           The index of the parameter in the prepared statement.
	 * @param key         The name of the parameter.
	 * @throws SQLException If a database access error occurs.
	 */
	private static void setNvarcharData(JsonNode fieldValue, NamedParameterStatement pstmnt,
										int i, String key) throws SQLException {
		if (fieldValue != null) {
			pstmnt.setNString(key, StringEscapeUtils.unescapeJava(fieldValue.asText()));
		} else {
			pstmnt.setNull(i, Types.NVARCHAR);
		}
	}

	/**
	 * Sets the Date value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateData(JsonNode fieldValue, NamedParameterStatement pstmnt, int i,
			String key, String databaseName) throws SQLException {
		if (fieldValue != null) {
			if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
				pstmnt.setString(key, fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			} else {
				try {
					pstmnt.setDate(key, Date.valueOf(fieldValue.toString().replace(DOUBLE_QUOTE, "")));
				}catch(IllegalArgumentException e) {
					throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
				}
			}
		} else {
			pstmnt.setNull(i, Types.DATE);
		}
	}

	/**
	 * Sets the CLOB value
	 * @param fieldValue
	 * @param pstmnt
	 * @param key
	 * @param databaseName
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void setCLOBData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			String key, String databaseName) throws SQLException, IOException {
		if (fieldValue != null && DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
			try(Reader string = new StringReader(fieldValue.toString());){
				pstmnt.setClob(key, string);
			}
		}
	}
	
	/**
	 * Sets the TIME value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeData(JsonNode fieldValue, NamedParameterStatement pstmnt, int i,
			String key, String databaseName) throws SQLException {
		if (fieldValue != null) {
			String time = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
				pstmnt.setString(key, time);
			} else {
				pstmnt.setTime(key, Time.valueOf(time));
			}
		} else {
			pstmnt.setNull(i, Types.TIME);
		}
	}
	
	/**
	 * Sets the Boolean value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			int i, String key) throws SQLException {
		if (fieldValue != null) {
			boolean flag = Boolean.parseBoolean(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			pstmnt.setBoolean(key, flag);
		} else {
			pstmnt.setNull(i, Types.BOOLEAN);
		}
	}
	
	/**
	 * Sets the Long value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			int i, String key) throws SQLException {
		if (fieldValue != null) {
			long value = Long.parseLong(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			pstmnt.setLong(key, value);
		} else {
			pstmnt.setNull(i, Types.BIGINT);
		}
	}
	
	/**
	 * Sets the Float value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			int i, String key) throws SQLException {
		if (fieldValue != null) {
			float value = Float.parseFloat(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			pstmnt.setFloat(key, value);
		} else {
			pstmnt.setNull(i, Types.FLOAT);
		}
	}
	
	/**
	 * Sets the Double value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			int i, String key) throws SQLException {
		if (fieldValue != null) {
			BigDecimal value   = new BigDecimal(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			pstmnt.setBigDecimal(key, value);
		} else {
			pstmnt.setNull(i, Types.DECIMAL);
		}
	}
	
	/**
	 * Sets the BLOB value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void setBLOBData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			int i, String key) throws SQLException, IOException {
		if (fieldValue != null) {
			String value = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			pstmnt.setBlob(key, value);

		} else {
			pstmnt.setNull(i, Types.BLOB);
		}
	}
	
	/**
	 * Sets the Timestamp value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void setTimestampData(JsonNode fieldValue, NamedParameterStatement pstmnt,
			int i, String key) throws SQLException {
		if (fieldValue != null) {
			String timeStamp = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			pstmnt.setTimestamp(key, Timestamp.valueOf(timeStamp));
		} else {
			pstmnt.setNull(i, Types.TIMESTAMP);
		}
	}
	
	private static  String constructFinalQuery(String query, ObjectData objdata, JsonNode userData) {
		boolean wherePropSet = false;
		if(query != null && !query.isEmpty()) {
			wherePropSet = true;
		}
		String finalQuery = getQuery(query, userData);
		if(!wherePropSet) {
			finalQuery = setWhereAndOrderBy(finalQuery, objdata);
		}
		return finalQuery;
	}
	
	/**
	 * Gets the Connection instance.
	 *
	 * @return the connection
	 */
	@Override
	public DatabaseConnectorConnection getConnection() {
		return (DatabaseConnectorConnection) super.getConnection();
	}

}
