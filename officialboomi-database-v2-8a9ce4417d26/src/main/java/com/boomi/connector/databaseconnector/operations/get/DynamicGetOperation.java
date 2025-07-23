// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.get;

import oracle.jdbc.OracleType;
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
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONObject;
import org.postgresql.util.PGobject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.BATCH_COUNT;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.BLOB;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.BOOLEAN;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.COLUMN_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DATE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DOUBLE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DOUBLE_QUOTE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.FLOAT;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.INTEGER;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.JSON;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.LONG;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.MYSQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.NVARCHAR;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.ORACLE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.POSTGRESQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SCHEMA_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.STRING;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TIME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TIMESTAMP;

/**
 * The Class DynamicGetOperation.
 *
 * @author swastik.vn
 */
public class DynamicGetOperation extends SizeLimitedUpdateOperation {

	private static final String PARAM_REPLACE_REGEX = "'[ ]{0,}\\$%s[ ]{0,}'|\\$%s|\"[ ]{0,}\\$%s[ ]{0,}\"";

	/**
	 * Instantiates a new dynamic get operation.
	 *
	 * @param databaseConnectorConnection the databaseConnectorConnection
	 */
	public DynamicGetOperation(DatabaseConnectorConnection databaseConnectorConnection) {
		super(databaseConnectorConnection);
	}

	/**
	 * Execute size limited update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		Long maxRows = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.MAX_ROWS);
		String linkElement = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.LINK_ELEMENT,
				"");
		Long maxFieldSize = getContext().getOperationProperties().getLongProperty(
                DatabaseConnectorConstants.MAX_FIELD_SIZE);
		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if (sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			String schemaName = (String) getContext().getOperationProperties()
					.get(SCHEMA_NAME);
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, databaseConnectorConnection.getSchemaName());
			executeStatements(sqlConnection, request, response, maxRows, linkElement, maxFieldSize);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * Method which will fetch the maxRows linkElement from Operation UI and
	 * dynamically creates the Select Statements.
	 *
	 * @param sqlConnection          the sqlConnection
	 * @param trackedData  the tracked data
	 * @param response     the response
	 * @param maxRows      the max rows
	 * @param linkElement  the link element
	 * @param maxFieldSize the max field size
	 * @throws SQLException the SQL exception
	 */
    private void executeStatements(Connection sqlConnection, UpdateRequest trackedData, OperationResponse response,
            Long maxRows, String linkElement, Long maxFieldSize) throws SQLException {
		boolean inClause = getContext().getOperationProperties()
				.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE, false);
		Long fetchSize = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.FETCH_SIZE);
		String schemaName = getContext().getOperationProperties()
				.getProperty(SCHEMA_NAME);
        String schema = QueryBuilderUtil.getSchemaFromConnection(sqlConnection.getMetaData().getDatabaseProductName(),
                sqlConnection, schemaName, getConnection().getSchemaName());
        Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
                schema).getDataType();
		for (ObjectData objdata : trackedData) {
			PreparedStatement st = null;
			try {
				StringBuilder query = this.buildQuery(objdata, dataTypes, sqlConnection, schema);
				
				if ((linkElement != null) && !linkElement.equalsIgnoreCase("")) {
					addLinkElement(query, linkElement);
				}
				if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.ORDER_BY_PARAM, null) != null) {
					addOrderByStatement(query, objdata);
				}
				st = sqlConnection.prepareStatement(query.toString());
				st.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(getConnection().getReadTimeOut() != null ? 1 : 0));
				prepareAllStatements(st, inClause, query, sqlConnection, objdata, dataTypes, schema);
				//check if "maxRow" is configured as DOP and override the value
				maxRows = QueryBuilderUtil.getMaxRowIfConfigureAsDOP(objdata, maxRows);
				setStatementValues(st, maxRows, maxFieldSize, fetchSize);
				processResultSet(st, objdata, response);
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (Exception e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} finally {
				if (st != null) {
					st.close();
				}
			}
		}
	}

	/**
	 * Returns true if entered Query has IN CLAUSE.
	 *
	 * @param query the query
	 * @return the in check
	 */
	private static boolean inCheck(StringBuilder query) {
		return Pattern.compile(DatabaseConnectorConstants.CAPS_IN).matcher(query).find() || Pattern.compile(
				DatabaseConnectorConstants.SMALL_IN).matcher(query).find();
	}

	/**
	 * In clause prepared statement.
	 *
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param bstmnt    the bstmnt
	 * @param sqlConnection       the sqlConnection
	 * @param schemaName the schema name
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void inClausePreparedStatement(ObjectData objdata, Map<String, String> dataTypes, PreparedStatement bstmnt,
			Connection sqlConnection, String schemaName) throws IOException, SQLException {
		ObjectReader reader = DBv2JsonUtil.getBigDecimalObjectMapper().reader();
		boolean isNormalFlow = true;		
		HashMap<String,String> paramValues = new HashMap<>(); 
		List<String> paramNames = new ArrayList<>();
		if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.WHERE_PARAM, null) != null) {
			paramValues = getParamValues(objdata);
			String whereQuery = paramValues.get(DatabaseConnectorConstants.WHERE_PARAM);
			paramNames = QueryBuilderUtil.getParamNames(whereQuery,DatabaseConnectorConstants.DYNAMIC);
			isNormalFlow = false;
		}
		try (InputStream is = objdata.getData()) {
			if(is.available() != 0) {
				JsonNode json = reader.readTree(is);
				if (json != null) {
					DatabaseMetaData md = sqlConnection.getMetaData();
					String databaseName = md.getDatabaseProductName();
					String objectTypeId = getContext().getObjectTypeId();
					try (ResultSet resultSet = md.getColumns(sqlConnection.getCatalog(), schemaName,
							QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId, databaseName), null);) {
						int i = 0;
						int j = 0;
						int keyCount = 0;
						while (resultSet.next()) {
							String key = resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME);
							if(!isNormalFlow && (paramNames != null) && (paramNames.size() != json.size())) {
								throw new ConnectorException(
										"Please check the Input Request. Mismatch in Where statement parameter and the"
												+ " Input parameter.!!!");
							}
							if(!isNormalFlow && (paramNames != null) && !paramNames.contains(key)) {
								continue;
							}
							if (json.get(key) != null) {
								keyCount++;
								ArrayNode arrayNode = (json.get(key) instanceof ArrayNode) ? (ArrayNode) json.get(key)
                                        : new ArrayNode(JsonNodeFactory.instance).add(json.get(key));
								Iterator<JsonNode> slaidsIterator = arrayNode.elements();
								while (slaidsIterator.hasNext() || (j < arrayNode.size())) {
									i++;
									JsonNode fieldValue = slaidsIterator.next();
									checkInClauseDataType(dataTypes, bstmnt, databaseName, i, key, arrayNode, fieldValue);
									j++;
								}
							}
						}
						if (keyCount != json.size()) {
							throw new ConnectorException("Kindly check the input provided!!!");
						}
					}
				}
			}
		}

	}

	/**
	 * 
	 * This method will check the data type for each key in the Json Request and
	 * sets the value to the prepared statement accordingly
	 * 
	 * @param dataTypes
	 * @param bstmnt
	 * @param databaseName
	 * @param i
	 * @param key
	 * @param arrayNode
	 * @param fieldValue
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void checkInClauseDataType(Map<String, String> dataTypes, PreparedStatement bstmnt, String databaseName,
			int i, String key, ArrayNode arrayNode, JsonNode fieldValue) throws SQLException, IOException {
		switch (dataTypes.get(key)) {
		case INTEGER:
			setIntegerDataType(fieldValue, bstmnt, i);
			break;
		case DATE:
			setDateDataType(fieldValue, bstmnt, i, databaseName);
			break;
		case JSON:
			setJSONDataType(fieldValue, bstmnt, i, databaseName, arrayNode);
			break;
		case NVARCHAR:
			setNvarcharDataType(fieldValue, bstmnt, i);
			break;
		case STRING:
			setStringDataType(fieldValue, bstmnt, i);
			break;
		case TIME:
			setTimeDataType(fieldValue, bstmnt, i, databaseName);
			break;
		case BOOLEAN:
			setBooleanDataType(fieldValue, bstmnt, i);
			break;
		case LONG:
			setLongDataType(fieldValue, bstmnt, i);
			break;
		case FLOAT:
			setFloatDataType(fieldValue, bstmnt, i);
			break;
		case DOUBLE:
			setDoubleDataType(fieldValue, bstmnt, i);
			break;
		case BLOB:
			setBLOBDataType(fieldValue, bstmnt, i, databaseName);
			break;
		case TIMESTAMP:
			setTimestampDataType(fieldValue, bstmnt, i);
			break;
		default:
			break;
		}
	}

	/**
	 * 
	 * Method to extract JSON Value from the input request and set it to prepared
	 * statement based on the database names
	 * 
	 * @param bstmnt
	 * @param databaseName
	 * @param i
	 * @param arrayNode
	 * @param fieldValue
	 * @throws SQLException
	 */
	private static void extractInClauseJson(PreparedStatement bstmnt, String databaseName, int i, ArrayNode arrayNode,
			JsonNode fieldValue) throws SQLException {
		if (databaseName.equals(POSTGRESQL)) {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(arrayNode.toString());
			bstmnt.setObject(i, jsonObject);
		} else if (databaseName.equals(ORACLE)) {
			OracleJsonFactory factory = new OracleJsonFactory();
			OracleJsonObject object = factory.createObject();
			JSONObject jsonObject = new JSONObject(arrayNode.toString());
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String jsonKeys = keys.next();
				object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
			}
			bstmnt.setObject(i, object, OracleType.JSON);
		} else {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldValue.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * This method will add the parameters to the Prepared Statements based on the
	 * incoming requests.
	 *
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param bstmnt    the bstmnt
	 * @param sqlConnection       the sqlConnection
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void prepareStatement(ObjectData objdata, Map<String, String> dataTypes, PreparedStatement bstmnt,
			Connection sqlConnection) throws IOException, SQLException {

		ObjectReader reader = DBv2JsonUtil.getBigDecimalObjectMapper().reader();
		boolean isNormalFlow = true;
		HashMap<String,String> paramValues = new HashMap<>(); 
		List<String> paramNames = new ArrayList<>();
		if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.WHERE_PARAM, null) != null) {
			paramValues = getParamValues(objdata);
			String whereQuery = paramValues.get(DatabaseConnectorConstants.WHERE_PARAM);
			paramNames = QueryBuilderUtil.getParamNames(whereQuery,DatabaseConnectorConstants.DYNAMIC);
			isNormalFlow = false;
		}
		try (InputStream is = objdata.getData()) {
			if (is.available() != 0) {
				JsonNode json = reader.readTree(is);
				if (json != null) {
					DatabaseMetaData md = sqlConnection.getMetaData();
					String databaseName = md.getDatabaseProductName();
					String objectTypeId = getContext().getObjectTypeId();
					try (ResultSet resultSet = md.getColumns(null, null,
							QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId, databaseName), null);) {
						int i = 0;
						while (resultSet.next()) {
							String key = resultSet.getString(COLUMN_NAME);
							if(!isNormalFlow && paramNames != null && paramNames.size() != json.size()) {
								throw new ConnectorException(
										"Please check the Input Request. Mismatch in Where statement parameter and the"
												+ " Input parameter.!!!");
							}
							if(!isNormalFlow && paramNames != null && !paramNames.contains(key)) {
								continue;
							}							
							JsonNode fieldName = json.get(key);
							if (dataTypes.containsKey(key) && fieldName != null) {
								i++;
								setStatementValuesWithDataType(fieldName, bstmnt, i, databaseName, dataTypes, key);
							}
						}
					}
				} else {
					throw new ConnectorException("Please check the input data!!");
				}
			}
		}

	}

	/**
	 * Method to extract JSON Value from the input request and set it to prepared
	 * statement based on the database names
	 * 
	 * @param bstmnt
	 * @param databaseName
	 * @param i
	 * @param fieldName
	 * @throws SQLException
	 */
	private static void extractJson(PreparedStatement bstmnt, String databaseName, int i, JsonNode fieldName)
			throws SQLException {
		if (databaseName.equals(POSTGRESQL)) {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(fieldName.toString());
			bstmnt.setObject(i, jsonObject);
		} else if (databaseName.equals(ORACLE)) {
			OracleJsonFactory factory = new OracleJsonFactory();
			OracleJsonObject object = factory.createObject();
			JSONObject jsonObject = new JSONObject(fieldName.toString());
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String jsonKeys = keys.next();
				object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
			}
			bstmnt.setObject(i, object, OracleType.JSON);
		} else {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldName.toString().replace(DOUBLE_QUOTE, "")));
		}
	}

	/**
	 * Builds the Prepared Statement by taking the request.
	 *
	 * @param objdata   the is
	 * @param dataTypes the data types
	 * @param sqlConnection the sqlConnection
	 * @param schemaName the schema Name
	 * @return the string builder
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
    private StringBuilder buildQuery(ObjectData objdata, Map<String, String> dataTypes, Connection sqlConnection,
            String schemaName) throws IOException, SQLException {
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		String tableName = QueryBuilderUtil.checkTableName(getContext().getObjectTypeId(), databaseName, schemaName);
		StringBuilder query = new StringBuilder(DatabaseConnectorConstants.SELECT_INITIAL + tableName);
		boolean inClause = getContext().getOperationProperties()
				.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE, false);
		if (inClause) {
			this.buildFinalQueryForINClause(sqlConnection, query, objdata, schemaName);
		} else {
			this.buildFinalQuery(sqlConnection, query, objdata, dataTypes);
		}
		return query;
	}

	/**
	 * Builds the final query for IN clause.
	 *
	 * @param sqlConnection     the sqlConnection
	 * @param query   the query
	 * @param objdata the objdata
	 * @param schemaName the schema Name
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void buildFinalQueryForINClause(Connection sqlConnection, StringBuilder query,
			ObjectData objdata, String schemaName) throws IOException, SQLException {
		ObjectReader           reader      = DBv2JsonUtil.getObjectReader();
		HashMap<String,String> paramValues = getParamValues(objdata);
		boolean isWhereDocProp = false;
		try (InputStream is = objdata.getData();) {
			JsonNode json = null;
			if ((is.available() == 0 || null == (json = reader.readTree(is))) &&
					paramValues.get(DatabaseConnectorConstants.WHERE_PARAM) == null){
				throw new ConnectorException("Please check the Input Request!!!");
			}
			// After filtering out the inputs (which are more than 1MB) we are loading the
			// inputstream to memory here.
			DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
			String databaseName = databaseMetaData.getDatabaseProductName();
			String objectTypeId = getContext().getObjectTypeId();
			try (ResultSet resultSet = databaseMetaData.getColumns(sqlConnection.getCatalog(), schemaName,
					QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId, databaseName), null);) {
				buildWhereClause(resultSet, paramValues, json, query, isWhereDocProp);
			}
		}

	}

	/**
	 * Adds the link element field values to the GROUPBY Clause of the query.
	 *
	 * @param query       the query
	 * @param linkElement the link element
	 */
	private static void addLinkElement(StringBuilder query, String linkElement) {
		query.append(DatabaseConnectorConstants.GROUP_BY).append(linkElement);
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
	private void processResultSet(PreparedStatement pstmnt, ObjectData objdata, OperationResponse response)
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
				if ((batchCount == null || batchCount == 0)  && !isBatching ) {
					load = new CustomPayloadUtil(rs);
				} else if (batchCount != null && isBatching && batchCount>0) {
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
	 * This Method will build the Sql query based on the request parameters.
	 *
	 * @param sqlConnection       the sqlConnection
	 * @param query     the query
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
    private void buildFinalQuery(Connection sqlConnection, StringBuilder query, ObjectData objdata,
            Map<String, String> dataTypes) throws IOException, SQLException {
		ObjectReader           reader      = DBv2JsonUtil.getBigDecimalObjectMapper().reader();
		HashMap<String,String> paramValues = getParamValues(objdata);
		boolean withInput = false;
		try (InputStream is = objdata.getData();) {
			if (is.available() != 0) {
				withInput =  true;
				// After filtering out the inputs (which are more than 1MB) we are loading the
				// inputstream to memory here.
				JsonNode json = reader.readTree(is);
				if (json != null) {
					query.append(DatabaseConnectorConstants.WHERE);
					if(paramValues.get(DatabaseConnectorConstants.WHERE_PARAM) != null) {
						buildWhereFromDocumentProperty(paramValues, query);
					}else {
						this.buildWhere(sqlConnection.getMetaData(), query, dataTypes, json);
					}
				}
				else if(!paramValues.isEmpty() && paramValues.get(DatabaseConnectorConstants.WHERE_PARAM) != null) {
					query.append(DatabaseConnectorConstants.WHERE);
					query.append(paramValues.get(DatabaseConnectorConstants.WHERE_PARAM));
				}

				else {
					throw new ConnectorException("Please check the Input Request!!!");
				}

			}
		}
		if(!paramValues.isEmpty() && !withInput && paramValues.get(DatabaseConnectorConstants.WHERE_PARAM) != null) {
			query.append(DatabaseConnectorConstants.WHERE);
			query.append(paramValues.get(DatabaseConnectorConstants.WHERE_PARAM));
		}

	}

	/**
	 * This method will check whether the incoming request parameter is first one
	 * and append the AND character to the query accordingly.
	 *
	 * @param and   the and
	 * @param query the query
	 */
	private static void checkforAnd(boolean and, StringBuilder query) {
		if (and) {
			query.append(" AND ");
		}

	}
	
	private static HashMap<String,String> getParamValues(ObjectData objdata) {
		HashMap<String,String> paramValues = new HashMap<>(); 
		if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.WHERE_PARAM, null) != null) {
			paramValues.putIfAbsent(DatabaseConnectorConstants.WHERE_PARAM,
					objdata.getDynamicProperties().get(DatabaseConnectorConstants.WHERE_PARAM));
		}
		if(objdata.getDynamicProperties().getOrDefault(DatabaseConnectorConstants.ORDER_BY_PARAM, null) != null) {
			paramValues.putIfAbsent(DatabaseConnectorConstants.ORDER_BY_PARAM,
					objdata.getDynamicProperties().get(DatabaseConnectorConstants.ORDER_BY_PARAM));
		}
		return paramValues;
	}
	
	private static void addOrderByStatement(StringBuilder query, ObjectData objdata) {
		HashMap<String,String> paramValues = getParamValues(objdata); 
		if(paramValues.get(DatabaseConnectorConstants.ORDER_BY_PARAM) != null) {
			String orderByQuery = paramValues.get(DatabaseConnectorConstants.ORDER_BY_PARAM);
			List<String> paramNames = QueryBuilderUtil.getParamNames(orderByQuery,DatabaseConnectorConstants.DYNAMIC);
			for(String param : paramNames) {
				String curParamRegex = String.format(PARAM_REPLACE_REGEX, param, param, param);
				if(paramValues.get(param) != null) {
					orderByQuery = orderByQuery.replaceAll(curParamRegex, "?");
				}
			}
			query.append(" order by ");
			query.append(orderByQuery);
		}
	}
	
	private static void buildWhereClause(ResultSet resultSet,
			HashMap<String,String> paramValues, JsonNode json, StringBuilder query, boolean isWhereDocProp) throws SQLException {
		int keyCount = 0;
		query.append(DatabaseConnectorConstants.WHERE);
		if(paramValues.get(DatabaseConnectorConstants.WHERE_PARAM) != null) {
			String whereQuery = paramValues.get(DatabaseConnectorConstants.WHERE_PARAM);
			List<String> paramNames = QueryBuilderUtil.getParamNames(whereQuery,DatabaseConnectorConstants.DYNAMIC);
			while (resultSet.next()) {
				String key = resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME);
				if(paramNames.contains(key)) {
					String curParamRegex = String.format(PARAM_REPLACE_REGEX, key, key, key);
					String question = buildQuestionMarks(json, key);
					whereQuery = whereQuery.replaceAll(curParamRegex,  question);
				}
			}
			query.append(whereQuery);
			isWhereDocProp = true;
		}else {
			while (resultSet.next()) {
				String key = resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME);
				if (json != null && json.get(key) != null) {
					keyCount++;
					query.append(key).append(" IN (");
					String question = buildQuestionMarks(json, key);
					query.append(question);
					query.append(")");
					if (json.size() > 1 && keyCount < json.size()) {
						query.append(" AND ");
					}
				}
			}
		}
		if (json != null && keyCount != json.size() && !isWhereDocProp) {
			throw new ConnectorException("Column name doesnot exist!!!");
		}
	}

	private static String buildQuestionMarks(JsonNode json, String key) {
		StringBuilder question = new StringBuilder();
		int arraySize = json.get(key) instanceof ArrayNode ? json.get(key).size() : 1;
		for (int i = 0; i < arraySize; i++) {
			question.append("?");
			if (i < arraySize - 1) {
				question.append(",");
			}
		}
		return question.toString();
	}
	
	/**
	 * Sets the Integer value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			setBigDecimalData(fieldValue, bstmnt, i);
		} else {
			bstmnt.setNull(i, Types.INTEGER);
		}
	}
	
	/**
	 * Sets the Date value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException {
		if (!fieldValue.isNull()) {
			if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
				bstmnt.setString(i, fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			} else {
				try {
					bstmnt.setDate(i, Date.valueOf(fieldValue.toString().replace(DOUBLE_QUOTE, "")));
				}catch(IllegalArgumentException e) {
					throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
				}
			}
		} else {
			bstmnt.setNull(i, Types.DATE);
		}
	}
	
	/**
	 * Sets the JSON value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setJSONDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i, String databaseName,
			ArrayNode arrayNode) throws SQLException {
		if (!fieldValue.isNull()) {
			extractInClauseJson(bstmnt, databaseName, i, arrayNode, fieldValue);
		} else {
			bstmnt.setNull(i, Types.NULL);
		}
	}
	
	/**
	 * Sets the NVARCHAR value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setNvarcharDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldValue.toString().replace(DOUBLE_QUOTE, "")));
		} else {
			bstmnt.setNull(i, Types.NVARCHAR);
		}
	}
	
	/**
	 * Sets the String value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setStringDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			bstmnt.setString(i, fieldValue.toString().replace(DOUBLE_QUOTE, ""));
		} else {
			bstmnt.setNull(i, Types.VARCHAR);
		}
	}
	
	/**
	 * Sets the Time value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException {
		if (!fieldValue.isNull()) {
			String time = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			if (databaseName.equals(MYSQL)) {
				bstmnt.setString(i, time);
			} else {
				bstmnt.setTime(i, Time.valueOf(time));
			}
		} else {
			bstmnt.setNull(i, Types.TIME);
		}
	}
	
	/**
	 * Sets the Boolean value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			boolean flag = Boolean.parseBoolean(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			bstmnt.setBoolean(i, flag);
		} else {
			bstmnt.setNull(i, Types.BOOLEAN);
		}
	}
	
	/**
	 * Sets the Long value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			long value = Long.parseLong(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			bstmnt.setLong(i, value);
		} else {
			bstmnt.setNull(i, Types.BIGINT);
		}
	}
	
	/**
	 * Sets the Float value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			float value = Float.parseFloat(fieldValue.toString().replace(DOUBLE_QUOTE, ""));
			bstmnt.setFloat(i, value);
		} else {
			bstmnt.setNull(i, Types.FLOAT);
		}
	}
	
	/**
	 * Sets the Double value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			setBigDecimalData(fieldValue, bstmnt, i);
		} else {
			bstmnt.setNull(i, Types.DECIMAL);
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
	private static void setBLOBDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException, IOException {
		if (!fieldValue.isNull()) {
			String value = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			try(InputStream stream = new ByteArrayInputStream(value.getBytes());){
				if (databaseName.equals(POSTGRESQL)) {
					bstmnt.setBinaryStream(i, stream);
				} else {
					bstmnt.setBlob(i, stream);
				}
			}
		} else {
			bstmnt.setNull(i, Types.BLOB);
		}
	}
	
	/**
	 * Sets the Timestamp value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimestampDataType(JsonNode fieldValue, PreparedStatement bstmnt, int i) throws SQLException {
		if (!fieldValue.isNull()) {
			String timeStamp = fieldValue.toString().replace(DOUBLE_QUOTE, "");
			bstmnt.setTimestamp(i, Timestamp.valueOf(timeStamp));
		} else {
			bstmnt.setNull(i, Types.TIMESTAMP);
		}
	}

	private static void setStatementValues(PreparedStatement st, Long maxRows, Long maxFieldSize, Long fetchSize)
			throws SQLException {
		if (maxRows != null && maxRows > 0) {
			st.setMaxRows(maxRows.intValue());
		}
		if (maxFieldSize != null && maxFieldSize > 0) {
			st.setMaxFieldSize(maxFieldSize.intValue());
		}
		if (fetchSize != null && fetchSize > 0) {
			st.setFetchSize(fetchSize.intValue());
		}
	}
	
	/*
	 * Set bind values to the prepared statement based on the IN clause selection.
	 * 
	 * @param st 		 the prepared statement
	 * @param inClause	 the IN Clause selection
	 * @param query 	 the SQL Query
	 * @param con		 the Connection
	 * @param objdata	 the Object data
	 * @param dataTypes	 the data type of the columns map
	 * @param schemaName the schema Name
	 */
	private void prepareAllStatements(PreparedStatement st, boolean inClause, StringBuilder query, Connection con,
			ObjectData objdata, Map<String, String> dataTypes, String schemaName) throws SQLException, IOException {
		if ((!inClause && inCheck(query)) || (inClause && !inCheck(query))) {
			throw new ConnectorException(
					"Kindly select the IN clause check box only if Sql Query contains IN Clause !");
		} else if (inClause && inCheck(query)) {
			this.inClausePreparedStatement(objdata, dataTypes, st, con, schemaName);
		} else {
			this.prepareStatement(objdata, dataTypes, st, con);
		}
	}
	
	/**
	 * Sets the Integer value
	 * @param fieldName
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		setBigDecimalData(fieldName, bstmnt, i);
	}

	/**
	 * Sets the BigDecimal value
	 * @param fieldName
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setBigDecimalData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		BigDecimal num = new BigDecimal(fieldName.toString().replace(DOUBLE_QUOTE, ""));
		bstmnt.setBigDecimal(i, num);
	}

	/**
	 * Sets the Double value
	 * @param fieldName
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		setBigDecimalData(fieldName, bstmnt, i);
	}
	
	/**
	 * Sets the Date value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateData(JsonNode fieldName, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException {
		if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
			bstmnt.setString(i, fieldName.toString().replace(DOUBLE_QUOTE, ""));
		} else {
			try {
				bstmnt.setDate(i, Date.valueOf(fieldName.toString().replace(DOUBLE_QUOTE, "")));
			}catch(IllegalArgumentException e) {
				throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
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
	private static void setStringData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		String varchar = fieldName.toString().replace(DOUBLE_QUOTE, "");
		bstmnt.setString(i, varchar);
	}
	
	/**
	 * Sets the Time value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeData(JsonNode fieldName, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException {
		String time = fieldName.toString().replace(DOUBLE_QUOTE, "");
		if (databaseName.equals(ORACLE) || databaseName.equals(MYSQL)) {
			bstmnt.setString(i, time);
		} else {
			bstmnt.setTime(i, Time.valueOf(time));
		}
	}
	
	/**
	 * Sets the Boolean value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		boolean flag = Boolean.parseBoolean(fieldName.toString().replace(DOUBLE_QUOTE, ""));
		bstmnt.setBoolean(i, flag);
	}
	
	/**
	 * Sets the Long value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		long longValue = Long.parseLong(fieldName.toString().replace(DOUBLE_QUOTE, ""));
		bstmnt.setLong(i, longValue);
	}
	
	/**
	 * Sets the Float value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		float floatValue = Float.parseFloat(fieldName.toString().replace(DOUBLE_QUOTE, ""));
		bstmnt.setFloat(i, floatValue);
	}
	
	/**
	 * Sets the BLOB value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void setBLOBData(JsonNode fieldName, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException, IOException {
		String blobData = fieldName.toString().replace(DOUBLE_QUOTE, "");
		try(InputStream stream = new ByteArrayInputStream(blobData.getBytes());){
			if (databaseName.equals(POSTGRESQL)) {
				bstmnt.setBinaryStream(i, stream);
			} else {
				bstmnt.setBlob(i, stream);
			}
		}
	}
	
	/**
	 * Sets the Timestamp value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimestampData(JsonNode fieldName, PreparedStatement bstmnt, int i) throws SQLException {
		String timeStamp = fieldName.toString().replace(DOUBLE_QUOTE, "");
		bstmnt.setTimestamp(i, Timestamp.valueOf(timeStamp));
	}
	
	/**
	 * Sets the Statement values
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void setStatementValuesWithDataType(JsonNode fieldName, PreparedStatement bstmnt, int i,
			String databaseName, Map<String, String> dataTypes, String key) throws SQLException, IOException {
		switch (dataTypes.get(key)) {
		case INTEGER:
			setIntegerData(fieldName, bstmnt, i);
			break;
		case DATE:
			setDateData(fieldName, bstmnt, i, databaseName);
			break;
		case JSON:
			extractJson(bstmnt, databaseName, i, fieldName);
			break;
		case NVARCHAR:
			bstmnt.setString(i, StringEscapeUtils
					.unescapeJava(fieldName.toString().replace(DOUBLE_QUOTE, "")));
			break;
		case STRING:
			setStringData(fieldName, bstmnt, i);
			break;
		case TIME:
			setTimeData(fieldName, bstmnt, i, databaseName);
			break;
		case BOOLEAN:
			setBooleanData(fieldName, bstmnt, i);
			break;
		case LONG:
			setLongData(fieldName, bstmnt, i);
			break;
		case FLOAT:
			setFloatData(fieldName, bstmnt, i);
			break;
		case DOUBLE:
			setDoubleData(fieldName, bstmnt, i);
			break;
		case BLOB:
			setBLOBData(fieldName, bstmnt, i, databaseName);
			break;
		case TIMESTAMP:
			setTimestampData(fieldName, bstmnt, i);
			break;
		default:
			break;
		}
	}
	
	private static void buildWhereFromDocumentProperty(HashMap<String,String> paramValues, StringBuilder query) {

		String whereQuery = paramValues.get(DatabaseConnectorConstants.WHERE_PARAM);
		List<String> paramNames = QueryBuilderUtil.getParamNames(whereQuery,DatabaseConnectorConstants.DYNAMIC);
		for(String param : paramNames) {
			String curParamRegex = String.format(PARAM_REPLACE_REGEX, param, param, param);
			whereQuery = whereQuery.replaceAll(curParamRegex,  "?");
		}
		query.append(whereQuery);
	
	}
	
	private void buildWhere(DatabaseMetaData databaseMetaData, StringBuilder query, Map<String, String> dataTypes,
							JsonNode json) throws SQLException {

		boolean and = false;
		String objectTypeId = getContext().getObjectTypeId();
		if (databaseMetaData.getDatabaseProductName().equals(ORACLE) && getContext().getObjectTypeId().contains("/")) {
			objectTypeId = getContext().getObjectTypeId().replace("/", "//");
		} else if ((databaseMetaData.getDatabaseProductName().equals(MYSQL)
				|| databaseMetaData.getDatabaseProductName().equals(POSTGRESQL)) && objectTypeId.contains("\\")) {
			objectTypeId = objectTypeId.replace("\\", "\\\\");
		}
		try (ResultSet resultSet = databaseMetaData.getColumns(null, null, objectTypeId, null);) {
			while (resultSet.next()) {
				String key = resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME);
				JsonNode node = json.get(key);
				if (node != null) {
					checkforAnd(and, query);
					query.append(key).append("=");
					if (dataTypes.containsKey(key)) {
						query.append("?");
					}
					and = true;
				}
			}
		}
	
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
