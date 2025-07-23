// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.get;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.text.StringEscapeUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.util.CustomPayloadUtil;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * The Class DynamicGetOperation.
 *
 * @author swastik.vn
 */
public class DynamicGetOperation extends SizeLimitedUpdateOperation {

	/**
	 * Instantiates a new dynamic get operation.
	 *
	 * @param con the con
	 */
	public DynamicGetOperation(OracleDatabaseConnection con) {
		super(con);
	}

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(DynamicGetOperation.class.getName());
	
	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/**
	 * Execute size limited update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		OracleDatabaseConnection conn = getConnection();
		Long maxRows = getContext().getOperationProperties().getLongProperty(MAX_ROWS);
		String linkElement = getContext().getOperationProperties().getProperty(LINK_ELEMENT);
		Long maxFieldSize = getContext().getOperationProperties().getLongProperty("maxFieldSize");
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			this.executeStatements(con, request, response, maxRows, linkElement, maxFieldSize);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * Method which will fetch the maxRows linkElement from Operation UI and
	 * dynamically creates the Select Statements.
	 *
	 * @param con          the con
	 * @param trackedData  the tracked data
	 * @param response     the response
	 * @param maxRows      the max rows
	 * @param linkElement  the link element
	 * @param maxFieldSize the max field size
	 * @throws SQLException the SQL exception
	 */
	private void executeStatements(Connection con, UpdateRequest trackedData, OperationResponse response, Long maxRows,
			String linkElement, Long maxFieldSize) throws SQLException {
		Long fetchSize = getContext().getOperationProperties().getLongProperty(FETCH_SIZE);
		Map<String, String> dataTypes = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();
		for (ObjectData objdata : trackedData) {
			PreparedStatement st = null;
			try {
				ExecutionParameters executionParameters = new ExecutionParameters(con, response, objdata);
				st = executeStatementsValue(executionParameters, maxRows, linkElement, maxFieldSize, fetchSize, dataTypes);
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IOException e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} catch (NumberFormatException e) {
				CustomResponseUtil.writeErrorResponseForEmptyMessages(e, objdata, response);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, objdata, e);
			} finally {
				if (st != null) {
					st.close();
				}
			}
		}
		logger.info("Statements proccessed Successfully!!");
	}

	/**
	 * 
	 * @param con
	 * @param response
	 * @param maxRows
	 * @param linkElement
	 * @param maxFieldSize
	 * @param fetchSize
	 * @param dataTypes
	 * @param objdata
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	private PreparedStatement executeStatementsValue(ExecutionParameters executionParameters, Long maxRows,
			String linkElement, Long maxFieldSize, Long fetchSize, Map<String, String> dataTypes)
			throws IOException, SQLException {
		Connection con = executionParameters.getCon();
		ObjectData objdata = executionParameters.getObjdata();
		OperationResponse response = executionParameters.getResponse();

		StringBuilder query = this.buildQuery(objdata, dataTypes, con);
		if (linkElement != null && !linkElement.equalsIgnoreCase("")) {
			this.addLinkElement(query, linkElement);
		}
		try (PreparedStatement st = con.prepareStatement(query.toString())) {
			st.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
			if (objdata.getDataSize() != 0) {
				checkObjdataSize(con, dataTypes, objdata, st);
			}
			setStatement(maxRows, maxFieldSize, fetchSize, st);
			this.processResultSet(st, objdata, response, con);
			return st;
		}
	}

	/**
	 * 
	 * @param con
	 * @param dataTypes
	 * @param objdata
	 * @param st
	 * @throws IOException
	 * @throws SQLException
	 */
	private void checkObjdataSize(Connection con, Map<String, String> dataTypes, ObjectData objdata,
			PreparedStatement st) throws IOException, SQLException {
		boolean inClause = getContext().getOperationProperties().getBooleanProperty(INCLAUSE, false);
		if (inClause) {
			this.inClausePreparedStatement(objdata, dataTypes, st, con);
		} else {
			this.prepareStatement(objdata, dataTypes, st, con);
		}
	}

	/**
	 * 
	 * @param maxRows
	 * @param maxFieldSize
	 * @param fetchSize
	 * @param st
	 * @throws SQLException
	 */
	private void setStatement(Long maxRows, Long maxFieldSize, Long fetchSize, PreparedStatement st)
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

	/**
	 * This method will add the parameters to the Prepared Statements based on the
	 * incoming requests.
	 *
	 * @param userData  the user data
	 * @param dataTypes the data types
	 * @param pstmnt    the pstmnt
	 * @throws SQLException the SQL exception
	 * @throws NumberFormatException the exception
	 */
	private void prepareStatement(ObjectData objdata, Map<String, String> dataTypes, PreparedStatement bstmnt,
			Connection con) throws IOException, SQLException, NumberFormatException{
		try (InputStream is = objdata.getData()) {
			JsonNode json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null)) {
					int i = 0;
					while (resultSet.next()) {

						String key = resultSet.getString(COLUMN_NAME);
						JsonNode fieldName = json.get(key);
						if (dataTypes.containsKey(key) && fieldName != null) {
							i++;
							switch (dataTypes.get(key)) {
							case INTEGER:
								BigDecimal num = new BigDecimal(fieldName.toString().replace(BACKSLASH, ""));
								bstmnt.setBigDecimal(i, num);
								break;
							case DATE:
								bstmnt.setString(i, fieldName.toString().replace(BACKSLASH, ""));
								break;
							case STRING:
								String varchar = fieldName.toString().replace(BACKSLASH, "");
								bstmnt.setString(i, varchar);
								break;
							case NVARCHAR:
								String nvarchar = StringEscapeUtils
										.unescapeJava(fieldName.toString().replace(BACKSLASH, ""));
								bstmnt.setString(i, nvarchar);
								break;
							case TIME:
								String time = fieldName.toString().replace(BACKSLASH, "");
								bstmnt.setTime(i, Time.valueOf(time));
								break;
							case BOOLEAN:
								boolean flag = Boolean.parseBoolean(fieldName.toString().replace(BACKSLASH, ""));
								bstmnt.setBoolean(i, flag);
								break;
							case LONG:
								long longValue = Long.parseLong(fieldName.toString().replace(BACKSLASH, ""));
								bstmnt.setLong(i, longValue);
								break;
							case FLOAT:
								float floatValue = Float.parseFloat(fieldName.toString().replace(BACKSLASH, ""));
								bstmnt.setFloat(i, floatValue);
								break;
							case DOUBLE:
								double doubleValue = Double.parseDouble(fieldName.toString().replace(BACKSLASH, ""));
								bstmnt.setDouble(i, doubleValue);
								break;
							case BLOB:
								String blobData = fieldName.toString().replace(BACKSLASH, "");
								try(InputStream stream = new ByteArrayInputStream(blobData.getBytes());){
									bstmnt.setBlob(i, stream);
								}
								break;
							case TIMESTAMP:
								QueryBuilderUtil.timeStampDataType(bstmnt,i,fieldName);
								break;
							default:
								break;
							}
						}
					}
				} 
			} else {
				throw new ConnectorException("Please check the input data!!");
			}
		}

	}

	/**
	 * In clause prepared statement.
	 *
	 * @param objdata the objdata
	 * @param dataTypes the data types
	 * @param bstmnt the bstmnt
	 * @param con the con
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void inClausePreparedStatement(ObjectData objdata, Map<String, String> dataTypes, PreparedStatement bstmnt,
			Connection con) throws IOException, SQLException {
		try (InputStream is = objdata.getData()) {
			JsonNode json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(),
						getContext().getObjectTypeId(), null);) {
					int i = 0;
					int j = 0;
					int keyCount = getKeyCount(dataTypes, bstmnt, json, resultSet, i, j);
					if (keyCount != json.size()) {
						throw new ConnectorException("Kindly check the input provided!!!");
					}
				}
			}
		}

	}

	/**
	 * 
	 * @param dataTypes
	 * @param bstmnt
	 * @param json
	 * @param resultSet
	 * @param i
	 * @param j
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private int getKeyCount(Map<String, String> dataTypes, PreparedStatement bstmnt, JsonNode json, ResultSet resultSet, int i, int j) throws SQLException, IOException {
		int keyCount = 0;
		while (resultSet.next()) {
			String key = resultSet.getString(OracleDatabaseConstants.COLUMN_NAME);
			if (json.get(key) != null) {
				keyCount++;
				ArrayNode arrayNode = json.get(key) instanceof ArrayNode ? (ArrayNode) json.get(key)
						: new ArrayNode(JsonNodeFactory.instance).add(json.get(key));
				Iterator<JsonNode> slaidsIterator = arrayNode.elements();
				while (slaidsIterator.hasNext() || j < arrayNode.size()) {
					i++;
					JsonNode fieldValue = slaidsIterator.next();
					switch (dataTypes.get(key)) {
					case INTEGER:
						setIntType(bstmnt, i, fieldValue);
						break;
					case DATE:
						setDateType(bstmnt, i, fieldValue);
						break;
					case VARCHAR:
						setVarcharType(bstmnt, i, fieldValue);
						break;
					case STRING:
						setStringType(bstmnt, i, fieldValue);
						break;
					case TIME:
						setTimeType(bstmnt, i, fieldValue);
						break;
					case BOOLEAN:
						setBooleanType(bstmnt, i, fieldValue);
						break;
					case LONG:
						setLongType(bstmnt, i, fieldValue);
						break;
					case FLOAT:
						setFloatType(bstmnt, i, fieldValue);
						break;
					case DOUBLE:
						setDoubleType(bstmnt, i, fieldValue);
						break;
					case BLOB:
						setBlobType(bstmnt, i, fieldValue);
						break;
					case TIMESTAMP:
						setTimestampType(bstmnt, i, fieldValue);
						break;
					default:
						break;
					}
					j++;
				}
			}
		}
		return keyCount;
	}

	/**
 * 
 * @param bstmnt
 * @param i
 * @param fieldValue
 * @throws SQLException
 */
	private void setTimestampType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			QueryBuilderUtil.timeStampDataType(bstmnt,i,fieldValue);
		} else {
			bstmnt.setNull(i, Types.TIMESTAMP);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setBlobType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException, IOException {
		if (!fieldValue.isNull()) {
			String blobData = fieldValue.toString().replace(BACKSLASH, "");
			try(InputStream stream = new ByteArrayInputStream(blobData.getBytes());){
				bstmnt.setBlob(i, stream);
			}
		} else {
			bstmnt.setNull(i, Types.BLOB);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setDoubleType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			Double db = Double.valueOf(fieldValue.toString().replace(BACKSLASH, ""));
			bstmnt.setDouble(i, db);
		} else {
			bstmnt.setNull(i, Types.DOUBLE);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setFloatType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			Float ft = Float.valueOf(fieldValue.toString().replace(BACKSLASH, ""));
			bstmnt.setFloat(i, ft);
		} else {
			bstmnt.setNull(i, Types.FLOAT);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setLongType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			Long lg = Long.valueOf(fieldValue.toString().replace(BACKSLASH, ""));
			bstmnt.setLong(i, lg);
		} else {
			bstmnt.setNull(i, Types.BIGINT);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setBooleanType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			Boolean flag = Boolean.valueOf(fieldValue.toString().replace(BACKSLASH, ""));
			bstmnt.setBoolean(i, flag);
		} else {
			bstmnt.setNull(i, Types.BOOLEAN);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setTimeType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			String time = fieldValue.toString().replace(BACKSLASH, "");
			bstmnt.setTime(i, Time.valueOf(time));
		} else {
			bstmnt.setNull(i, Types.TIME);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setStringType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			bstmnt.setString(i, fieldValue.toString().replace(BACKSLASH, ""));
		} else {
			bstmnt.setNull(i, Types.VARCHAR);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setVarcharType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			bstmnt.setString(i, StringEscapeUtils
					.unescapeJava(fieldValue.toString().replace(BACKSLASH, "")));
		} else {
			bstmnt.setNull(i, Types.NVARCHAR);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setDateType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			bstmnt.setString(i, fieldValue.toString().replace(BACKSLASH, ""));
		} else {
			bstmnt.setNull(i, Types.DATE);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param fieldValue
	 * @throws SQLException
	 */
	private void setIntType(PreparedStatement bstmnt, int i, JsonNode fieldValue) throws SQLException {
		if (!fieldValue.isNull()) {
			BigDecimal num = new BigDecimal(fieldValue.toString().replace(BACKSLASH, ""));
			bstmnt.setBigDecimal(i, num);
		} else {
			bstmnt.setNull(i, Types.INTEGER);
		}
	}

	/**
	 * Builds the Prepared Statement by taking the request.
	 *
	 * @param objdata   the is
	 * @param dataTypes the data types
	 * @param con       the con
	 * @return the string builder
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private StringBuilder buildQuery(ObjectData objdata, Map<String, String> dataTypes, Connection con)
			throws IOException, SQLException {
		StringBuilder query = new StringBuilder(
				OracleDatabaseConstants.SELECT_INITIAL + getContext().getObjectTypeId());
		boolean inClause = getContext().getOperationProperties().getBooleanProperty(INCLAUSE, false);
		if (inClause) {
			this.buildFinalQueryForINClause(con, query, objdata);
		} else {
			this.buildFinalQuery(con, query, objdata, dataTypes);
		}
		return query;
	}

	/**
	 * Builds the final query for IN clause.
	 *
	 * @param con the con
	 * @param query the query
	 * @param objdata the objdata
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void buildFinalQueryForINClause(Connection con, StringBuilder query, ObjectData objdata)
			throws IOException, SQLException {
		try (InputStream is = objdata.getData();) {
			JsonNode json = null;
			if (is.available() == 0 || null == (json = mapper.readTree(is))) {
				throw new ConnectorException("Please check the Input Request!!!");
			}
			DatabaseMetaData md = con.getMetaData();
			try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null);) {
				int keyCount = 0;
				query.append(OracleDatabaseConstants.WHERE);
				while (resultSet.next()) {
					String key = resultSet.getString(OracleDatabaseConstants.COLUMN_NAME);
					if (json.get(key) != null) {
						keyCount++;
						query.append(key).append(" IN (");
						int arraySize = json.get(key) instanceof ArrayNode ? json.get(key).size() : 1;
						appendToQuery(query, arraySize);
						query.append(")");
						if (json.size() > 1 && keyCount < json.size()) {
							query.append(" AND ");
						}
					}
				}
				if (keyCount != json.size()) {
					throw new ConnectorException("Column name does not exist!!!");
				}
			}
		}

	}
	
	/**
	 * 
	 * @param query
	 * @param arraySize
	 */
	private void appendToQuery(StringBuilder query, int arraySize) {
		for (int i = 0; i < arraySize; i++) {
			query.append("?");
			if (i < arraySize - 1) {
				query.append(",");
			}
		}
	}

	/**
	 * Adds the link element field values to the GROUPBY Clause of the query.
	 *
	 * @param query       the query
	 * @param linkElement the link element
	 */
	private void addLinkElement(StringBuilder query, String linkElement) {
		query.append(GROUP_BY).append(linkElement);
	}

	/**
	 * This method will Process the Result Set and create the payload for Operation
	 * response.
	 *
	 * @param st       the st
	 * @param objdata  the objdata
	 * @param response the response
	 * @param con
	 * @throws IOException 
	 */
	private void processResultSet(PreparedStatement st, ObjectData objdata, OperationResponse response,
			Connection con) throws IOException {
		CustomPayloadUtil load = null;
		try (ResultSet rs = st.executeQuery()) {
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
				if ((batchCount == null || batchCount == 0)  && !isBatching ) {
					load = new CustomPayloadUtil(rs, con);
				} else if (batchCount != null && isBatching && batchCount>0) {
					load = new CustomPayloadUtil(rs, con, batchCount);
				}
				else {
					throw new ConnectorException("Kindly check the profile details!!");
				}
				response.addPartialResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
						SUCCESS_RESPONSE_MESSAGE, load);
			}
			response.finishPartialResult(objdata);

		} catch (SQLException | IOException e) {
			throw new ConnectorException(e.getMessage());
		} finally {
			IOUtil.closeQuietly(load);
		}

	}

	/**
	 * This Method will build the Sql query based on the request parameters.
	 *
	 * @param con       the con
	 * @param query     the query
	 * @param objdata
	 * @param is        the is
	 * @param dataTypes the data types
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void buildFinalQuery(Connection con, StringBuilder query, ObjectData objdata, Map<String, String> dataTypes)
			throws IOException, SQLException {
		try (InputStream is = objdata.getData()) {
			if (is.available() != 0) {
				// After filtering out the inputs (which are more than 1MB) we are loading the
				// inputstream to memory here.
				JsonNode json = mapper.readTree(is);
				if (json != null) {
					query.append(WHERE);
					boolean and = false;
					DatabaseMetaData md = con.getMetaData();
					try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null)) {
						while (resultSet.next()) {
							and = buildFinalQueryValue(query, dataTypes, json, and, resultSet);
						}
					}
				}

				else {
					throw new ConnectorException("Please check the Input Request!!!");
				}

			}
		}

	}

	/**
	 * 
	 * @param query
	 * @param dataTypes
	 * @param json
	 * @param and
	 * @param resultSet
	 * @return
	 * @throws SQLException
	 */
	private boolean buildFinalQueryValue(StringBuilder query, Map<String, String> dataTypes, JsonNode json, boolean and,
			ResultSet resultSet) throws SQLException {
		String key = resultSet.getString(COLUMN_NAME);
		JsonNode node = json.get(key);
		if (node != null) {
			this.checkforAnd(and, query);
			query.append(key).append('=');
			if (dataTypes.containsKey(key)) {
				query.append('?');
			}
			and = true;
		}
		return and;
	}

	/**
	 * This method will check whether the incoming request parameter is first one
	 * and append the AND character to the query accordingly.
	 *
	 * @param and   the and
	 * @param query the query
	 */
	private void checkforAnd(boolean and, StringBuilder query) {
		if (and) {
			query.append(" AND ");
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
