// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.upsert;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.model.QueryResponse;
import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.util.RequestUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author swastik.vn
 *
 */
public class StandardUpsert extends SizeLimitedUpdateOperation {

	/**
	 * Instantiates a new standard upsert.
	 *
	 * @param query        the query
	 * @param connection   the connection
	 * @param objectTypeId the object type id
	 */
	public StandardUpsert(OracleDatabaseConnection conn) {
		super(conn);
	}

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(StandardUpsert.class.getName());

	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		OracleDatabaseConnection conn = getConnection();
		String query = getContext().getOperationProperties().getProperty(OracleDatabaseConstants.QUERY);
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			int readTimeout=conn.getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			con.setAutoCommit(false);
			this.execute(request, response, con, query, readTimeout);
		} catch (SQLException e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}

	}

	/**
	 * Entry point for the Standard Upsert Operation.
	 *
	 * @param request      the request
	 * @param response     the response
	 * @param query
	 * @param commitOption
	 * @param batchCount
	 * @param con
	 * @param readTimeout
	 * @throws SQLException the SQL exception
	 */
	public void execute(UpdateRequest request, OperationResponse response, Connection con, String query, int readTimeout)
			throws SQLException {

		Map<String, String> dataTypes = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();
		for (ObjectData objdata : request) {
			this.executeObjectData(con, objdata, response, dataTypes, query, false, readTimeout);
		}
		try {
			con.commit();
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

	}

	/**
	 * This method will take the Query from the Operation UI and Append the
	 * placeholder of the query with the request values. Post appending it will
	 * execute the Query and commit the connection once all the records are bieng
	 * processed successfully.
	 * 
	 * @param con
	 *
	 * @param objdata   the objdata
	 * @param response  the response
	 * @param dataTypes the data types
	 * @param query     the query
	 * @param update    the update
	 * @param readTimeout
	 * @throws SQLException the SQL exception
	 */
	private void executeObjectData(Connection con, ObjectData objdata, OperationResponse response,
			Map<String, String> dataTypes, String query, boolean update, int readTimeout) throws SQLException {
		Payload payload = null;
		try (InputStream is = objdata.getData()) {
			Map<String, Object> userData = RequestUtil.getUserData(is);
			String finalQuery;
			if (userData != null) {
				if (update) {
					finalQuery = query;
				} else {
					finalQuery = userData.get(SQL_QUERY) == null ? query : (String) userData.get(SQL_QUERY);
				}
				if (finalQuery != null) {
					ExecutionParameters executionParameters = new ExecutionParameters(con, response, objdata, dataTypes, payload);
					payload = executeObjectDataValue(executionParameters, update, readTimeout, userData, finalQuery);
				} else {
					throw new ConnectorException("Please enter SQLQuery");
				}
			} else if (query != null) {
				payload = executeObjectDataValue(con, objdata, response, query, readTimeout, payload);
			} else {
				throw new ConnectorException("Please enter SQLQuery");
			}

		} catch (IOException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} catch (ConnectorException e) {
			ResponseUtil.addExceptionFailure(response, objdata, e);
		} finally {
			IOUtil.closeQuietly(payload);
		}

	}

	/**
	 * 
	 * @param con
	 * @param objdata
	 * @param response
	 * @param query
	 * @param readTimeout
	 * @param payload
	 * @return
	 */
	private Payload executeObjectDataValue(Connection con, ObjectData objdata, OperationResponse response, String query,
			int readTimeout, Payload payload) {
		try (PreparedStatement stmnt = con.prepareStatement(query)) {
			stmnt.setQueryTimeout(readTimeout);
			int updatedRowCount = stmnt.executeUpdate();
			payload = JsonPayloadUtil
					.toPayload(new QueryResponse(query, updatedRowCount, "Executed Successfully"));
			response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
					SUCCESS_RESPONSE_MESSAGE, payload);
		} catch (IllegalArgumentException e) {
			response.addErrorResult(objdata, OperationStatus.APPLICATION_ERROR, null,
					"IllegalArgumentException", e);
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		}
		return payload;
	}

	/**
	 * 
	 * @param con
	 * @param objdata
	 * @param response
	 * @param dataTypes
	 * @param update
	 * @param readTimeout
	 * @param payload
	 * @param userData
	 * @param finalQuery
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	private Payload executeObjectDataValue(ExecutionParameters executionParameters,  boolean update, int readTimeout,
			Map<String, Object> userData, String finalQuery) throws IOException, SQLException {
		Connection con = executionParameters.getCon();
		ObjectData objdata = executionParameters.getObjdata();
		OperationResponse response = executionParameters.getResponse();
		Map<String, String> dataTypes = executionParameters.getDataTypes();
		Payload payload = executionParameters.getPayload();

		try (PreparedStatement stmnt = con.prepareStatement(finalQuery)) {
			stmnt.setQueryTimeout(readTimeout);
			if (!update) {
				this.prepareStatement(userData, dataTypes, stmnt);
			}
			int updatedRowCount = stmnt.executeUpdate();
			payload = JsonPayloadUtil
					.toPayload(new QueryResponse(finalQuery, updatedRowCount, "Executed Successfully"));
			response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
					SUCCESS_RESPONSE_MESSAGE, payload);
		} catch (IllegalArgumentException  e) {
			response.addErrorResult(objdata, OperationStatus.APPLICATION_ERROR, null,
					"IllegalArgumentException", e);
		} catch (SQLException e) {
			if (e.getErrorCode() == 1) {
				String updateQuery = this.buildStatements(objdata, dataTypes, con).toString();
				this.executeObjectData(con, objdata, response, dataTypes, updateQuery, true, readTimeout);
			} else {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			}

		}
		return payload;
	}

	/**
	 * Builds the statements.
	 *
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param con
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private StringBuilder buildStatements(ObjectData objdata, Map<String, String> dataTypes, Connection con)
			throws SQLException, IOException {
		StringBuilder updateQuery = new StringBuilder();
		// This List will be holding the names of the columns which will satisfy the
		// Primary Key and Unique Key constraints if any.
		List<String> conflict = this.checkForViolation(objdata, getPrimaryKeys(con), dataTypes, con);
		if (!conflict.isEmpty()) {
			this.buildUpdateSyntax(updateQuery, objdata, dataTypes, conflict, con);
		}
		return updateQuery;
	}

	/**
	 * Builds the update syntax.
	 *
	 * @param query     the query
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param conflict  the conflict
	 * @param con
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void buildUpdateSyntax(StringBuilder query, ObjectData objdata, Map<String, String> dataTypes,
			List<String> conflict, Connection con) throws IOException, SQLException {
		JsonNode json = null;
		query.append("UPDATE " + getContext().getObjectTypeId() + " SET ");
		try (InputStream is = objdata.getData()) {
			json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(null, null, getContext().getObjectTypeId(), null)) {
					while (resultSet.next()) {
						String key = resultSet.getString(COLUMN_NAME);
						if (!conflict.contains(key)) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								query.append(key + "=");
								String value = fieldName.toString().replace("\"", "");
								this.checkDataType(dataTypes, key, value, query, true);

							}
						}
					}
				}
			} else {
				throw new ConnectorException(INPUT_ERROR);
			}
			query.deleteCharAt(query.length() - 1);
		}
		query.append(" WHERE ");
		for (int i = 0; i <= conflict.size() - 1; i++) {
			updateSyntax(query, objdata, dataTypes, conflict, i);
		}

	}

	/**
	 * 
	 * @param query
	 * @param objdata
	 * @param dataTypes
	 * @param conflict
	 * @param i
	 * @throws IOException
	 */
	private void updateSyntax(StringBuilder query, ObjectData objdata, Map<String, String> dataTypes,
			List<String> conflict, int i) throws IOException {
		if (i > 0) {
			query.append(" AND ");
		}
		String key = conflict.get(i);
		query.append(key + " = ");
		JsonNode json1 = null;
		try (InputStream is = objdata.getData()) {
			json1 = mapper.readTree(is);
			if (json1 != null) {
				JsonNode fieldName = json1.get(key);
				if (fieldName != null) {
					String value = fieldName.toString().replace("\"", "");
					this.checkDataType(dataTypes, key, value, query, false);
				}
			}

		}
	}

	/**
	 * This method will check for Primary Key and Unique Key constraints if any.
	 *
	 * @param objdata     the objdata
	 * @param primaryKeys the primary keys
	 * @param dataTypes   the data types
	 * @param con
	 * @return the list
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	public List<String> checkForViolation(ObjectData objdata, List<String> primaryKeys, Map<String, String> dataTypes,
			Connection con) throws SQLException, IOException {
		List<String> conflict = new ArrayList<>();
		JsonNode json = null;

		try (InputStream is = objdata.getData(); ) {
			json = mapper.readTree(is);
			if (json != null) {
				for (int i = 0; i <= primaryKeys.size() - 1; i++) {
					StringBuilder constraintQuery = new StringBuilder("Select " + primaryKeys.get(i) + " from "
							+ getContext().getObjectTypeId() + " WHERE " + primaryKeys.get(i) + " = ");
					String key = primaryKeys.get(i);
					JsonNode fieldName = json.get(key);
					if (fieldName != null) {
						String value = fieldName.toString().replace("\"", "");
						this.checkDataType(dataTypes, key, value, constraintQuery, false);
						try (ResultSet set = con.prepareCall(constraintQuery.toString()).executeQuery()) {
							if (set.isBeforeFirst()) {
								conflict.add(primaryKeys.get(i));
							}
						}

					}

				}
			} else {
				throw new ConnectorException(INPUT_ERROR);
			}

		}catch (IOException | IllegalArgumentException | ClassCastException e) {
			logger.log(Level.SEVERE, e.toString());
		}
		return conflict;

	}

	/**
	 * This method will Check for the datatype and append the query with values
	 * based on the datatype.
	 *
	 * @param dataTypes the data types
	 * @param key       the key
	 * @param value     the value
	 * @param query     the query
	 * @param comma     the comma
	 */
	private void checkDataType(Map<String, String> dataTypes, String key, String value, StringBuilder query,
			boolean comma) {
		if(dataTypes.containsKey(key)) {
		switch (dataTypes.get(key)) {
		case INTEGER:
			query.append(new BigDecimal(value));
			break;
		case STRING:
		case DATE:
		case TIME:
		case NVARCHAR:	
			query.append("'");
			query.append(value);
			query.append("'");
			break;
		case BOOLEAN:
			Boolean flag = Boolean.valueOf(value);
			query.append(flag);
			break;
		case FLOAT:
			Float fnum = Float.valueOf(value);
			query.append(fnum);
			break;
		case DOUBLE:
			Double dnum = Double.valueOf(value);
			query.append(dnum);
			break;
		case BLOB:
			Integer bnum = Integer.valueOf(value);
			query.append(bnum);
			break;
		case TIMESTAMP:
			query.append("'");
			String svalue = this.timeStampDataType(value);
			String tnum = String.valueOf(svalue);
			query.append(tnum);
			query.append("'");
			break;
		case LONG:
			Long lnum = Long.valueOf(value);
			query.append(lnum);
			break;
		default:
			break;
		}
		}
		if (comma) {
			query.append(",");
		}
		

	}

	
	
	/**
	 * This method will check for the valid timestamp format.
	 * @param Value       the Value
	 * @return String
	 */
	public  String timeStampDataType(String value) {

		   SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME);
		   SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
		   String reformattedStr = null;
		   try {
			   reformattedStr  = myFormat.format(fromUser.parse(value.replace(BACKSLASH, "")));
		   } catch (ParseException e) {
			   SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME_FORMAT1);
			   SimpleDateFormat myFormat1 = new SimpleDateFormat(DATETIME_FORMAT);
			   try {
				   reformattedStr  = myFormat1.format(fromUser1.parse(value.replace(BACKSLASH, "")));
			   } catch (ParseException e1) {
			   reformattedStr = value.replace(BACKSLASH, "");
			   }
		   }
		   return reformattedStr;

	}
	/**
	 * Gets the primary keys of the table which needs to be operated.
	 * 
	 * @param con
	 *
	 * @return the primary keys
	 * @throws SQLException the SQL exception
	 */
	private List<String> getPrimaryKeys(Connection con) throws SQLException {
		List<String> pk = new ArrayList<>();
		try (ResultSet resultSet = con.getMetaData().getIndexInfo(null, null, getContext().getObjectTypeId(), true,
				false)) {
			while (resultSet.next()) {
				if (null != resultSet.getString(NON_UNIQUE)
						&& (resultSet.getString(NON_UNIQUE).equals("0") || resultSet.getString(NON_UNIQUE).equals("f"))
						&& resultSet.getString(COLUMN_NAME) != null && !pk.contains(resultSet.getString(COLUMN_NAME)))
					pk.add(resultSet.getString(COLUMN_NAME));
			}

		}
		return pk;

	}

	/**
	 * This method will check the datatypes of the each column in the database and
	 * append the values accordingly.
	 *
	 * @param con       the con
	 * @param userData  the user data
	 * @param dataTypes the data types
	 * @param pstmnt    the pstmnt
	 * @param objdata   the objdata
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private void prepareStatement(Map<String, Object> userData, Map<String, String> dataTypes,
			PreparedStatement pstmnt) throws SQLException, IOException {
		int i = 0;
		for (Map.Entry<String, Object> entry : userData.entrySet()) {
			String key = entry.getKey();
			if (!key.equals(SQL_QUERY)) {
				i++;
				if (dataTypes.containsKey(key)) {
					QueryBuilderUtil.checkDataType(pstmnt, dataTypes, key, entry, i);
				}

			}

		}

		logger.log(Level.INFO, "Values appended for prepared statement");

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
