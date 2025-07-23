// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.BatchResponse;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.databaseconnector.util.RequestUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.commons.text.StringEscapeUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The Class StandardOperation.
 *
 * @author swastik.vn
 */
public class StandardOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(StandardOperation.class.getName());

	/**
	 * Instantiates a new standard operation.
	 *
	 * @param connection the connection
	 */
	public StandardOperation(DatabaseConnectorConnection connection) {
		super(connection);
	}

	/**
	 * Execute size limited update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		Long batchCount = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.BATCH_COUNT);
		String commitOption = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.COMMIT_OPTION);
		String query = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.QUERY, "");
		String schemaName = (String) getContext().getOperationProperties()
				.get(DatabaseConnectorConstants.SCHEMA_NAME);
		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if(sqlConnection == null) {
				throw new ConnectorException(DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			sqlConnection.setAutoCommit(false);
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, databaseConnectorConnection.getSchemaName());
			String schema = QueryBuilderUtil.getSchemaFromConnection(
					sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
					databaseConnectorConnection.getSchemaName());
			Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
					schema).getDataType();
			if (DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(commitOption) && batchCount != null && batchCount > 0) {
				try(PreparedStatement pstmnt = sqlConnection.prepareStatement(query)){
					pstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
							getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue()
									: 0));
					List<ObjectData> batchData = new ArrayList<>();
					for (ObjectData objdata : request) {
						batchData.add(objdata);
					}
					this.executeBatch(sqlConnection, batchData, response, batchCount, pstmnt, dataTypes);
				}
			} else if (DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(commitOption) || batchCount == null
					|| batchCount <= 0) {
				for (ObjectData objdata : request) {
					commitByProfile(response, objdata, sqlConnection, query, dataTypes);
				}
				DatabaseUtil.commit(sqlConnection);
				logger.log(Level.FINE, "Non Batching statements proccessed Successfully!!");
			}
		}catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}

	}

	/**
	 * This method will form the statements by taking query as input parameter and
	 * executes the statement.
	 *
	 * @param sqlConnection         the sqlConnection
	 * @param response    the response
	 * @param query       the query
	 * @param dataTypes   the data types
	 */
	private void commitByProfile(OperationResponse response, ObjectData objdata, Connection sqlConnection, String query,
			Map<String, String> dataTypes) {
		try {
			int updatedRowCount = executeNonBatch(sqlConnection, objdata, response, query, dataTypes);
			QueryResponse queryResponse = new QueryResponse(query, updatedRowCount,
					"Executed Successfully");
			CustomResponseUtil.handleSuccess(objdata, response, null, queryResponse);
		} catch (IOException | IllegalArgumentException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		} catch (ConnectorException e) {
			ResponseUtil.addExceptionFailure(response, objdata, e);
		}
	}

	/**
	 * This method will form the statements by taking query as input parameter and
	 * executes the statement.
	 *
	 * @param sqlConnection         the sqlConnection
	 * @param response    the response
	 * @param query       the query
	 * @param dataTypes   the data types
	 */
	protected int executeNonBatch(Connection sqlConnection, ObjectData objdata, OperationResponse response, String query,
			Map<String, String> dataTypes) throws SQLException, IOException {
			int updatedRowCount = 0;
			try (InputStream is = objdata.getData();) {
				JsonNode jsonNode = RequestUtil.getJsonData(is);
				if (jsonNode != null) {

					String finalQuery = null;
					if (!query.toUpperCase().contains("EXEC(")) {
                        finalQuery = (jsonNode.get(DatabaseConnectorConstants.SQL_QUERY) == null) ? query
                                : jsonNode.get(DatabaseConnectorConstants.SQL_QUERY).toString().replace(
                                        DatabaseConnectorConstants.DOUBLE_QUOTE, "");
					} else {
						finalQuery = query;
					}
					if (finalQuery != null) {
						try (PreparedStatement stmnt = sqlConnection.prepareStatement(finalQuery)) {
							stmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
									getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut()
											.intValue() : 0));
							this.prepareStatement(sqlConnection, jsonNode, dataTypes, stmnt, query);
							updatedRowCount = stmnt.executeUpdate();
						}
					}
				} else if (query != null) {
					try (PreparedStatement stmnt = sqlConnection.prepareStatement(query)) {
						stmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
								getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue()
										: 0));
						updatedRowCount = stmnt.executeUpdate();
					}
				} else {
					throw new ConnectorException("Please enter SQLQuery");
				}
			}

			return updatedRowCount;
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param sqlConnection  the sqlConnection
	 * @param batchData  the tracked data
	 * @param response   the response
	 * @param batchCount the batch count
	 * @param pstmnt     the pstmnt
	 * @param dataTypes  the data types
	 * @throws SQLException the SQL exception
	 */
	private void executeBatch(Connection sqlConnection, List<ObjectData> batchData, OperationResponse response,
			Long batchCount, PreparedStatement pstmnt, Map<String, String> dataTypes) throws SQLException {
		int currentBatchSize = 0;
		int batchnum = 0;
		int currentDocIndex = 0;
		boolean shouldExecute = true;
		for (ObjectData objdata : batchData) {
			Payload payload = null;
			currentBatchSize++;
			currentDocIndex++;

			try (InputStream is = objdata.getData();) {
				// Here we are storing the Object data in MAP, Since the input request is not
				// having the fixed number of fields and Keys are unknown to extract the Json
				// Values.
				JsonNode jsonNode = RequestUtil.getJsonData(is);
				if (jsonNode == null) {
					pstmnt.execute();
					sqlConnection.commit();
					response.addResult(objdata, OperationStatus.SUCCESS,
							DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
							DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, null);
					logger.log(Level.FINE, "Batching statements processed Successfully!");
					return;
				}
				if (jsonNode.get(DatabaseConnectorConstants.SQL_QUERY) != null) {
					throw new ConnectorException(
							"Commit by rows doesnt support SQLQuery field in request profile");
				}
				this.prepareStatement(sqlConnection, jsonNode, dataTypes, pstmnt, "");
				pstmnt.addBatch();
				if (currentBatchSize == batchCount) {
					batchnum++;
					if (shouldExecute) {
						int res[] = pstmnt.executeBatch();
						sqlConnection.commit();
						response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + batchnum);
						response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_RECORDS + res.length);
						payload = JsonPayloadUtil.toPayload(
								new BatchResponse("Batch executed successfully", batchnum, res.length));
						ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
								payload);
					} else {
						pstmnt.clearBatch();
						shouldExecute = true;
						CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
						CustomResponseUtil.batchExecuteError(objdata, response, batchnum, currentBatchSize);
					}

					currentBatchSize = 0;
				} else if (currentBatchSize < batchCount) {
					int remainingBatch = batchnum + 1;
					if (currentDocIndex == batchData.size()) {
						this.executeRemaining(objdata, pstmnt, response, remainingBatch, sqlConnection, currentBatchSize);
					} else {
						payload = JsonPayloadUtil.toPayload(
								new BatchResponse("Record added to batch successfully", remainingBatch, currentBatchSize));
						ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
								payload);
					}
				}

				logger.log(Level.FINE, "Batching statements processed Successfully!");
			} catch (BatchUpdateException batchUpdateException) {
				CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
				CustomResponseUtil.batchExecuteError(batchUpdateException, objdata, response, batchnum, currentBatchSize);
				currentBatchSize = 0;
			} catch (SQLException e) {
				CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
				shouldExecute = this.checkLastRecord(currentBatchSize, batchCount);
				if (shouldExecute) {
					currentBatchSize = 0;
				}
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IOException | IllegalArgumentException e) {
				shouldExecute = this.checkLastRecord(currentBatchSize, batchCount);
				if (shouldExecute || currentDocIndex == batchData.size()) {
					pstmnt.clearBatch();
					batchnum++;
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					currentBatchSize = 0;
				}
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			}finally {
				IOUtil.closeQuietly(payload);
			}
		}
	}

	/**
	 * This method will check whether the input is the last object data of the batch
	 * or not.
	 *
	 * @param b          the b
	 * @param batchCount the batch count
	 * @return if yes returns true or else return false
	 */

	private boolean checkLastRecord(int b, Long batchCount) {
		return b == batchCount;
	}

	/**
	 * This method will execute the remaining statements of the batching.
	 *
	 * @param objdata        the objdata
	 * @param pstmnt         the pstmnt
	 * @param response       the response
	 * @param remainingBatch the remaining batch
	 * @param sqlConnection  the sqlConnection
	 * @param b              the b
	 */
	private void executeRemaining(ObjectData objdata, PreparedStatement pstmnt, OperationResponse response,
			int remainingBatch, Connection sqlConnection, int b) {

		Payload payload = null;
		try {
			int res[] = pstmnt.executeBatch();
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + remainingBatch);
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.REMAINING_BATCH_RECORDS + res.length);
			payload = JsonPayloadUtil.toPayload(new BatchResponse(
					"Remaining records added to batch and executed successfully", remainingBatch, res.length));
			ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
			sqlConnection.commit();
		} catch (SQLException e) {
			CustomResponseUtil.logFailedBatch(response, remainingBatch, b);
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		} finally {
			IOUtil.closeQuietly(payload);
		}

	}

	/**
	 * This method will take the input requests and set the values to the Prepared
	 * statement provided by the user.
	 *
	 * @param connection       the con
	 * @param jsonData  the json data
	 * @param dataTypes the data types
	 * @param preparedStatement    the pstmnt
	 * @param query
	 * @return true if the input request exists or else false.
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private void prepareStatement(Connection connection, JsonNode jsonData, Map<String, String> dataTypes,
			PreparedStatement preparedStatement, String query) throws SQLException, IOException {

		if (jsonData != null) {
			int index = 0;
			String queryUpperCase = query.toUpperCase();
			String databaseName = connection.getMetaData().getDatabaseProductName();
			for (Iterator<String> fieldName = jsonData.fieldNames(); fieldName.hasNext(); ) {
				String key = fieldName.next();
				JsonNode fieldValue = jsonData.get(key);

				if (!DatabaseConnectorConstants.SQL_QUERY.equals(key)) {
					index++;
					if (dataTypes.containsKey(key)) {
						setPreparedStatementWithInputData(connection, dataTypes, preparedStatement, index,
								databaseName,
								key, fieldValue);
					}
				} else if (queryUpperCase.contains("EXEC(")) {
					preparedStatement.setString(1,
							fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				}
			}
		}
	}

	/**
	 * This method will set the values based on datatype to the Prepared
	 * statement provided by the user.
	 *
	 * @param connection       the connection
	 * @param dataTypes the data types
	 * @param preparedStatement    the preparedStatement
	 * @param index the index
	 * @param databaseName the database name
	 * @param key the json key
	 * @param fieldValue the field value
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private static void setPreparedStatementWithInputData(Connection connection, Map<String, String> dataTypes, PreparedStatement preparedStatement, int index,
			String databaseName, String key, JsonNode fieldValue) throws SQLException, IOException {
		boolean isNonNullField = isNonNullField(fieldValue);
		switch (dataTypes.get(key)) {
			case DatabaseConnectorConstants.INTEGER:
				if (isNonNullField) {
					BigDecimal bigDecimal = new BigDecimal(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					preparedStatement.setBigDecimal(index, bigDecimal);
				} else {
					preparedStatement.setNull(index, Types.INTEGER);
				}
				break;
			case DatabaseConnectorConstants.STRING:
				if (isNonNullField) {
					preparedStatement.setString(index, fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				} else {
					preparedStatement.setNull(index, Types.VARCHAR);
				}
				break;
			case DatabaseConnectorConstants.JSON:
				if (isNonNullField) {
					QueryBuilderUtil.extractUnescapeJson(preparedStatement, index, databaseName, fieldValue);
				}
				break;
			case DatabaseConnectorConstants.NVARCHAR:
				if (isNonNullField) {
					preparedStatement.setString(index, StringEscapeUtils.unescapeJava(
							fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
				} else {
					preparedStatement.setNull(index, Types.NVARCHAR);
				}
				break;
			case DatabaseConnectorConstants.DATE:
				if (isNonNullField) {
					setPreparedStatementWithDate(connection, preparedStatement, index, fieldValue);
				} else {
					preparedStatement.setNull(index, Types.DATE);
				}
				break;
			case DatabaseConnectorConstants.TIME:
				if (isNonNullField) {
					String time = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
					preparedStatement.setTime(index, Time.valueOf(time));
				} else {
					preparedStatement.setNull(index, Types.TIME);
				}
				break;
			case DatabaseConnectorConstants.BOOLEAN:
				if (isNonNullField) {
					Boolean flag = Boolean.valueOf(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					preparedStatement.setBoolean(index, flag);
				} else {
					preparedStatement.setNull(index, Types.BOOLEAN);
				}
				break;
			case DatabaseConnectorConstants.LONG:
				if (isNonNullField) {
					long longValue = Long.parseLong(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					preparedStatement.setLong(index, longValue);
				} else {
					preparedStatement.setNull(index, Types.BIGINT);
				}
				break;
			case DatabaseConnectorConstants.FLOAT:
				if (isNonNullField) {
					float floatValue = Float.parseFloat(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					preparedStatement.setFloat(index, floatValue);
				} else {
					preparedStatement.setNull(index, Types.FLOAT);
				}
				break;
			case DatabaseConnectorConstants.DOUBLE:
				if (isNonNullField) {
					BigDecimal bigDecimal = new BigDecimal(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					preparedStatement.setBigDecimal(index, bigDecimal);
				} else {
					preparedStatement.setNull(index, Types.DECIMAL);
				}
				break;
			case DatabaseConnectorConstants.BLOB:
				if (isNonNullField) {
					setPreparedStatementWithBlob(preparedStatement, index, databaseName, fieldValue);
				} else {
					preparedStatement.setNull(index, Types.BLOB);
				}
				break;
			case DatabaseConnectorConstants.TIMESTAMP:
				if (isNonNullField) {
					String timeStamp = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
					preparedStatement.setTimestamp(index, Timestamp.valueOf(timeStamp));
				} else {
					preparedStatement.setNull(index, Types.TIMESTAMP);
				}
				break;
			default:
				break;
		}
	}

	/**
	 * This method will set the Prepared statement
	 * with Blob value provided by the user.
	 * @param preparedStatement    the preparedStatement
	 * @param index the index
	 * @param databaseName the database name
	 * @param fieldValue the field value
	 * @throws IOException
	 * @throws SQLException the SQL exception
	 */
	private static void setPreparedStatementWithBlob(PreparedStatement preparedStatement, int index, String databaseName, JsonNode fieldValue)
			throws IOException, SQLException {
		String value = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
		try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
			if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
				preparedStatement.setBinaryStream(index, stream);
			} else {
				preparedStatement.setBlob(index, stream);
			}
		}
	}

	/**
	 * This method will set the Prepared statement
	 * with Date value provided by the user.
	 * @param connection       the connection
	 * @param preparedStatement    the preparedStatement
	 * @param index the index
	 * @param fieldValue the field value
	 * @throws SQLException the SQL exception
	 */
	private static void setPreparedStatementWithDate(Connection connection, PreparedStatement preparedStatement, int index, JsonNode fieldValue)
			throws SQLException {
		String fieldValueString = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
		if (DatabaseConnectorConstants.ORACLE.equals(connection.getMetaData().getDatabaseProductName())) {
			preparedStatement.setString(index, fieldValueString);
		} else {
			try {
				preparedStatement.setDate(index,
						Date.valueOf(fieldValueString));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR + fieldValueString, e);
			}
		}
	}

	private static boolean isNonNullField(JsonNode fieldValue) {
		return fieldValue != null && !fieldValue.isNull();
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
