// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.BatchResponse;
import com.boomi.connector.databaseconnector.model.BatchResponseWithId;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.model.QueryResponseWithId;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.InsertionIDUtil;
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
 * The Class StandardInsertOperation.
 *
 * @author sweta.b.das
 */
public class StandardInsertOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger LOG = Logger.getLogger(StandardInsertOperation.class.getName());

	/**
	 * Instantiates a new standard insert operation.
	 *
	 * @param connection the connection
	 */
	public StandardInsertOperation(DatabaseConnectorConnection connection) {
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
		PropertyMap operationProperties = getContext().getOperationProperties();

		try (Connection databaseConnection = databaseConnectorConnection.getDatabaseConnection()) {
			executeStatements(request, response, operationProperties, databaseConnectorConnection.getSchemaName(),
					databaseConnection);
		} catch (SQLException e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * Executes the SQL statements for the update request.
	 *
	 * @param request the update request
	 * @param response the operation response
	 * @param operationProperties the operationProperties
	 * @param schemaNameFromConnectorConnection the schema name from the connector connection
	 * @param databaseConnection the database connection
	 */
	private void executeStatements(UpdateRequest request, OperationResponse response, PropertyMap operationProperties,
			String schemaNameFromConnectorConnection, Connection databaseConnection) {

		Long batchCount = operationProperties.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT);
		String commitOption = operationProperties.getProperty(DatabaseConnectorConstants.COMMIT_OPTION);
		String query = operationProperties.getProperty(DatabaseConnectorConstants.QUERY, "");
		String schemaName = (String) operationProperties.get(DatabaseConnectorConstants.SCHEMA_NAME);

		if (DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(commitOption) && batchCount != null && batchCount > 0) {
			commitByRows(request, response, batchCount, query, schemaName, schemaNameFromConnectorConnection,
					databaseConnection);
		} else if (DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(commitOption) || batchCount == null
				|| batchCount <= 0) {
			commitByProfile(request, response, query, schemaName, databaseConnection,
					schemaNameFromConnectorConnection,
					null);
			DatabaseUtil.commit(databaseConnection);
			LOG.log(Level.FINE, "Non Batching statements processed Successfully!");
		}
	}

	/**
	 * Builds the query for each document and commits the transaction after executing all the operations.
	 *
	 * @param request contains the documents that needs to be processed
	 * @param response the {@link OperationResponse} object to store the results
	 * @param query the SQL query to be executed
	 * @param schemaName the schema name set in operation
	 * @param sqlConnection the database connection
	 * @param schemaNameFromConnectorConnection the schema name that is set in connection
	 * @param payloadMetadata the {@link PayloadMetadata} object containing additional metadata
	 */
	protected void commitByProfile(UpdateRequest request, OperationResponse response, String query, String schemaName,
			Connection sqlConnection, String schemaNameFromConnectorConnection, PayloadMetadata payloadMetadata) {
		try {
			if(sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, schemaNameFromConnectorConnection);
			String schema = QueryBuilderUtil.getSchemaFromConnection(
					sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
					getConnection().getSchemaName());
			Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
					schema).getDataType();
			sqlConnection.setAutoCommit(false);
			for (ObjectData objectData : request) {
				processObjectData(response, query, sqlConnection, payloadMetadata, objectData, dataTypes);
			}
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * This method processes the object data and executes the query.
	 *
	 * @param response the operation response
	 * @param query the SQL query
	 * @param sqlConnection the SQL connection
	 * @param payloadMetadata the payload metadata
	 * @param objectData the object data
	 * @param dataTypes the data types
	 */
	private void processObjectData(OperationResponse response, String query, Connection sqlConnection,
			PayloadMetadata payloadMetadata, ObjectData objectData, Map<String, String> dataTypes) {
		try {
			QueryResponseWithId queryResponseWithId = executeNonBatch(sqlConnection, objectData, response, query,
					dataTypes);

			if (null == queryResponseWithId.getId()) {
				QueryResponse queryResponse = new QueryResponse(queryResponseWithId.getQuery(),
						queryResponseWithId.getRowsEffected(), queryResponseWithId.getStatus());
				CustomResponseUtil.handleSuccess(objectData, response, payloadMetadata, queryResponse);
				return;
			}
			CustomResponseUtil.handleSuccess(objectData, response, payloadMetadata, queryResponseWithId);
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objectData, response);
		} catch (IOException | IllegalArgumentException e) {
			CustomResponseUtil.writeErrorResponse(e, objectData, response);
		} catch (ConnectorException e) {
			ResponseUtil.addExceptionFailure(response, objectData, e);
		}
	}

	/**
	 * This method commits a batch of update requests by rows.
	 *
	 * @param request the update request
	 * @param response the operation response
	 * @param batchCount the batch count
	 * @param query the SQL query
	 * @param schemaName the schema name
	 * @param schemaNameFromConnectorConnection the schema name from the connector connection
	 * @param sqlConnection the SQL connection
	 * @throws ConnectorException if the SQL connection is null
	 */
	private void commitByRows(UpdateRequest request, OperationResponse response, Long batchCount, String query,
			String schemaName, String schemaNameFromConnectorConnection, Connection sqlConnection) {
		PreparedStatement pstmnt = null;
		try {
			if(sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, schemaNameFromConnectorConnection);
			String schema = QueryBuilderUtil.getSchemaFromConnection(
					sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
					schemaNameFromConnectorConnection);
			sqlConnection.setAutoCommit(false);
			pstmnt = sqlConnection.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);
			pstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
					getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
			Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
					schema).getDataType();
			// We are extending SizeLimitUpdate Operation it loads only single document into
			// memory. Hence we are preparing the list of Object Data which will be required
			// for Statement batching.
			List<ObjectData> batchData = new ArrayList<>();
			for (ObjectData objdata : request) {
				batchData.add(objdata);
			}
			executeBatch(sqlConnection, batchData, response, batchCount, pstmnt, dataTypes);

		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		} finally {
			if (null != pstmnt) {
				try {
					pstmnt.close();
				} catch (SQLException e) {
					LOG.log(Level.WARNING, e.getMessage(), e);
				}
			}

		}
	}

	/**
	 * This method will form the statements by taking query as input parameter and
	 * executes the statement.
	 *
	 * @param sqlConnection   the sqlConnection
	 * @param objectData     the objectData
	 * @param response    the response
	 * @param query       the query
	 * @param dataTypes   the data types
	 * @throws SQLException the SQL exception
	 */
	protected QueryResponseWithId executeNonBatch(Connection sqlConnection, ObjectData objectData,
			OperationResponse response, String query, Map<String, String> dataTypes) throws SQLException, IOException {
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		List<Integer> ids = new ArrayList<>();
		String finalQuery = query;
		int updatedRowCount=0;
			try (InputStream is = objectData.getData()) {
				JsonNode jsonData = RequestUtil.getJsonData(is);

				if (jsonData != null) {
					if (!query.toUpperCase().contains("EXEC(")) {
						finalQuery = jsonData.get(DatabaseConnectorConstants.SQL_QUERY) == null ? query : jsonData.get(
								DatabaseConnectorConstants.SQL_QUERY).toString().replace(
								DatabaseConnectorConstants.DOUBLE_QUOTE, "");
					}
					try (PreparedStatement stmnt = sqlConnection.prepareStatement(finalQuery,
							PreparedStatement.RETURN_GENERATED_KEYS)) {
						stmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
								getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue()
										: 0));
						prepareStatement(sqlConnection, jsonData, dataTypes, stmnt, query);
						updatedRowCount = stmnt.executeUpdate();
						if (updatedRowCount > 1) {
							if (DatabaseConnectorConstants.MYSQL.equals(databaseName)
									|| DatabaseConnectorConstants.MSSQL.equals(databaseName)) {
								InsertionIDUtil.getIdOfInsertedRecords(stmnt, updatedRowCount);
							} else if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
								ids = InsertionIDUtil.queryLastIdPostgreSQL(sqlConnection, updatedRowCount);
							}
						} else if (!DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
							ids = InsertionIDUtil.getInsertIds(stmnt);
						}
					}
				} else if (!"".equals(query)) {
					if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
						preparedStatementforOracle(sqlConnection, query);
					} else {
						try (PreparedStatement stmnt = sqlConnection.prepareStatement(query,
								PreparedStatement.RETURN_GENERATED_KEYS)) {
							stmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(
									getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut()
											.intValue() : 0));
							updatedRowCount = stmnt.executeUpdate();
							ids = InsertionIDUtil.getInsertIds(stmnt);
						}
					}
				} else {
					throw new ConnectorException("Please enter SQLQuery");
				}
			}
		return new QueryResponseWithId(finalQuery, updatedRowCount, ids,
				DatabaseConnectorConstants.SUCCESSFUL_EXECUTION_MESSAGE);
    }

	/**
	 * Gets the indexes.
	 *
	 * @param sqlConnection the sqlConnection
	 * @param objectTypeId the object type id
	 * @return the indexes
	 * @throws SQLException the SQL exception
	 */
	private static String[] getIndexes(Connection sqlConnection, String objectTypeId) throws SQLException {
		String[] indexes = null;
		if (sqlConnection.getMetaData().getDatabaseProductName().equalsIgnoreCase(DatabaseConnectorConstants.ORACLE)) {
			try (ResultSet rs = sqlConnection.prepareStatement(
							DatabaseConnectorConstants.IDENTITY_QUERY + objectTypeId + "'")
					.executeQuery()) {
				if (rs.isBeforeFirst()) {
					indexes = new String[rs.getMetaData().getColumnCount()];
					int i = 0;
					while (rs.next()) {
						indexes[i] = rs.getString(DatabaseConnectorConstants.COLUMN_NAME);
						i++;
					}
				}

			}
		}
		return indexes;
	}

	/**
	 * Prepared statement for oracle.
	 *
	 * @param sqlConnection the sqlConnection
	 * @param query    the query
	 * @throws SQLException the SQL exception
	 */
	private QueryResponseWithId preparedStatementforOracle(Connection sqlConnection, String query) throws SQLException {
		List<Integer> ids = new ArrayList<>();
		int updatedRowCount = 0;
		String[] indexes = getIndexes(sqlConnection, getContext().getObjectTypeId());
		if (indexes != null) {
			try (PreparedStatement stmnt = sqlConnection.prepareStatement(query, indexes)) {
				updatedRowCount = stmnt.executeUpdate();
				ids = InsertionIDUtil.insertIdsForOracle(stmnt);
			}
			return new QueryResponseWithId(query, updatedRowCount, ids,
					DatabaseConnectorConstants.SUCCESSFUL_EXECUTION_MESSAGE);
		} else {
			try (PreparedStatement stmnt = sqlConnection.prepareStatement(query)) {
				updatedRowCount = stmnt.executeUpdate();
			}
			return new QueryResponseWithId(query, updatedRowCount, null,
					DatabaseConnectorConstants.SUCCESSFUL_EXECUTION_MESSAGE);
		}

	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param sqlConnection the sqlConnection
	 * @param batchData  the tracked data
	 * @param response   the response
	 * @param batchCount the batch count
	 * @param pstmnt     the pstmnt
	 * @param dataTypes  the data types
	 * @throws SQLException the SQL exception
	 */
	private static void executeBatch(Connection sqlConnection, List<ObjectData> batchData, OperationResponse response,
			Long batchCount, PreparedStatement pstmnt, Map<String, String> dataTypes) throws SQLException {
		int currentBatchSize = 0;
		int batchnum = 0;
		int currentDocIndex = 0;
		boolean shouldExecute = true;
		List<Integer> ids = new ArrayList<>();
		for (ObjectData objdata : batchData) {
			Payload payload = null;
			currentBatchSize++;
			currentDocIndex++;
			try (InputStream is = objdata.getData();) {
				String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
				// Here we are storing the Object data in MAP, Since the input request is not
				// having the fixed number of fields and Keys are unknown to extract the Json
				// Values.
				JsonNode jsonNode = RequestUtil.getJsonData(is);
				if (jsonNode != null) {
					if (jsonNode.get(DatabaseConnectorConstants.SQL_QUERY) != null) {
						throw new ConnectorException("Commit by rows doesnt support SQLQuery field in request profile");
					} else {
						prepareStatement(sqlConnection, jsonNode, dataTypes, pstmnt, "");
						pstmnt.addBatch();
						if (currentBatchSize == batchCount) {
							batchnum++;
							if (shouldExecute) {
								int[] res = pstmnt.executeBatch();
								if ((DatabaseConnectorConstants.MYSQL.equals(databaseName))
										|| (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName))) {
									ids = InsertionIDUtil.getInsertIds(pstmnt);
								}
								sqlConnection.commit();
								response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + batchnum);
								response.getLogger().log(Level.INFO,
										DatabaseConnectorConstants.BATCH_RECORDS + res.length);
								if ((DatabaseConnectorConstants.MSSQL.equals(databaseName))) {
									payload = JsonPayloadUtil.toPayload(
											new BatchResponse("Batch executed successfully", batchnum, res.length));
								} else {
									payload = JsonPayloadUtil.toPayload(new BatchResponseWithId(
											"Batch executed successfully", batchnum, ids, res.length));
								}
								ResponseUtil.addSuccess(response, objdata,
										DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
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
								executeRemaining(objdata, pstmnt, response, remainingBatch, sqlConnection, currentBatchSize,
										databaseName);
							} else {
								payload = JsonPayloadUtil.toPayload(
										new BatchResponse("Record added to batch successfully", remainingBatch, currentBatchSize));
								ResponseUtil.addSuccess(response, objdata,
										DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
							}
						}
					}
				} else {
					pstmnt.execute();
					sqlConnection.commit();
					response.addResult(objdata, OperationStatus.SUCCESS,
							DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
							DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, null);

				}

			} catch (BatchUpdateException batchUpdateException) {
				CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
				CustomResponseUtil.batchExecuteError(batchUpdateException, objdata, response, batchnum, currentBatchSize);
				currentBatchSize = 0;
				LOG.log(Level.INFO, batchUpdateException.getMessage(), batchUpdateException);
			} catch (SQLException e) {
				CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
				shouldExecute = checkLastRecord(currentBatchSize, batchCount);
				if (shouldExecute) {
					currentBatchSize = 0;
				}
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IOException | IllegalArgumentException e) {
				shouldExecute = checkLastRecord(currentBatchSize, batchCount);
				if (shouldExecute || (currentDocIndex == batchData.size())) {
					pstmnt.clearBatch();
					batchnum++;
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					currentBatchSize = 0;
				}
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, objdata, e);
			} finally {
				IOUtil.closeQuietly(payload);
			}

		}
		LOG.log(Level.FINE, "Batching statements proccessed Successfully!!");

	}

	/**
	 * This method will check whether the input is the last object data of the batch
	 * or not.
	 *
	 * @param b          the b
	 * @param batchCount the batch count
	 * @return if yes returns true or else return false
	 */

	private static boolean checkLastRecord(int b, Long batchCount) {
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
	 * @param databaseName   the database name
	 */
	private static void executeRemaining(ObjectData objdata, PreparedStatement pstmnt, OperationResponse response,
			int remainingBatch, Connection sqlConnection, int b, String databaseName) {

		Payload payload = null;
		List<Integer> ids = new ArrayList<>();
		try {
			int[] res = pstmnt.executeBatch();
			if ((DatabaseConnectorConstants.MYSQL.equals(databaseName))
					|| (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName))) {
				ids = InsertionIDUtil.getInsertIds(pstmnt);
			}
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + remainingBatch);
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.REMAINING_BATCH_RECORDS + res);
			if ((DatabaseConnectorConstants.MSSQL.equals(databaseName))) {
				payload = JsonPayloadUtil.toPayload(
						new BatchResponse("Remaining records added to batch and executed successfully", remainingBatch,
								res.length));
			} else {
				payload = JsonPayloadUtil.toPayload(
						new BatchResponseWithId("Remaining records added to batch and executed successfully",
								remainingBatch, ids, res.length));
			}
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
	 * @param sqlConnection the sqlConnection
	 * @param jsonNode  the json node
	 * @param dataTypes the data types
	 * @param pstmnt    the pstmnt
	 * @param query     the query
	 * @return true if the input request exists or else false.
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private static void prepareStatement(Connection sqlConnection, JsonNode jsonNode, Map<String, String> dataTypes,
			PreparedStatement pstmnt, String query) throws SQLException, IOException {

		int i = 0;
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		Iterator<String> fieldName = jsonNode.fieldNames();
		while (fieldName.hasNext()) {
			String key = fieldName.next();
			JsonNode fieldValue = jsonNode.get(key);
			if (!DatabaseConnectorConstants.SQL_QUERY.equals(key)) {
				i++;
				Boolean hasNonNullField = fieldValue != null && !fieldValue.isNull();
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.INTEGER.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						BigDecimal num = new BigDecimal(
								fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						pstmnt.setBigDecimal(i, num);
					} else {
						pstmnt.setNull(i, Types.INTEGER);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.STRING.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						String varchar = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
						pstmnt.setString(i, varchar);
					} else {
						pstmnt.setNull(i, Types.VARCHAR);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.NVARCHAR.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						pstmnt.setString(i, StringEscapeUtils.unescapeJava(
								fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
					} else {
						pstmnt.setNull(i, Types.NVARCHAR);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.JSON.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						QueryBuilderUtil.extractUnescapeJson(pstmnt, i, databaseName, fieldValue);
					} else {
						pstmnt.setNull(i, Types.NULL);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.DATE.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
							pstmnt.setString(i, fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						} else {
							try {
								pstmnt.setDate(i, Date.valueOf(
										fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
							}catch(IllegalArgumentException e) {
								throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
							}
						}
					} else {
						pstmnt.setNull(i, Types.DATE);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.TIME.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						String time = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
						pstmnt.setTime(i, Time.valueOf(time));
					} else {
						pstmnt.setNull(i, Types.TIME);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.BOOLEAN.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						Boolean flag = Boolean.valueOf(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						pstmnt.setBoolean(i, flag);
					} else {
						pstmnt.setNull(i, Types.BOOLEAN);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.LONG.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						long value = Long.parseLong(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						pstmnt.setLong(i, value);
					} else {
						pstmnt.setNull(i, Types.BIGINT);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.FLOAT.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						float value = Float.parseFloat(fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						pstmnt.setFloat(i, value);
					} else {
						pstmnt.setNull(i, Types.FLOAT);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.DOUBLE.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						BigDecimal num = new BigDecimal(
								fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						pstmnt.setBigDecimal(i, num);
					} else {
						pstmnt.setNull(i, Types.DECIMAL);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.BLOB.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						String value = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
						try(InputStream stream = new ByteArrayInputStream(value.getBytes())) {
							if (databaseName.equals(DatabaseConnectorConstants.POSTGRESQL)) {
								pstmnt.setBinaryStream(i, stream);
							} else {
								pstmnt.setBlob(i, stream);
							}
						}
					} else {
						pstmnt.setNull(i, Types.BLOB);
					}
				}
				if (dataTypes.containsKey(key) && DatabaseConnectorConstants.TIMESTAMP.equals(dataTypes.get(key))) {
					if (hasNonNullField) {
						String timeStamp = fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
						pstmnt.setTimestamp(i, Timestamp.valueOf(timeStamp));
					} else {
						pstmnt.setNull(i, Types.TIMESTAMP);
					}
				}
			} else if (DatabaseConnectorConstants.SQL_QUERY.equals(key) && query.toUpperCase().contains("EXEC(")) {
				pstmnt.setString(1, fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
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
