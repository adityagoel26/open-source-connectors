// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.BatchResponse;
import com.boomi.connector.databaseconnector.model.BatchResponseWithId;
import com.boomi.connector.databaseconnector.model.QueryResponseWithId;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.InsertionIDUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
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
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class DynamicInsertOperation.
 *
 * @author swastik.vn
 */
public class DynamicInsertOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger LOG = Logger.getLogger(DynamicInsertOperation.class.getName());

	/**
	 * Instantiates a new dynamic insert operation.
	 *
	 * @param databaseConnectorConnection the databaseConnectorConnection
	 */
	public DynamicInsertOperation(DatabaseConnectorConnection databaseConnectorConnection) {
		super(databaseConnectorConnection);
	}

	/**
	 * Overriden method of SizeLimitUpdateOperation.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		DatabaseConnectorConnection databaseConnectorConnection = getConnection();

		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {

			if (sqlConnection == null) {
				throw new ConnectorException(DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			Long batchCount = getContext().getOperationProperties().getLongProperty(
					DatabaseConnectorConstants.BATCH_COUNT);
			String commitOption = getContext().getOperationProperties().getProperty(
					DatabaseConnectorConstants.COMMIT_OPTION);
			sqlConnection.setAutoCommit(false);
			String schemaName = (String) getContext().getOperationProperties().get(
					DatabaseConnectorConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
					databaseConnectorConnection.getSchemaName());
			this.executeStatements(sqlConnection, request, response, batchCount, commitOption);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * This method will identify whether batching is required based on the input and
	 * process the statements accordingly.
	 *
	 * @param sqlConnection          the sqlConnection
	 * @param trackedData  the tracked data
	 * @param response     the response
	 * @param batchCount   the batch count
	 * @param commitOption the commit option
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void executeStatements(Connection sqlConnection, UpdateRequest trackedData, OperationResponse response,
			Long batchCount, String commitOption) throws SQLException {
		DatabaseMetaData metaData = sqlConnection.getMetaData();
		String objectTypeId = getContext().getObjectTypeId();
		String schemaName = getContext().getOperationProperties()
				.getProperty(DatabaseConnectorConstants.SCHEMA_NAME);
		String schema = QueryBuilderUtil.getSchemaFromConnection(metaData.getDatabaseProductName(),
				sqlConnection, schemaName, getConnection().getSchemaName());
		// This Map will be getting the datatype of the each column associated with the
		// table.
		MetadataExtractor meta = new MetadataExtractor(sqlConnection, objectTypeId, schema);
		Map<String, String> dataTypes = meta.getDataType();

		// Validates that the specified objectTypeId exists in the schema for further steps
		//Other dynamic operations will be fixed as part of CONC-8148
		QueryBuilderUtil.validateDataTypeMappings(dataTypes, objectTypeId);

		Map<String, String> typeNames = meta.getTypeNames();
		StringBuilder query = null;
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0;
		String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(sqlConnection, objectTypeId, schema);
		if (batchCount != null && batchCount > 0 && DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(commitOption)) {
			commitByRows(sqlConnection, trackedData, response, batchCount, schema, dataTypes, typeNames, query, readTimeout,
					autoIncrementColumn);
		} else if (DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(commitOption) || batchCount == null
				|| batchCount == 0) {
			commitByProfile(sqlConnection, trackedData, response, schema, dataTypes, typeNames, readTimeout, autoIncrementColumn,
					null);
			DatabaseUtil.commit(sqlConnection);
		} else if (batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	/**
	 * This method commits a batch of update requests by rows.
	 * @param con the connection
	 * @param trackedData the updateRequest
	 * @param response the response
	 * @param batchCount the batchCount
	 * @param schema the schema
	 * @param dataTypes the dataTypes
	 * @param typeNames the typeNames
	 * @param query the query
	 * @param readTimeout readTimeout
	 * @param autoIncrementColumn the autoIncrementColumn
	 */
	private void commitByRows(Connection con, UpdateRequest trackedData, OperationResponse response, Long batchCount,
			String schema, Map<String, String> dataTypes, Map<String, String> typeNames, StringBuilder query,
			int readTimeout, String autoIncrementColumn) {
		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching.
		List<ObjectData> batchedData = new ArrayList<>();
		for (ObjectData objdata : trackedData) {
			batchedData.add(objdata);
		}
		for (ObjectData data : batchedData) {
			try {
				query = QueryBuilderUtil.buildInitialInsertQuery(con, data, getContext().getObjectTypeId(), typeNames,
						autoIncrementColumn, schema);
				break;
			}catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, data, response);
			}
			catch (IllegalArgumentException e) {
				CustomResponseUtil.writeErrorResponse(e, data, response);
			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, "Error occurred! Moving on to the next record...", e);
			}
		}

		doBatch(con, dataTypes, batchCount, batchedData, response, query, readTimeout, typeNames,
				autoIncrementColumn);
	}

	/**
	 * Builds the query for each document and commits the transaction after executing all the operations.
	 *
	 * @param con
	 * @param trackedData
	 * @param response
	 * @param schema
	 * @param dataTypes
	 * @param typeNames
	 * @param readTimeout
	 * @param autoIncrementColumn
	 * @param payloadMetadata
	 * @throws SQLException
	 */
	protected void commitByProfile(Connection con, UpdateRequest trackedData, OperationResponse response,
			String schema,
			Map<String, String> dataTypes, Map<String, String> typeNames, int readTimeout, String autoIncrementColumn,
			PayloadMetadata payloadMetadata) throws SQLException {
		String databaseName = con.getMetaData().getDatabaseProductName();

		for (ObjectData objdata : trackedData) {
			try {
				QueryResponseWithId queryResponseWithId = processObjectData(con, schema, dataTypes,
						typeNames,
						readTimeout, autoIncrementColumn, objdata, databaseName);
				CustomResponseUtil.handleSuccess(objdata, response, payloadMetadata, queryResponseWithId);
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IOException | IllegalArgumentException e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, objdata, e);
			}
		}
	}

	private QueryResponseWithId processObjectData(Connection con, String schema,
			Map<String, String> dataTypes, Map<String, String> typeNames, int readTimeout, String autoIncrementColumn,
			ObjectData objdata, String databaseName) throws SQLException, IOException {
		StringBuilder query;
		List<Integer> ids = new ArrayList<>();

		query = QueryBuilderUtil.buildInitialInsertQuery(con, objdata, getContext().getObjectTypeId(), typeNames,
				autoIncrementColumn, schema);
		try (PreparedStatement st = con.prepareStatement(query.toString(), PreparedStatement.RETURN_GENERATED_KEYS)) {
			st.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			buildFinalQuery(con, objdata, dataTypes, st, typeNames, autoIncrementColumn);
			int effectedRowCount = st.executeUpdate();
			if (effectedRowCount > 1) {
				if (DatabaseConnectorConstants.MYSQL.equals(databaseName) || DatabaseConnectorConstants.MSSQL.equals(
						databaseName)) {
					InsertionIDUtil.getIdOfInsertedRecords(st, effectedRowCount);
				} else if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
					ids = InsertionIDUtil.queryLastIdPostgreSQL(con, effectedRowCount);
				}
			} else if (!DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
				ids = InsertionIDUtil.getInsertIds(st);
			}
			return new QueryResponseWithId(query.toString(), effectedRowCount, ids, "Executed Successfully");
		}
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param sqlConnection the sqlConnection
	 * @param dataTypes   the data types
	 * @param batchCount  the batch count
	 * @param batchedData the batched data
	 * @param response    the response
	 * @param query       the query
	 */
	private static void doBatch(Connection sqlConnection, Map<String, String> dataTypes, Long batchCount,
			List<ObjectData> batchedData, OperationResponse response, StringBuilder query, int readTimeout,
			Map<String, String> typeNames, String autoIncrementColumn) {
		int batchnum = 0;
		int currentBatchSize = 0;
		int currentDocIndex = 0;
		boolean shouldExecute = true;
		List<Integer> ids = new ArrayList<>();
		try (PreparedStatement bstmnt = sqlConnection.prepareStatement(query.toString(),
				PreparedStatement.RETURN_GENERATED_KEYS);) {

			bstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
			for (ObjectData objdata : batchedData) {
				currentBatchSize++;
				currentDocIndex++;
				Payload payload = null;
				try {
					buildFinalQuery(sqlConnection, objdata, dataTypes, bstmnt, typeNames,autoIncrementColumn);
					bstmnt.addBatch();
					if (currentBatchSize == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int[] res = bstmnt.executeBatch();
							if ((DatabaseConnectorConstants.MYSQL.equals(databaseName))
									|| (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName))) {
								ids = InsertionIDUtil.getInsertIds(bstmnt);
							}

							bstmnt.clearParameters();
							sqlConnection.commit();
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + batchnum);
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_RECORDS + res.length);
							if ((DatabaseConnectorConstants.MSSQL.equals(databaseName))) {
								payload = JsonPayloadUtil.toPayload(
										new BatchResponse("Batch executed successfully", batchnum, res.length));
							} else {
								payload = JsonPayloadUtil.toPayload(new BatchResponseWithId(
										"Batch executed successfully", batchnum, ids, res.length));
							}
							response.addResult(objdata, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
									DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
						} else {
							bstmnt.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
							CustomResponseUtil.batchExecuteError(objdata, response, batchnum, currentBatchSize);
						}
						currentBatchSize = 0;
					} else if (currentBatchSize < batchCount) {
						int remainingBatch = batchnum + 1;
						if (currentDocIndex == batchedData.size()) {
							executeRemaining(objdata, bstmnt, response, remainingBatch, sqlConnection, currentBatchSize, databaseName);
						} else {
							payload = JsonPayloadUtil.toPayload(
									new BatchResponse("Record added to batch successfully", remainingBatch, currentBatchSize));
							ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
						}

					}

				} catch (BatchUpdateException batchUpdateException) {
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					CustomResponseUtil.batchExecuteError(batchUpdateException, objdata, response, batchnum, currentBatchSize);
					currentBatchSize = 0;
				} catch (SQLException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					shouldExecute = checkLastRecord(currentBatchSize, batchCount);
					if (shouldExecute) {
						currentBatchSize = 0;
					}
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (IOException | IllegalArgumentException e) {
					shouldExecute = checkLastRecord(currentBatchSize, batchCount);
					if (shouldExecute || currentDocIndex == batchedData.size()) {
						bstmnt.clearBatch();
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

			LOG.info("Batching statements proccessed Successfully!!");
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage(), e);
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

	private static boolean checkLastRecord(int b, Long batchCount) {
		return b == batchCount;
	}

	/**
	 * This method will execute the remaining statements of the batching.
	 *
	 * @param data           the data
	 * @param execStatement  the exec statement
	 * @param response       the response
	 * @param remainingBatch the remaining batch
	 * @param sqlConnection  the sqlConnection
	 * @param b              the b
	 * @param databaseName   the database name
	 */
	private static void executeRemaining(ObjectData data, PreparedStatement execStatement, OperationResponse response,
			int remainingBatch, Connection sqlConnection, int b, String databaseName) {
		List<Integer> ids = new ArrayList<>();
		Payload payload = null;
		try {
			int[] res = execStatement.executeBatch();
			if ((DatabaseConnectorConstants.MYSQL.equals(databaseName))
					|| (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName))) {
				ids = InsertionIDUtil.getInsertIds(execStatement);
			}
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + remainingBatch);
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.REMAINING_BATCH_RECORDS + res.length);
			if ((DatabaseConnectorConstants.MSSQL.equals(databaseName))) {
				payload = JsonPayloadUtil.toPayload(new BatchResponse(
						"Remaining records added to batch and executed successfully", remainingBatch, res.length));
			} else {
				payload = JsonPayloadUtil.toPayload(new BatchResponseWithId(
						"Remaining records added to batch and executed successfully", remainingBatch, ids, res.length));
			}
			response.addResult(data, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
					DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
			sqlConnection.commit();
		} catch (SQLException e) {
			CustomResponseUtil.logFailedBatch(response, remainingBatch, b);
			CustomResponseUtil.writeSqlErrorResponse(e, data, response);
		} finally {
			IOUtil.closeQuietly(payload);
		}
	}

	/**
	 * This method will build the query based on the request parameter and data type
	 * for the incoming requests.
	 *
	 * @param sqlConnection the sqlConnection
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param bstmnt    the bstmnt
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private static void buildFinalQuery(Connection sqlConnection, ObjectData objdata, Map<String, String> dataTypes,
			PreparedStatement bstmnt, Map<String, String> typeNames, String autoIncrementColumn)
			throws IOException, SQLException {
		JsonNode json = null;
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		try (InputStream is = objdata.getData()) {
			// After filtering out the inputs (which are more than 1MB) we are loading the
			// inputstream to memory here.
			json = DBv2JsonUtil.getBigDecimalObjectMapper().readTree(is);
			if (json != null) {
				int i = 0;
				for (Map.Entry<String, String> entries : typeNames.entrySet()) {
					String key = entries.getKey();
					JsonNode fieldValue = json.get(key);
					if (dataTypes.containsKey(key) && (autoIncrementColumn == null || json.get(autoIncrementColumn)
							!= null || !autoIncrementColumn.equalsIgnoreCase(entries.getKey()))) {
						i++;
						switch (dataTypes.get(key)) {
							case DatabaseConnectorConstants.INTEGER:
								if (fieldValue != null) {
									BigDecimal num = new BigDecimal(
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
													""));
									bstmnt.setBigDecimal(i, num);
								} else {
									bstmnt.setNull(i, Types.INTEGER);
								}
								break;
							case DatabaseConnectorConstants.DATE:
								if (fieldValue != null) {
									if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
										bstmnt.setString(i, fieldValue.toString()
												.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
									} else {
										try {
											bstmnt.setDate(i, Date.valueOf(fieldValue.toString()
													.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
										} catch (IllegalArgumentException e) {
											throw new IllegalArgumentException(
													DatabaseConnectorConstants.INVALID_ERROR + e);
										}
									}
								} else {
									bstmnt.setNull(i, Types.DATE);
								}
								break;
							case DatabaseConnectorConstants.STRING:
								if (fieldValue != null) {
									bstmnt.setString(i,
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
													""));
								} else {
									bstmnt.setNull(i, Types.VARCHAR);
								}
								break;
							case DatabaseConnectorConstants.TIME:
								if (fieldValue != null) {
									String time =
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
											"");
									bstmnt.setTime(i, Time.valueOf(time));
								} else {
									bstmnt.setNull(i, Types.TIME);
								}
								break;
							case DatabaseConnectorConstants.NVARCHAR:
								if (fieldValue != null && !fieldValue.isNull()) {
									// If the fieldValue represents a textual value (e.g., a string)
									// Treat it as a string and unescape any escaped characters using a utility method
									if (fieldValue.isTextual()) {
										bstmnt.setString(i, QueryBuilderUtil.unescapeEscapedStringFrom(fieldValue));
									} else {
										// If the value is not textual, we fall back to converting the object to a
										// string and unescaping it
										String fieldValueAsString = fieldValue.toString();
										bstmnt.setString(i, StringEscapeUtils.unescapeJava(
												fieldValueAsString.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
									}
								} else {
									bstmnt.setNull(i, Types.NVARCHAR);
								}
								break;
							case DatabaseConnectorConstants.JSON:
								if (fieldValue != null) {
									QueryBuilderUtil.extractJson(fieldValue.toString(), bstmnt, i, databaseName);
								} else {
									bstmnt.setNull(i, Types.NULL);
								}
								break;
							case DatabaseConnectorConstants.BOOLEAN:
								if (fieldValue != null) {
									Boolean flag = Boolean.valueOf(
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
													""));
									bstmnt.setBoolean(i, flag);
								} else {
									bstmnt.setNull(i, Types.BOOLEAN);
								}
								break;
							case DatabaseConnectorConstants.LONG:
								if (fieldValue != null) {
									long value = Long.parseLong(
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
													""));
									bstmnt.setLong(i, value);
								} else {
									bstmnt.setNull(i, Types.BIGINT);
								}
								break;
							case DatabaseConnectorConstants.FLOAT:
								if (fieldValue != null) {
									float value = Float.parseFloat(
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
													""));
									bstmnt.setFloat(i, value);
								} else {
									bstmnt.setNull(i, Types.FLOAT);
								}
								break;
							case DatabaseConnectorConstants.DOUBLE:
								if (fieldValue != null) {
									BigDecimal num = new BigDecimal(
											fieldValue.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE,
													""));
									bstmnt.setBigDecimal(i, num);
								} else {
									bstmnt.setNull(i, Types.DECIMAL);
								}
								break;
							case DatabaseConnectorConstants.BLOB:
								if (fieldValue != null) {
									String value = fieldValue.toString().replace(
											DatabaseConnectorConstants.DOUBLE_QUOTE, "");
									try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
										if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
											bstmnt.setBinaryStream(i, stream);
										} else {
											bstmnt.setBlob(i, stream);
										}
									}
								} else {
									bstmnt.setNull(i, Types.BLOB);
								}
								break;
							case DatabaseConnectorConstants.TIMESTAMP:
								if (fieldValue != null) {
									String timeStamp = fieldValue.toString().replace(
											DatabaseConnectorConstants.DOUBLE_QUOTE, "");
									bstmnt.setTimestamp(i, Timestamp.valueOf(timeStamp));
								} else {
									bstmnt.setNull(i, Types.TIMESTAMP);
								}
								break;
							default:
								break;
						}
					}
				}
			} else {
				throw new ConnectorException("Please check the input data!!");
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