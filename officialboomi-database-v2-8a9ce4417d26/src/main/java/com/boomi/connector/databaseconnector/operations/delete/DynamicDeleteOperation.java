// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.delete;

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
import com.boomi.connector.databaseconnector.model.DeletePojo;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.model.Where;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;
import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class DynamicDeleteOperation.
 *
 * @author swastik.vn
 */
public class DynamicDeleteOperation extends SizeLimitedUpdateOperation {

	/**
	 * Instantiates a new dynamic delete operation.
	 *
	 * @param databaseConnectorConnection the databaseConnectorConnection
	 */
	public DynamicDeleteOperation(DatabaseConnectorConnection databaseConnectorConnection) {
		super(databaseConnectorConnection);
	}

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(DynamicDeleteOperation.class.getName());

	/**
	 * Execute update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		Long batchCount = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.BATCH_COUNT);
		String commitOption = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.COMMIT_OPTION);
		List<ObjectData> trackedData = getObjectDataList(request);
		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if(sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			String schemaName = (String) getContext().getOperationProperties()
					.get(DatabaseConnectorConstants.SCHEMA_NAME);
			String schemaNameFromConnection = databaseConnectorConnection.getSchemaName();
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, schemaNameFromConnection);
			sqlConnection.setAutoCommit(false);
			this.executeStatements(sqlConnection, trackedData, response, batchCount, commitOption,
					schemaNameFromConnection);
			LOG.log(Level.INFO, "Statements Executed!!");
		}
		catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, trackedData, e);
		}
	}

	/**
	 * This method consumer iterator of the provided UpdateRequest object and returns a list of ObjectData instances.
	 * @param request the UpdateRequest object to be consumed
	 * @return a list of ObjectData instances from the consumed UpdateRequest
	 */
	protected static List<ObjectData> getObjectDataList(UpdateRequest request) {
		List<ObjectData> trackedData = new ArrayList<>();
		for (ObjectData objdata : request) {
			trackedData.add(objdata);
		}
		return trackedData;
	}

	/**
	 * Execute statements. This Method will take the input params and process the
	 * statements according to the batch count provided
	 *
	 * @param sqlConnection            the sqlConnection
	 * @param trackedData              the request
	 * @param response                 the response
	 * @param batchCount               the batch count
	 * @param commitOption             the commit option
	 * @param schemaNameFromConnection
	 * @throws SQLException the SQL exception
     */
	private void executeStatements(Connection sqlConnection, List<ObjectData> trackedData,
								   OperationResponse response, Long batchCount,
								   String commitOption, String schemaNameFromConnection) throws SQLException {

		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching and for creating the Query for Prepared Statement.
		DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
		String schemaName = getContext().getOperationProperties()
				.getProperty(DatabaseConnectorConstants.SCHEMA_NAME);
		String schema = QueryBuilderUtil.getSchemaFromConnection(databaseMetaData.getDatabaseProductName(),
				sqlConnection, schemaName, schemaNameFromConnection);
		String databaseName = databaseMetaData.getDatabaseProductName();
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0;
		StringBuilder query = new StringBuilder(DatabaseConnectorConstants.DELETE_QUERY +
				QueryBuilderUtil.checkTableName(getContext().getObjectTypeId(), databaseName, schema));
		this.appendKeys(trackedData, query);
		Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
				schema).getDataType();
		if (batchCount != null && batchCount > 0 && DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(commitOption)) {
			this.doBatch(sqlConnection, dataTypes, batchCount, trackedData, response, readTimeout, schema);
		} else if (DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(commitOption) ||
				batchCount == null || batchCount == 0) {
			commitByProfile(sqlConnection, trackedData, response, query, readTimeout, dataTypes, null);
			DatabaseUtil.commit(sqlConnection);

		} else if (batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	/**
	 * Commits the changes for the given list of {@link ObjectData} objects by executing the
	 * specified SQL query.
	 *
	 * @param con             the {@link Connection} to the database
	 * @param trackedData     the list of {@link ObjectData} objects to be committed
	 * @param response        the {@link OperationResponse} object to store the results
	 * @param query           the SQL query to be executed
	 * @param readTimeout     the read timeout value for the database connection
	 * @param dataTypes       a map of data types for the SQL query parameters
	 * @param payloadMetadata the {@link PayloadMetadata} object containing additional metadata
	 *
	 * @throws SQLException          if a database error occurs during the execution of the SQL query
	 * @throws IOException           if an I/O error occurs during the execution of the SQL query
	 * @throws IllegalArgumentException if an invalid argument is provided
	 * @throws ConnectorException    if a connector-specific error occurs
	 */
	protected void commitByProfile(Connection con, List<ObjectData> trackedData, OperationResponse response,
								   StringBuilder query, int readTimeout, Map<String, String> dataTypes,
								   PayloadMetadata payloadMetadata) {
		for (ObjectData objdata : trackedData) {
			try (PreparedStatement bstmnt = con.prepareStatement(query.toString());) {
				int rowsEffected = processObjectData(con, response, readTimeout, dataTypes, objdata, bstmnt);
				CustomResponseUtil.handleSuccess(objdata, response, payloadMetadata,
						new QueryResponse(query.toString(), rowsEffected, "Executed Successfully"));
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IOException | IllegalArgumentException| ConnectorException e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			}
		}
	}

	private int processObjectData(Connection con, OperationResponse response, int readTimeout,
								  Map<String, String> dataTypes, ObjectData objdata,
								  PreparedStatement bstmnt) throws SQLException, IOException {
		bstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
		this.appendValues(objdata, bstmnt, dataTypes, con, response);
		return bstmnt.executeUpdate();
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param sqlConnection         the sqlConnection
	 * @param dataTypes   the data types
	 * @param batchCount  the batch count
	 * @param trackedData the tracked data
	 * @param response    the response
	 * @param readTimeout the read timeout
	 * @param schemaName the schema name
	 * @throws SQLException
	 */
	private void doBatch(Connection sqlConnection, Map<String, String> dataTypes, Long batchCount,
			List<ObjectData> trackedData, OperationResponse response, int readTimeout, String schemaName)
			throws SQLException {

		int batchnum = 0;
		int b = 0;
		boolean shouldExecute = true;
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		StringBuilder query = new StringBuilder(DatabaseConnectorConstants.DELETE_QUERY +
				QueryBuilderUtil.checkTableName(getContext().getObjectTypeId(), databaseName, schemaName));
		this.appendKeys(trackedData, query);
		try (PreparedStatement bstmnt = sqlConnection.prepareStatement(query.toString());) {
			bstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			for (ObjectData objdata : trackedData) {
				b++;
				Payload payload = null;
				try {
					this.appendValues(objdata, bstmnt, dataTypes, sqlConnection, response);
					bstmnt.addBatch();
					if (b == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int[] res = bstmnt.executeBatch();
							bstmnt.clearParameters();
							sqlConnection.commit();
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + batchnum);
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_RECORDS + res.length);
							payload = JsonPayloadUtil
									.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
							response.addResult(objdata, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
									DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
						} else {
							bstmnt.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, b);
							CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
						}

						b = 0;

					} else if (b < batchCount) {
						int remainingBatch = batchnum + 1;
						if (trackedData.lastIndexOf(objdata) == trackedData.size() - 1) {
							executeRemaining(objdata, bstmnt, response, remainingBatch, sqlConnection, b);
						} else {
							payload = JsonPayloadUtil.toPayload(
									new BatchResponse("Record added to batch successfully", remainingBatch, b));
							ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
						}
					}
				} catch (BatchUpdateException e) {

					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
					b = 0;
				} catch (SQLException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					shouldExecute = checkLastRecord(b, batchCount);
					if (shouldExecute) {
						b = 0;
					}
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (IOException | IllegalArgumentException e) {
					shouldExecute = checkLastRecord(b, batchCount);
					if (shouldExecute || trackedData.lastIndexOf(objdata) == trackedData.size() - 1) {
						bstmnt.clearBatch();
						batchnum++;
						CustomResponseUtil.logFailedBatch(response, batchnum, b);
						b = 0;
					}
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} finally {
					IOUtil.closeQuietly(payload);
				}
			}

		} catch (SQLException e) {
			throw new ConnectorException("Creating the statement got failed from Connection", e);
		}

	}

	/**
	 * This method will append the question mark place holders to the query based on
	 * the column names.
	 *
	 * @param batchData the batch data
	 * @param query     the query
	 */
	public void appendKeys(List<ObjectData> batchData, StringBuilder query) {

		for (ObjectData data : batchData) {
			boolean dataConsistent = false;
			ObjectReader reader = DBv2JsonUtil.getObjectReader();

			try (InputStream is = data.getData()) {
				DeletePojo deletePojo = reader.readValue(is, DeletePojo.class);
				boolean comma = false;
				if (deletePojo.getWhere() != null) {
					for (Where where : deletePojo.getWhere()) {
						whereInitial(comma, query);
						String column = where.getColumn();
						query.append(column);
						String operator = where.getOperator();
						query.append(operator);
						String value = where.getValue();
						if (value != null) {
							query.append("?");
						}
						comma = true;
						dataConsistent = true;
					}
				}
			} catch (IOException e) {
				// moving to next request
				LOG.log(Level.SEVERE, e.getMessage(), e);
			}
			if (dataConsistent) {
				break;
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

	private static boolean checkLastRecord(int b, Long batchCount) {
		return b == batchCount;
	}

	/**
	 * This method will append the values to the Prepared Statement Parameters.
	 *
	 * @param data          the data
	 * @param execStatement the exec statement
	 * @param dataType      the data type
	 * @param sqlConnection the sqlConnection
	 * @param response      the response
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void appendValues(ObjectData data, PreparedStatement execStatement, Map<String, String> dataType,
							 Connection sqlConnection, OperationResponse response) throws IOException {
		boolean flag = true;
		try (InputStream is = data.getData()) {
			ObjectReader reader     = DBv2JsonUtil.getObjectReader();
			DeletePojo   deletePojo = reader.readValue(is, DeletePojo.class);
			int i = 0;
			if (deletePojo.getWhere() != null) {
				for (Where where : deletePojo.getWhere()) {
					i++;
					String column = where.getColumn();
					String value = where.getValue();
					flag = QueryBuilderUtil.doesColumnExistInTable(sqlConnection, column, getContext().getObjectTypeId());
					if(flag) {
						QueryBuilderUtil.checkDataType(dataType, column, value, execStatement, i, sqlConnection);
					}
					else {
						throw new ConnectorException("The column name " +column+ " does not exists in the database.");
					}
				}
			}
		} catch (SQLException e) {
			ResponseUtil.addExceptionFailure(response, data, e);
		}

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
	 */
	private static void executeRemaining(ObjectData data, PreparedStatement execStatement, OperationResponse response,
								  int remainingBatch, Connection sqlConnection, int b) {
		Payload payload = null;
		try {
			int[] res = execStatement.executeBatch();
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + remainingBatch);
			response.getLogger().log(Level.INFO, DatabaseConnectorConstants.REMAINING_BATCH_RECORDS + res.length);
			payload = JsonPayloadUtil.toPayload(new BatchResponse(
					"Remaining records added to batch and executed successfully", remainingBatch, res.length));
			response.addResult(data, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
					DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
			sqlConnection.commit();
		} catch (BatchUpdateException e) {
			CustomResponseUtil.logFailedBatch(response, remainingBatch, b);
			CustomResponseUtil.writeSqlErrorResponse(e, data, response);
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, data, response);
		} finally {
			IOUtil.closeQuietly(payload);
		}

	}

	/**
	 * This method will append the Where clause to the query if any values present
	 * in the Where parameters. Also it will check whether parameter is first one to
	 * append AND for multiple Where values.
	 *
	 * @param comma the comma
	 * @param query the query
	 */
	private static void whereInitial(boolean comma, StringBuilder query) {
		if (comma) {
			query.append(" AND ");
		} else {
			query.append(" WHERE ");
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