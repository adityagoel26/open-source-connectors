// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.update;

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
import com.boomi.connector.databaseconnector.model.UpdatePojo;
import com.boomi.connector.databaseconnector.model.UpdatePojo.Set;
import com.boomi.connector.databaseconnector.model.Where;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
 * The Class DynamicUpdateOperation.
 *
 * @author swastik.vn
 */
public class DynamicUpdateOperation extends SizeLimitedUpdateOperation {

	/**
	 * Instantiates a new dynamic update operation.
	 *
	 * @param connection the connection
	 */
	public DynamicUpdateOperation(DatabaseConnectorConnection connection) {
		super(connection);
	}

	/** The Constant logger. */
	private static final Logger LOG = Logger.getLogger(DynamicUpdateOperation.class.getName());

	/**
	 * Returns true in {@link DynamicUpdateOperation} class.
	 * @return
	 */
	protected boolean shouldCommit(){
		return true;
	}

	/**
	 * Execute update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if(sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			DatabaseUtil.setupConnection(getContext(), sqlConnection, databaseConnectorConnection);
			this.executeUpdateOperation(request, response, sqlConnection);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}

	}

	/**
	 * Update Operation where it will take the List of ObjectData and
	 * OperationResponse and Process the Requests.
	 *
	 * @param trackedData the tracked data
	 * @param response    the response
	 * @param sqlConnection         the sqlConnection
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	public void executeUpdateOperation(UpdateRequest trackedData, OperationResponse response, Connection sqlConnection)
			throws SQLException, IOException {

		Long batchCount = getContext().getOperationProperties().getLongProperty(DatabaseConnectorConstants.BATCH_COUNT);
		String commitOption = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.COMMIT_OPTION);
		DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
		String schema = getSchema(sqlConnection, databaseMetaData);

		// This Map will be getting the datatype of the each column associated with the
		// table.
		Map<String, String> dataType = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
				schema).getDataType();
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0;

		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching and for creating the Query for Prepared Statement.
		List<ObjectData> batchData = new ArrayList<>();
		for (ObjectData objdata : trackedData) {
			batchData.add(objdata);
		}
		String databaseName = databaseMetaData.getDatabaseProductName();
		StringBuilder query = this.getInitialQuery(
				QueryBuilderUtil.checkTableName(getContext().getObjectTypeId(), databaseName, schema));
		this.appendKeys(batchData, query, response);
		if (batchCount != null && batchCount > 0 && DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(commitOption)) {
			this.doBatch(sqlConnection, dataType, batchCount, batchData, response, query, readTimeout);
		} else if (DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(commitOption) || batchCount == null
				|| batchCount == 0) {
			java.util.Set<String> columnNames = QueryBuilderUtil.retrieveTableColumns(getContext(), sqlConnection,
					schema);
			for (ObjectData data : batchData) {
				try {
					int updatedRowCount = executeNonBatchUpdate(data, sqlConnection, dataType, query,
							readTimeout, columnNames);
					QueryResponse queryResponse = new QueryResponse(query.toString(), updatedRowCount,
							"Executed Successfully");
					CustomResponseUtil.handleSuccess(data, response, null, queryResponse);
				} catch (SQLException e) {
					CustomResponseUtil.writeSqlErrorResponse(e, data, response);
				} catch (IOException | IllegalArgumentException | ConnectorException e) {
					CustomResponseUtil.writeErrorResponse(e, data, response);
				}
			}
			DatabaseUtil.commit(sqlConnection);
			LOG.log(Level.FINE, "Non Batching statements proccessed Successfully!!");
		} else if (batchCount < 0) {
			throw new ConnectorException("Batch Count Cannot be negative!!!");
		}

	}

	/**
	 * This method is to get the schema name
	 *
	 * @param sqlConnection
	 * @param databaseMetaData
	 * @return schema name
	 * @throws SQLException
	 */
	private String getSchema(Connection sqlConnection, DatabaseMetaData databaseMetaData) throws SQLException {
		String schemaName = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.SCHEMA_NAME);
		return QueryBuilderUtil.getSchemaFromConnection(databaseMetaData.getDatabaseProductName(), sqlConnection,
				schemaName, getConnection().getSchemaName());
	}

	/**
	 * This method will execute the non batching statements.
	 *
	 * @param data        the data
	 * @param con         the con
	 * @param dataType    the data type
	 * @param query       the query
	 * @param readTimeout the read timeout
	 * @param columnNames the column names
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	protected int executeNonBatchUpdate(ObjectData data, Connection con, Map<String, String> dataType,
			StringBuilder query, int readTimeout, java.util.Set<String> columnNames) throws SQLException, IOException {

		int updatedRowCount = 0;
		try (PreparedStatement execStatement = con.prepareStatement(query.toString())) {
			execStatement.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			this.appendValues(data, execStatement, dataType, con, columnNames);
			updatedRowCount = execStatement.executeUpdate();
			if (shouldCommit()) {
				con.commit();
			}
		}
		return updatedRowCount;
	}

	/**
	 * Append values to the non batching Statements.
	 *
	 * @param data     the data
	 * @param query    the query
	 * @param dataType the data type
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void appendValuesNonBatch(ObjectData data, StringBuilder query, Map<String, String> dataType)
			throws IOException {
		ObjectReader reader = DBv2JsonUtil.getObjectReader()
				.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

		try (InputStream is = data.getData()) {
			UpdatePojo updatePojo = reader.readValue(is, UpdatePojo.class);
			boolean comma = false;
			for (Set set : updatePojo.getSet()) {
				setInitial(comma, query);
				String key = set.getColumn();
				query.append(key);
				query.append("=");
				String value = set.getValue();
				this.checkDataType(dataType, key, value, query);
				comma = true;
			}

			comma = false;
			if (updatePojo.getWhere() != null) {
				for (Where where : updatePojo.getWhere()) {
					whereInitial(comma, query);
					String column = where.getColumn();
					query.append(column);
					String operator = where.getOperator();
					query.append(operator);
					String value = where.getValue();
					this.checkDataType(dataType, column, value, query);
					comma = true;
				}

			}

		}

	}

	/**
	 * This method will check the datatype of the column and append the values to
	 * the query according to the datatype.
	 *
	 * @param dataType the data type
	 * @param key      the key
	 * @param value    the value
	 * @param query    the query
	 */
	public void checkDataType(Map<String, String> dataType, String key, String value, StringBuilder query) {

		if (null != dataType.get(key) && (DatabaseConnectorConstants.STRING.equals(dataType.get(key))
				|| DatabaseConnectorConstants.DATE.equals(dataType.get(key))
				|| dataType.get(key).equals(DatabaseConnectorConstants.TIME))) {
			query.append(DatabaseConnectorConstants.SINGLE_QUOTE);
			query.append(value);
			query.append(DatabaseConnectorConstants.SINGLE_QUOTE);
		} else if (null != dataType.get(key) && (DatabaseConnectorConstants.INTEGER.equals(dataType.get(key))
				|| DatabaseConnectorConstants.BOOLEAN.equals(dataType.get(key)))) {
			query.append(value);
		} else {
			throw new ConnectorException("Invalid Column names");
		}

	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param sqlConnection        the sqlConnection
	 * @param dataType   the data type
	 * @param batchCount the batch count
	 * @param batchData  the tracked data
	 * @param response   the response
	 * @param query      the query
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void doBatch(Connection sqlConnection, Map<String, String> dataType,
						 Long batchCount, List<ObjectData> batchData,
			OperationResponse response, StringBuilder query, int readTimeout){
		int currentBatchSize = 0;
		int batchnum = 0;
		int currentDocIndex = 0;
		boolean shouldExecute = true;

		try (PreparedStatement execStatement = sqlConnection.prepareStatement(query.toString());) {
			execStatement.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			java.util.Set<String> columnNames = QueryBuilderUtil.retrieveTableColumns(getContext(), sqlConnection,
					getSchema(sqlConnection, sqlConnection.getMetaData()));
			for (ObjectData data : batchData) {

				Payload payload = null;
				try {
					currentBatchSize++;
					currentDocIndex++;
					this.appendValues(data, execStatement, dataType, sqlConnection, columnNames);
					execStatement.addBatch();
					if (currentBatchSize == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int[] res = execStatement.executeBatch();
							sqlConnection.commit();
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + batchnum);
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_RECORDS + res.length);
							payload = JsonPayloadUtil
									.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
							response.addResult(data, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
									DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
						} else {
							execStatement.clearBatch();
							execStatement.clearParameters();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
							CustomResponseUtil.batchExecuteError(data, response, batchnum, currentBatchSize);
						}
						currentBatchSize = 0;
					} else if (currentBatchSize < batchCount) {
						int remainingBatch = batchnum + 1;
						if (currentDocIndex == batchData.size()) {
							executeRemaining(data, execStatement, response, remainingBatch, sqlConnection, currentBatchSize);
						} else {
							payload = JsonPayloadUtil.toPayload(
									new BatchResponse("Record added to batch successfully", remainingBatch, currentBatchSize));
							ResponseUtil.addSuccess(response, data, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
						}
					}

				} catch (BatchUpdateException batchUpdateException) {
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					CustomResponseUtil.batchExecuteError(batchUpdateException, data, response, batchnum, currentBatchSize);
					currentBatchSize = 0;
				} catch (SQLException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					shouldExecute = checkLastRecord(currentBatchSize, batchCount);
					if (shouldExecute) {
						currentBatchSize = 0;
					}
					CustomResponseUtil.writeSqlErrorResponse(e, data, response);
				} catch (IOException | IllegalArgumentException e) {
					shouldExecute = checkLastRecord(currentBatchSize, batchCount);
					if (shouldExecute || currentDocIndex == batchData.size()) {
						execStatement.clearBatch();
						batchnum++;
						CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
						currentBatchSize = 0;
					}
					CustomResponseUtil.writeErrorResponse(e, data, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, data, e);
				} finally {
					IOUtil.closeQuietly(payload);
				}
			}

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
	 * @param sqlConnection            the sqlConnection
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
		} catch (SQLException e) {
			CustomResponseUtil.logFailedBatch(response, remainingBatch, b);
			CustomResponseUtil.writeSqlErrorResponse(e, data, response);
		} finally {
			IOUtil.closeQuietly(payload);
		}

	}

	/**
	 * This Method will Build the Initial String required for the Update Statement.
	 *
	 * @param objectTypeId the object type id
	 * @return new StringBuilder with initial string required for Update Operation
	 */
	protected StringBuilder getInitialQuery(String objectTypeId) {
		return new StringBuilder("UPDATE " + objectTypeId + " SET ");
	}

	/**
	 * This method will build the prepared statement query required for Update
	 * Operation based on the SET and WHERE parameters in the Requests.
	 *
	 * @param batchData the data
	 * @param query     the query
	 * @param response  the response
	 */
	public void appendKeys(List<ObjectData> batchData, StringBuilder query, OperationResponse response) {

		for (ObjectData data : batchData) {
			boolean      dataConsistent = false;
			ObjectReader reader         = DBv2JsonUtil.getObjectReader();
			reader.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			try (InputStream is = data.getData()) {
				UpdatePojo updatePojo = reader.readValue(is, UpdatePojo.class);
				boolean comma = false;
				for (Set set : updatePojo.getSet()) {
					setInitial(comma, query);
					String key = set.getColumn();
					if (key != null) {
						dataConsistent = true;
						
						query.append(key);
						query.append("=");
						query.append("?");
					}
					comma = true;
				}

				comma = false;
				if (updatePojo.getWhere() != null) {
					for (Where where : updatePojo.getWhere()) {
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
	 * This method will append the values to the prepared statements built for
	 * Update Query.
	 *
	 * @param data          the data
	 * @param execStatement the exec statement
	 * @param dataType      the data type
	 * @param sqlConnection the sqlConnection
	 * @param columnNames   the column names
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException
	 */
	public void appendValues(ObjectData data, PreparedStatement execStatement, Map<String, String> dataType,
			Connection sqlConnection, java.util.Set<String> columnNames) throws IOException, SQLException {
		boolean flag = true;
		try (InputStream is = data.getData()) {
			ObjectReader reader = DBv2JsonUtil.getObjectReader()
					.without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			UpdatePojo updatePojo = reader.readValue(is, UpdatePojo.class);
			int i = 0;
			for (Set set : updatePojo.getSet()) {
				i++;
				String key = set.getColumn();
				String value = set.getValue();
				flag = QueryBuilderUtil.doesColumnExistInTable(key, columnNames);
				if (flag) {
					QueryBuilderUtil.checkDataType(dataType, key, value, execStatement, i, sqlConnection);
				}else {
					throw new ConnectorException("The column name " +key+ " does not exists in the database.");
				}
			}

			if (updatePojo.getWhere() != null) {
				i++;
				for (Where where : updatePojo.getWhere()) {
					String column = where.getColumn();
					String value = where.getValue();
					flag = QueryBuilderUtil.doesColumnExistInTable(column, columnNames);
					if(flag) {
					QueryBuilderUtil.checkDataType(dataType, column, value, execStatement, i, sqlConnection);
					i++;
					}
					else {
						throw new ConnectorException("The column name " +column+ " does not exists in the database.");
					}
				}

			}

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
	 * This method will append the ',' after each parameter in the SET clause are
	 * set.
	 *
	 * @param comma the comma
	 * @param query the query
	 */
	private static void setInitial(boolean comma, StringBuilder query) {
		if (comma) {
			query.append(DatabaseConnectorConstants.COMMA);
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
