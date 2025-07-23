// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.upsert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.BatchResponse;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.commons.text.StringEscapeUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class MysqlUpsert.
 *
 * @author swastik.vn
 */
public class MysqlUpsert {

	/** The SQL connection */
	private final Connection _sqlConnection;
	/** The batch count. */
	private final Long _batchCount;
	/** The table name. */
	private String _tableName;
	/** The commit option. */
	private final String _commitOption;
	/** The table column name set. */
	private Set<String> _columnNames;
	/** The reader. */
	private final ObjectReader _reader = DBv2JsonUtil.getObjectReader();
	/** The Constant logger. */
	private static final Logger LOG = Logger.getLogger(MysqlUpsert.class.getName());

	/**
	 * Instantiates a new mysql upsert.
	 *
	 * @param sqlConnection          the sqlConnection
	 * @param batchCount   the batch count
	 * @param tableName    the table name
	 * @param commitOption the commit option
	 * @param columnNames   the column name
	 */
	public MysqlUpsert(Connection sqlConnection, Long batchCount, String tableName, String commitOption,
			Set<String> columnNames) {
		_sqlConnection = sqlConnection;
		_batchCount = batchCount;
		_tableName = tableName;
		_commitOption = commitOption;
		_columnNames = columnNames != null ? columnNames : new LinkedHashSet<>();
	}

	/**
	 * Entry point for MYSQL UPSERT Operation where as this method will process the
	 * Inputs and builds the statements and does the JDBC batching based on the
	 * commit options.
	 *
	 * @param trackedData
	 * @param response
	 * @param readTimeout
	 * @param schemaName
	 * @param payloadMetadata
	 * @throws SQLException
	 */
	public void executeStatements(UpdateRequest trackedData, OperationResponse response, int readTimeout,
			String schemaName, PayloadMetadata payloadMetadata) throws SQLException {

		Map<String, String> dataTypes = new MetadataExtractor(_sqlConnection, _tableName, schemaName).getDataType();
		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching and for creating the Query for Prepared Statement.
		List<ObjectData> batchData = new ArrayList<>();
		for (ObjectData objdata : trackedData) {
			batchData.add(objdata);
		}
		StringBuilder query = QueryBuilderUtil.buildInitialQuery(_sqlConnection, _tableName, schemaName, _columnNames);
		buildInsertQueryStatement(query, batchData, true);
		buildOnDuplicateKeyUpdate(query);
		buildInsertQueryStatement(query, batchData, false);
		if ((_batchCount != null) && (_batchCount > 0) && DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(_commitOption)) {
			this.doBatch(batchData, response, dataTypes, query, readTimeout);
		} else if ((_batchCount == null) || (_batchCount == 0) || DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(
				_commitOption)) {
			commitByProfile(response, readTimeout, dataTypes, batchData, query, payloadMetadata);
		} else if (_batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	/**
	 * Commit by Profile
	 * @param response
	 * @param readTimeout
	 * @param dataTypes
	 * @param batchData
	 * @param query
	 * @param payloadMetadata
	 * @throws SQLException
	 */
	private void commitByProfile(OperationResponse response, int readTimeout, Map<String, String> dataTypes,
			List<ObjectData> batchData, StringBuilder query, PayloadMetadata payloadMetadata) throws SQLException {
		try (PreparedStatement pstmnt = _sqlConnection.prepareStatement(query.toString())) {
			pstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			for (ObjectData objdata : batchData) {
				try {
					int pos = this.buildInsertValues(pstmnt, objdata, dataTypes);
					this.appendUpdate(pstmnt, objdata, dataTypes, response, pos);
					int effectedRowCount = pstmnt.executeUpdate();
					QueryResponse queryResponse = new QueryResponse(query.toString(), effectedRowCount,
							"Executed Successfully");
					CustomResponseUtil.handleSuccess(objdata, response, payloadMetadata, queryResponse);
				} catch (SQLException e) {
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, objdata, e);
				} catch (IOException | IllegalArgumentException e) {
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				}
			}
		}
	}

	/**
	 * Does the JDBC Statement batching if the batch count is greater than zero and
	 * commit option is commit by rows. This method will take the input request and
	 * Builds the SQL Statements and does the batching.
	 *
	 * @param batchData the tracked data
	 * @param response  the response
	 * @param dataTypes the data types
	 * @param query     the query
	 */
	private void doBatch(List<ObjectData> batchData, OperationResponse response, Map<String, String> dataTypes,
			StringBuilder query, int readTimeout)  {

		int batchnum = 0;
		int currentBatchSize = 0;
		int currentDocIndex = 0;
		boolean shouldExecute = true;

		try (PreparedStatement bstmnt = _sqlConnection.prepareStatement(query.toString())) {
			bstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			for (ObjectData objdata : batchData) {
				Payload payload = null;
				currentBatchSize++;
				currentDocIndex++;
				try {
					int pos = this.buildInsertValues(bstmnt, objdata, dataTypes);
					this.appendUpdate(bstmnt, objdata, dataTypes, response, pos);
					bstmnt.addBatch();
					if (currentBatchSize == _batchCount) {
						batchnum++;
						if (shouldExecute) {
							int[] res = bstmnt.executeBatch();
							bstmnt.clearParameters();
							_sqlConnection.commit();
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_NUM + batchnum);
							response.getLogger().log(Level.INFO, DatabaseConnectorConstants.BATCH_RECORDS + res.length);
							payload = JsonPayloadUtil
									.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
							response.addResult(objdata, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
									DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
						} else {
							bstmnt.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
							CustomResponseUtil.batchExecuteError(objdata, response, batchnum, currentBatchSize);
						}
						currentBatchSize = 0;
					} else if (currentBatchSize < _batchCount) {
						int remainingBatch = batchnum + 1;
						if (currentDocIndex == batchData.size()) {
							executeRemaining(objdata, bstmnt, response, remainingBatch, _sqlConnection, currentBatchSize);
						} else {
							payload = JsonPayloadUtil.toPayload(
									new BatchResponse("Record added to batch successfully", remainingBatch, currentBatchSize));
							ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
									payload);
						}

					}

				} catch (BatchUpdateException batchUpdateException) {
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					CustomResponseUtil.batchExecuteError(batchUpdateException, objdata, response, batchnum, currentBatchSize);
					currentBatchSize = 0;
				} catch (SQLException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
					shouldExecute = checkLastRecord(currentBatchSize, _batchCount);
					if (shouldExecute) {
						currentBatchSize = 0;
					}
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (IOException | IllegalArgumentException e) {
					shouldExecute = checkLastRecord(currentBatchSize, _batchCount);
					if (shouldExecute || currentDocIndex == batchData.size()) {
						bstmnt.clearBatch();
						bstmnt.clearParameters();
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

		} catch (Exception e) {
			throw new ConnectorException(e);
		}

	}

	/**
	 * Builds the insert query required for Prepared Statement by taking the values
	 * from the 1st request. If the 1st request is improper then the loop will
	 * continue until it gets the correct request to form the insert Query.
	 *
	 * @param query        the query
	 * @param batchData    the batch data
	 * @param insert       the insert
	 */
	 private void buildInsertQueryStatement(StringBuilder query,
			List<ObjectData> batchData,
			boolean insert) {
		JsonNode json = null;
		for (ObjectData objdata : batchData) {
			boolean consistent = false;
			try (InputStream is = objdata.getData()) {
				// After filtering out the inputs (which are more than 1MB) we are loading the
				// inputstream to memory here.
				json = _reader.readTree(is);
				if (json != null) {
						for (String key : _columnNames) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								if (!insert) {
									query.append(key).append("=");
									query.append(DatabaseConnectorConstants.PARAM);
								} else {
									query.append(DatabaseConnectorConstants.PARAM);
								}
								consistent = true;
							}
						}
						query.deleteCharAt(query.length() - 1);
						if (insert) {
							query.append(")");
						}
				} else {
					throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
				}

			} catch (IOException e) {
				// moving to next request
				LOG.log(Level.SEVERE, e.getMessage(), e);
			}
			if (consistent) {
				break;
			}
		}
	}

	/**
	 * Appends ON DUPLICATE KEY UPDATE to the query.
	 *
	 * @param query the query
	 */
	private static void buildOnDuplicateKeyUpdate(StringBuilder query) {
		query.append(" ON DUPLICATE KEY UPDATE ");

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
	 * This method will append the parameters to the On Duplicate Key Query Part by
	 * taking the position from the Insert Query.
	 *
	 * @param bstmnt       the query
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @param response     the response
	 * @param pos          the pos
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void appendUpdate(PreparedStatement bstmnt, ObjectData objdata, Map<String, String> dataTypes,
			OperationResponse response, int pos) throws IOException, SQLException {

		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			// After filtering out the inputs (which are more than 1MB) we are loading the
			// inputstream to memory here.
			json = _reader.readTree(is);
			if (json != null) {
				populatePreparedStatementWithJsonData(bstmnt, objdata, dataTypes, response, pos, json);
			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}
		}
    }

	/**
	 * Populates the given PreparedStatement with data extracted from the provided JSON node.
	 * It checks the data types and increments the position for each parameter set.
	 *
	 * @param preparedStatement the PreparedStatement to be populated
	 * @param objectData        the ObjectData object containing data relevant to the operation
	 * @param dataTypes         a Map of data types for each column
	 * @param response          the OperationResponse object for writing error responses in case of an SQLException
	 * @param position          the initial position for the parameter index
	 * @param jsonNode          the JsonNode object containing the data to be checked against the database columns
	 * @throws IOException if an input or output exception occurs
	 */
	private void populatePreparedStatementWithJsonData(PreparedStatement preparedStatement, ObjectData objectData,
			Map<String, String> dataTypes, OperationResponse response, int position, JsonNode jsonNode) throws IOException {
		try {
			for (String key : _columnNames) {
				JsonNode fieldName = jsonNode.get(key);
				if (fieldName != null) {
					position++;
					this.checkDataType(preparedStatement, key, fieldName, dataTypes, position);
				}
			}
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objectData, response);
		}
	}

	/**
	 * This method will Check for the datatype and append the Statements with values
	 * based on the datatype.
	 *
	 * @param bstmnt    the query
	 * @param key       the key
	 * @param value     the value
	 * @param dataTypes the data types
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private void checkDataType(PreparedStatement bstmnt, String key, JsonNode value, Map<String, String> dataTypes,
			int i) throws SQLException, IOException {
		switch (dataTypes.get(key)) {
			case DatabaseConnectorConstants.INTEGER:
				if (!value.isNull()) {
					BigDecimal num = new BigDecimal(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					bstmnt.setBigDecimal(i, num);
				} else {
					bstmnt.setNull(i, Types.INTEGER);
				}
				break;
			case DatabaseConnectorConstants.DATE:
				if (!value.isNull()) {
					if (DatabaseConnectorConstants.ORACLE.equals(_sqlConnection.getMetaData().getDatabaseProductName())) {
						bstmnt.setString(i, value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					} else {
						try {
							bstmnt.setDate(i, Date.valueOf(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
						}catch(IllegalArgumentException e) {
							throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
						}
					}
				} else {
					bstmnt.setNull(i, Types.DATE);
				}
				break;
			case DatabaseConnectorConstants.STRING:
				if (!value.isNull()) {
					bstmnt.setString(i, value.asText());
				} else {
					bstmnt.setNull(i, Types.VARCHAR);
				}
				break;
			case DatabaseConnectorConstants.JSON:
				if (!value.isNull()) {
					bstmnt.setString(i, StringEscapeUtils.unescapeJava(value.toString()));
				} else {
					bstmnt.setNull(i, Types.NULL);
				}
				break;
			case DatabaseConnectorConstants.TIME:
				if (!value.isNull()) {
					bstmnt.setTime(i, Time.valueOf(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
				} else {
					bstmnt.setNull(i, Types.TIME);
				}
				break;
			case DatabaseConnectorConstants.NVARCHAR:
				if (!value.isNull()
						&& DatabaseConnectorConstants.MSSQL.equals(_sqlConnection.getMetaData().getDatabaseProductName())) {
					bstmnt.setString(i, StringEscapeUtils.unescapeJava(value.toString()));
				} else {
					bstmnt.setNull(i, Types.NVARCHAR);
				}
				break;
			case DatabaseConnectorConstants.BOOLEAN:
				if (!value.isNull()) {
					boolean flag = Boolean.parseBoolean(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					bstmnt.setBoolean(i, flag);
				} else {
					bstmnt.setNull(i, Types.BOOLEAN);
				}
				break;
			case DatabaseConnectorConstants.LONG:
				if (!value.isNull()) {
					long longValue = Long.parseLong(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					bstmnt.setLong(i, longValue);
				} else {
					bstmnt.setNull(i, Types.BIGINT);
				}
				break;
			case DatabaseConnectorConstants.FLOAT:
				if (!value.isNull()) {
					float floatValue = Float.parseFloat(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					bstmnt.setFloat(i, floatValue);
				} else {
					bstmnt.setNull(i, Types.FLOAT);
				}
				break;
			case DatabaseConnectorConstants.DOUBLE:
				if (!value.isNull()) {
					double doubleValue = Double.parseDouble(value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					bstmnt.setDouble(i, doubleValue);
				} else {
					bstmnt.setNull(i, Types.DECIMAL);
				}
				break;
			case DatabaseConnectorConstants.BLOB:
				if (!value.isNull()) {
					String blobdata = value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
					try(InputStream stream = new ByteArrayInputStream(blobdata.getBytes());){
						bstmnt.setBlob(i, stream);
					}
				} else {
					bstmnt.setNull(i, Types.BLOB);
				}

				break;
			case DatabaseConnectorConstants.TIMESTAMP:
				if (!value.isNull()) {
					String timeStamp = value.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
					bstmnt.setTimestamp(i, Timestamp.valueOf(timeStamp));
				} else {
					bstmnt.setNull(i, Types.TIMESTAMP);
				}
				break;
			default:
				break;
		}
	}

	/**
	 * This method will append the values to parameters of the Insert Prepared
	 * Statement.
	 *
	 * @param bstmnt       the query
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @return the int
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private int buildInsertValues(PreparedStatement bstmnt, ObjectData objdata, Map<String, String> dataTypes)
			throws IOException, SQLException {
		JsonNode json = null;
		int i = 0;
		try (InputStream is = objdata.getData()) {
			// After filtering out the inputs (which are more than 1MB) we are loading the
			// inputstream to memory here.
			json = _reader.readTree(is);
			if (json != null) {
					for (String key : _columnNames) {
						JsonNode fieldName = json.get(key);
						if (fieldName != null) {
							i++;
							this.checkDataType(bstmnt, key, fieldName, dataTypes, i);
						}
					}
			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}

		}
		return i;

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
		} catch (SQLException e) {
			CustomResponseUtil.logFailedBatch(response, remainingBatch, b);
			CustomResponseUtil.writeSqlErrorResponse(e, data, response);
		} finally {
			IOUtil.closeQuietly(payload);
		}

	}

}