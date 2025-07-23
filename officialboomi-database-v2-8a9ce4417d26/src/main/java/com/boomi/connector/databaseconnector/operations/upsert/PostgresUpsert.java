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

import org.postgresql.util.PGobject;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class PostgresUpsert.
 *
 * @author swastik.vn
 */
public class PostgresUpsert {

	/** The con. */
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
	private static final Logger LOG = Logger.getLogger(PostgresUpsert.class.getName());

	/**
	 * Instantiates a new postgres upsert.
	 *
	 * @param sqlConnection          the sqlConnection
	 * @param batchCount   the batch count
	 * @param objectTypeId the object type id
	 * @param commitOption the commit option
	 * @param columnNames   the column name
	 */
	public PostgresUpsert(Connection sqlConnection, Long batchCount, String objectTypeId,
			String commitOption, Set<String> columnNames) {
		_sqlConnection = sqlConnection;
		_batchCount = batchCount;
		_tableName = objectTypeId;
		_commitOption = commitOption;
		_columnNames = columnNames != null ? columnNames : new LinkedHashSet<>();
	}

	/**
	 * Builds the statements.
	 *
	 * @param trackedData     the tracked data
	 * @param response        the response
	 * @param readTimeout     the read timeout
	 * @param schemaName      the schema name
	 * @param payloadMetadata
	 * @throws SQLException the SQL exception
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
		StringBuilder query = QueryBuilderUtil.buildInitialQuery(_sqlConnection, _tableName, schemaName,_columnNames);
		this.buildInsertQueryStatement(query, batchData);
		boolean primaryKeys = this.buildOnConflict(query);
		if (primaryKeys) {
			this.buildUpdateStatement(query, batchData, response);
		}
		if ((_batchCount != null) && (_batchCount > 0) && DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(_commitOption)) {
			this.doBatch(batchData, response, dataTypes, query, primaryKeys, readTimeout);
		} else if ((_batchCount == null) || (_batchCount == 0)
				|| DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(_commitOption)) {
			commitByProfile(response, readTimeout, dataTypes, batchData, query, primaryKeys, payloadMetadata);
		} else if (_batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	/**
	 * Commits by profile.
	 * @param response
	 * @param readTimeout
	 * @param dataTypes
	 * @param batchData
	 * @param query
	 * @param primaryKeys
	 * @param payloadMetadata
	 */
	private void commitByProfile(OperationResponse response, int readTimeout, Map<String, String> dataTypes,
			List<ObjectData> batchData, StringBuilder query, boolean primaryKeys, PayloadMetadata payloadMetadata) {
		for (ObjectData objdata : batchData) {
			try (PreparedStatement pstmnt = _sqlConnection.prepareStatement(query.toString())) {
				pstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
				int pos = this.appendInsertPreapreStatement(pstmnt, objdata, dataTypes);
				if (primaryKeys) {
					this.appendUpdateStatement(pstmnt, objdata, dataTypes, response, pos);
				}
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

	/**
	 * Does the JDBC Statement batching if the batch count is greater than zero and
	 * commit option is commit by rows. This method will take the input request and
	 * Builds the SQL Statements and does the batching.
	 *
	 * @param batchData   the tracked data
	 * @param response    the response
	 * @param dataTypes   the data types
	 * @param query       the query
	 * @param primaryKeys the primary keys
	 * @throws SQLException the SQL exception
	 */
	private void doBatch(List<ObjectData> batchData, OperationResponse response, Map<String, String> dataTypes,
			StringBuilder query, boolean primaryKeys, int readTimeout) throws SQLException {

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
					int pos = this.appendInsertPreapreStatement(bstmnt, objdata, dataTypes);
					if (primaryKeys) {
						this.appendUpdateStatement(bstmnt, objdata, dataTypes, response, pos);
					}
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
							bstmnt.clearParameters();
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
							ResponseUtil.addSuccess(response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
						}

					}

				} catch (BatchUpdateException batchUpdateException) {
					_sqlConnection.commit();
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
						batchnum++;
						CustomResponseUtil.logFailedBatch(response, batchnum, currentBatchSize);
						currentBatchSize = 0;
					}
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, objdata, e);
				}

				finally {
					IOUtil.closeQuietly(payload);
				}
			}

		} catch (Exception e) {
			throw new ConnectorException(e);
		}

	}

	/**
	 * This method will build the Update Prepared Statement followed by ON CONFLICT
	 * keyword.
	 *
	 * @param query        the query
	 * @param batchData    the batch data
	 * @param response     the response
	 * @throws SQLException the SQL exception
	 */
	private void buildUpdateStatement(StringBuilder query, List<ObjectData> batchData, OperationResponse response) {
		boolean dataConsistent = false;
		JsonNode json = null;
		query.append(" DO UPDATE SET ");
		for (ObjectData objdata : batchData) {
			try (InputStream is = objdata.getData()) {
				json = _reader.readTree(is);
				if (json != null) {
					for (String key : _columnNames) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								dataConsistent = true;
								query.append(key).append(" = ?,");
							}
					}
				} else {
					throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
				}
				query.deleteCharAt(query.length() - 1);
			} catch (IOException e) {
				// moving to next request
				LOG.log(Level.SEVERE, e.getMessage(), e);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, objdata, e);
			}
			if (dataConsistent) {
				break;
			}

		}

	}

	/**
	 * Builds the insert query required for Prepared Statement by taking the values
	 * from the 1st request. If the 1st request is improper then the loop will
	 * continue until it gets the correct request to form the insert Query.
	 *
	 * @param query        the query
	 * @param batchData    the batch data
	 * @throws SQLException the SQL exception
	 */
	private void buildInsertQueryStatement(StringBuilder query, List<ObjectData> batchData) {
		JsonNode json = null;
		for (ObjectData objdata : batchData) {
			boolean dataConsistent = false;
			try (InputStream is = objdata.getData()) {
				json = _reader.readTree(is);
				if (json != null) {
					for (String key : _columnNames) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								dataConsistent = true;
								query.append(DatabaseConnectorConstants.PARAM);
							}
						}
						query.deleteCharAt(query.length() - 1);
						query.append(")");

				} else {
					throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
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
	 * Appends the Parameters to the Update Statements followed by On Conflict
	 * Keyword. Position for appending the parameter will be continued from the
	 * Insert Statement.
	 *
	 * @param pstmnt       the pstmnt
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @param response     the response
	 * @param i            the i
	 * @throws SQLException the SQL exception
	 */
	private void appendUpdateStatement(PreparedStatement pstmnt, ObjectData objdata, Map<String, String> dataTypes,
			OperationResponse response, int i) throws SQLException {

		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = _reader.readTree(is);
			if (json != null) {
				for (String key : _columnNames) {
						JsonNode fieldName = json.get(key);
						if (fieldName != null) {
							i++;
							this.checkDataType(pstmnt, key, fieldName, dataTypes, i);
						}
					}

			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}
		} catch (IOException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		} catch (ConnectorException e) {
			ResponseUtil.addExceptionFailure(response, objdata, e);
		}

	}

	/**
	 * Builds the on conflict Keyword if there is any conflicting primary keys in
	 * the table.
	 *
	 * @param query the query
	 * @return true, if successful
	 * @throws SQLException
	 */
	private boolean buildOnConflict(StringBuilder query) throws SQLException {
		boolean hasPK = false;
		if(_tableName.contains("\\\\")) {
			_tableName = _tableName.replace("\\\\", "\\");
		}
		try (ResultSet pk = _sqlConnection.getMetaData().
				getPrimaryKeys(_sqlConnection.getCatalog(), _sqlConnection.getSchema(), _tableName)) {
			if (pk.isBeforeFirst()) {
				query.append(" ON CONFLICT(");
				hasPK = true;
				while (pk.next()) {
					query.append(pk.getString(DatabaseConnectorConstants.COLUMN_NAME));
					query.append(DatabaseConnectorConstants.COMMA);
				}
				query.deleteCharAt(query.length() - 1);
				query.append(")");
			}
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
		return hasPK;

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
	 * Appends the Values to the insert prepared statement based on the Datatypes.
	 *
	 * @param stmnt        the stmnt
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @return the int
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private int appendInsertPreapreStatement(PreparedStatement stmnt, ObjectData objdata, Map<String,
			String> dataTypes) throws IOException, SQLException {
		JsonNode json = null;
		int i = 0;
		try (InputStream is = objdata.getData()) {
			json = _reader.readTree(is);
			if (json != null) {
				for (String key : _columnNames) {
						JsonNode fieldName = json.get(key);
						if (fieldName != null) {
							i++;
							this.checkDataType(stmnt, key, fieldName, dataTypes, i);
						}
					}
			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}

		}
		return i;

	}

	/**
	 * This method will Check for the datatype and append the query with values
	 * based on the datatype.
	 *
	 * @param bstmnt    the bstmnt
	 * @param key       the key
	 * @param fieldName the field name
	 * @param dataTypes the data types
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private void checkDataType(PreparedStatement bstmnt, String key, JsonNode fieldName, Map<String, String> dataTypes,
			int i) throws SQLException, IOException {
		if (dataTypes.containsKey(key)) {
			switch (dataTypes.get(key)) {
			case DatabaseConnectorConstants.INTEGER:
				if (!fieldName.isNull()) {
					BigDecimal num = new BigDecimal(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
					bstmnt.setBigDecimal(i, num);
				} else {
					bstmnt.setNull(i, Types.INTEGER);
				}
				break;
				case DatabaseConnectorConstants.DATE:
					if (!fieldName.isNull()) {
						try {
							bstmnt.setDate(i, Date.valueOf(fieldName.toString()
									.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
						} catch (IllegalArgumentException e) {
							throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR + e);
						}
					} else {
						bstmnt.setNull(i, Types.DATE);
					}
					break;
				case DatabaseConnectorConstants.STRING:
					if (!fieldName.isNull()) {
						String varchar = fieldName.asText();
						bstmnt.setString(i, varchar);
					} else {
						bstmnt.setNull(i, Types.VARCHAR);
					}
					break;
				case DatabaseConnectorConstants.JSON:
					if (!fieldName.isNull()) {
						PGobject jsonObject = new PGobject();
						jsonObject.setType("json");
						jsonObject.setValue(fieldName.toString());
						bstmnt.setObject(i, jsonObject);
					} else {
						bstmnt.setNull(i, Types.NULL);
					}
					break;
				case DatabaseConnectorConstants.TIME:
					if (!fieldName.isNull()) {
						String time = fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
						bstmnt.setTime(i, Time.valueOf(time));
					} else {
						bstmnt.setNull(i, Types.TIME);
					}
					break;
				case DatabaseConnectorConstants.BOOLEAN:
					if (!fieldName.isNull()) {
						boolean flag = Boolean.parseBoolean(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						bstmnt.setBoolean(i, flag);
					} else {
						bstmnt.setNull(i, Types.BOOLEAN);
					}
					break;
				case DatabaseConnectorConstants.LONG:
					if (!fieldName.isNull()) {
						long longValue = Long.parseLong(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						bstmnt.setLong(i, longValue);
					} else {
						bstmnt.setNull(i, Types.BIGINT);
					}
					break;
				case DatabaseConnectorConstants.FLOAT:
					if (!fieldName.isNull()) {
						float floatValue = Float.parseFloat(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						bstmnt.setFloat(i, floatValue);
					} else {
						bstmnt.setNull(i, Types.FLOAT);
					}
					break;
				case DatabaseConnectorConstants.DOUBLE:
					if (!fieldName.isNull()) {
						double doubleValue = Double.parseDouble(fieldName.toString()
								.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
						bstmnt.setDouble(i, doubleValue);
					} else {
						bstmnt.setNull(i, Types.DECIMAL);
					}
					break;
				case DatabaseConnectorConstants.BLOB:
					if (!fieldName.isNull()) {
						String blobdata = fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
						try (InputStream stream = new ByteArrayInputStream(blobdata.getBytes());) {
							if (DatabaseConnectorConstants.POSTGRESQL.equals(_sqlConnection.getMetaData().getDatabaseProductName())) {
								bstmnt.setBinaryStream(i, stream);
							}
						}
					} else {
						bstmnt.setNull(i, Types.BLOB);
					}
					break;
				case DatabaseConnectorConstants.TIMESTAMP:
					if (!fieldName.isNull()) {
						String timeStamp = fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
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
