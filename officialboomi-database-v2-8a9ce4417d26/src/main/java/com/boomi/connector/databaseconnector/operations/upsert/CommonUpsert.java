// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.upsert;

import oracle.jdbc.OracleType;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
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
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class CommonUpsert.
 *
 * @author swastik.vn
 */
public class CommonUpsert {


	/** The Constant logger. */
	private static final Logger LOG = Logger.getLogger(CommonUpsert.class.getName());

	/** The con. */
	private final Connection _sqlConnection;

	/** The batch count. */
	private final Long _batchCount;

	/** The table name. */
	private String _tableName;

	/** The commit option. */
	private final String _commitOption;
	
	/** The schema name. */
	private final String _schemaName;

	/** The table column name set. */
	private Set<String> _columnNames;

	/** The reader. */
	private ObjectReader _reader = DBv2JsonUtil.getObjectReader();

	/** The join transaction flag **/
	private final boolean _joinTransaction;

	/**
	 * Instantiates a new common upsert.
	 *
	 * @param sqlConnection   the sqlConnection
	 * @param batchCount      the batch count
	 * @param string          the string
	 * @param commitOption    the commit option
	 * @param schemaName      the schema name
	 * @param joinTransaction
	 * @param columnNames   the column name
	 */
	public CommonUpsert(Connection sqlConnection, Long batchCount, String string, String commitOption, String schemaName,
			boolean joinTransaction, Set<String> columnNames) {
		_sqlConnection = sqlConnection;
		_batchCount = batchCount;
		_commitOption = commitOption;
		_tableName = string;
		_schemaName = schemaName;
		_joinTransaction = joinTransaction;
		_columnNames = columnNames != null ? columnNames : new LinkedHashSet<>();
	}

	/**
	 * This method is the entry point for the Upsert Logic. This method will take
	 * the UpdateRequest and builds the SQL Statements and Executes them based on
	 * the Commit Options.
	 *
	 * @param trackedData     the tracked data
	 * @param response        the response
	 * @param readTimeout     the read timeout
	 * @param schemaName      the schema name
	 * @param payloadMetadata
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	public void executeStatements(UpdateRequest trackedData, OperationResponse response, int readTimeout,
			String schemaName, PayloadMetadata payloadMetadata) throws SQLException, IOException, NumberFormatException {

		// This Map will be getting the data type of the each column associated with the
		// table.
		Map<String, String> dataTypes = new MetadataExtractor(_sqlConnection, _tableName, schemaName).getDataType();
		List<String> primaryKeys = getPrimaryKeys();
		Map<String, List<String>> uniqueKeys = getUniqueKeys(primaryKeys);
		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching and for creating the Query for Prepared Statement.
		List<ObjectData> batchData = new ArrayList<>();
		for (ObjectData objdata : trackedData) {
			batchData.add(objdata);
		}
		StringBuilder query = this.buildPreparedStatement(batchData, dataTypes, primaryKeys, uniqueKeys);
		if ((_batchCount != null) && (_batchCount > 0) && DatabaseConnectorConstants.COMMIT_BY_ROWS.equals(_commitOption)) {
			this.doBatch(dataTypes, response, batchData, query, readTimeout, primaryKeys, uniqueKeys);
		} else if ((_batchCount == null) || (_batchCount == 0) || DatabaseConnectorConstants.COMMIT_BY_PROFILE.equals(
				_commitOption)) {
			commitByProfile(response, readTimeout, dataTypes, primaryKeys, uniqueKeys, batchData,
					payloadMetadata, _joinTransaction);
		} else if (_batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	/**
	 * Commits the transaction by profile.
	 * @param response
	 * @param readTimeout
	 * @param dataTypes
	 * @param primaryKeys
	 * @param uniqueKeys
	 * @param batchData
	 * @param payloadMetadata
	 * @param joinTransaction
	 * @throws SQLException
	 */
	private void commitByProfile(OperationResponse response, int readTimeout, Map<String, String> dataTypes,
			List<String> primaryKeys, Map<String, List<String>> uniqueKeys, List<ObjectData> batchData,
			PayloadMetadata payloadMetadata, boolean joinTransaction) throws SQLException {
		for (ObjectData objdata : batchData) {
			// This List will be holding the names of the columns which will satisfy the
			// Primary Key and Unique Key constraints if any.
			try {
				List<String> primaryKeyConflict = this.checkForVoilation(objdata, primaryKeys, dataTypes);
				List<String> uniqueKeyConflict =  new ArrayList<>();
				for (Map.Entry<String,List<String>> entry : uniqueKeys.entrySet()) {
					uniqueKeyConflict.addAll(this.checkForVoilation(objdata, entry.getValue(), dataTypes));
				}
				String queryNonBatch = this.buildStatements(objdata, primaryKeyConflict, uniqueKeyConflict).toString();
				try (PreparedStatement pstmnt = _sqlConnection.prepareStatement(queryNonBatch)) {
					pstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
					this.appendParams(objdata, dataTypes, pstmnt, primaryKeyConflict, uniqueKeyConflict);
					int effectedRowCount = pstmnt.executeUpdate();
					QueryResponse queryResponse = new QueryResponse(queryNonBatch, effectedRowCount,
							"Executed Successfully");
					CustomResponseUtil.handleSuccess(objdata, response, payloadMetadata, queryResponse);

					if (joinTransaction) {
						continue;
					}
					_sqlConnection.commit();
				} catch (SQLException e) {
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (IOException | IllegalArgumentException e) {
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, objdata, e);
				}
			}catch (IOException | IllegalArgumentException e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			}
		}
	}

	/**
	 * This method will build the SQL Statements based on the conflict. If conflict
	 * is present it will build Insert statement orelse Update
	 *
	 * @param objdata   the objdata
	 * @param primaryKeyConflict the primary keys
	 * @param uniqueKeyConflict the unique keys
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private StringBuilder buildStatements(ObjectData objdata, List<String> primaryKeyConflict,
			List<String> uniqueKeyConflict) throws SQLException, IOException {
		StringBuilder query = new StringBuilder();
		if (primaryKeyConflict.isEmpty() && uniqueKeyConflict.isEmpty()) {
			if(DatabaseConnectorConstants.ORACLE.equals(_sqlConnection.getMetaData().getDatabaseProductName())
					&& DatabaseConnectorConstants.DOUBLE_QUOTE.startsWith(_tableName)) {
				_tableName = _tableName.substring(1, _tableName.length()-1);
			}
			query = QueryBuilderUtil.buildInitialQuery(_sqlConnection, _tableName, _schemaName, _columnNames);
			this.buildInsertQuery(query, objdata);

		} else {
			this.buildUpdateSyntax(query, objdata, primaryKeyConflict, uniqueKeyConflict);

		}
		return query;
	}

	/**
	 * Does the JDBC Statement batching if the batch count is greater than zero and
	 * commit option is commit by rows. This method will take the input request and
	 * Builds the SQL Statements and does the batching.
	 *
	 * @param dataTypes the data types
	 * @param response  the response
	 * @param batchData the batch data
	 * @param readTimeout the read timeout
	 * @param query     the query
	 * @param primaryKeys the primary keys
	 * @param uniqueKeys the unique keys
	 * @throws SQLException the SQL exception
	 */
	private void doBatch(Map<String, String> dataTypes, OperationResponse response, List<ObjectData> batchData,
			StringBuilder query, int readTimeout, List<String> primaryKeys, Map<String,
			List<String>> uniqueKeys) throws NumberFormatException {

		int batchnum = 0;
		int currentBatchSize = 0;
		int currentDocIndex = 0;
		boolean shouldExecute = true;

		try (PreparedStatement bstmnt = _sqlConnection.prepareStatement(query.toString())) {
			bstmnt.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));
			for (ObjectData objdata : batchData) {
				currentBatchSize++;
				currentDocIndex++;
				Payload payload = null;
				try {
					List<String> uniqueKeyConflict =  new ArrayList<>();
					for (Map.Entry<String,List<String>> entry : uniqueKeys.entrySet()) {
						uniqueKeyConflict.addAll(checkForVoilation(objdata, entry.getValue(), dataTypes));
					}
					appendParams(objdata, dataTypes, bstmnt, checkForVoilation(objdata, primaryKeys, dataTypes),
							uniqueKeyConflict);
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

		} catch (SQLException e) {
			throw new ConnectorException(e);
		}

	}

	/**
	 * This method will check if any violation exists in the table and decides
	 * whether to form insert statement or update.
	 *
	 * @param objdata  			 the objdata
	 * @param dataTypes 		 the data types
	 * @param bstmnt  			 the bstmnt
	 * @param primaryKeyConflict the primary keys
	 * @param uniqueKeyConflict the unique keys
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void appendParams(ObjectData objdata, Map<String, String> dataTypes, PreparedStatement bstmnt,
			List<String> primaryKeyConflict, List<String> uniqueKeyConflict) throws SQLException, IOException {
		
		if (primaryKeyConflict.isEmpty() && uniqueKeyConflict.isEmpty()) {
			this.appendInsertParams(bstmnt, objdata, dataTypes);
		} else {
			this.appendUpdateParams(bstmnt, objdata, dataTypes, primaryKeyConflict, uniqueKeyConflict);
		}

	}

	/**
	 * This method will append the parameters for the Update Query formed in the
	 * Prepared Statement.
	 *
	 * @param bstmnt       the bstmnt
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @param primaryKeyConflict     the primary keys conflict
	 * @param uniqueKeyConflict  the unique keys conflict
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void appendUpdateParams(PreparedStatement bstmnt, ObjectData objdata, Map<String, String> dataTypes,
			List<String> primaryKeyConflict, List<String> uniqueKeyConflict) throws IOException, SQLException {

		int i = 0;
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = _reader.readTree(is);
			if (json != null) {
				String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
				if (DatabaseConnectorConstants.ORACLE.equals(databaseName) && _tableName.contains("/")) {
					_tableName = _tableName.replace("/", "//");
				}
				if ((DatabaseConnectorConstants.ORACLE.equals(databaseName))
						&& DatabaseConnectorConstants.DOUBLE_QUOTE.startsWith(_tableName)) {
						_tableName = _tableName.substring(1, _tableName.length()-1);
					}
				for (String key : _columnNames) {
						if (!primaryKeyConflict.contains(key) && !uniqueKeyConflict.contains(key)) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								i++;
								this.checkDatatype(bstmnt, dataTypes, key, fieldName, i);
							}
						}

				}

			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}
		}
		for (int j = 0; j <= primaryKeyConflict.size() - 1; j++) {

			String key = primaryKeyConflict.get(j);

			JsonNode jsonNode = null;
			try (InputStream is = objdata.getData()) {
				jsonNode = _reader.readTree(is);
				if (jsonNode != null) {
					JsonNode fieldName = jsonNode.get(key);
					if (fieldName != null) {
						i++;
						this.checkDatatype(bstmnt, dataTypes, key, fieldName, i);
					}
				}

			}

		}
		
		for (int j = 0; j <= uniqueKeyConflict.size() - 1; j++) {

			String key = uniqueKeyConflict.get(j);

			JsonNode jsonNode = null;
			try (InputStream is = objdata.getData()) {
				jsonNode = _reader.readTree(is);
				if (jsonNode != null) {
					JsonNode fieldName = jsonNode.get(key);
					if (fieldName != null) {
						i++;
						this.checkDatatype(bstmnt, dataTypes, key, fieldName, i);
					}
				}

			}

		}
	}

	/**
	 * This method will append the values to the Insert Query formed for the
	 * prepared Statements.
	 *
	 * @param bstmnt       the bstmnt
	 * @param objdata      the objdata
	 * @param dataTypes    the data types
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void appendInsertParams(PreparedStatement bstmnt, ObjectData objdata, Map<String, String> dataTypes)
			throws SQLException, IOException {
		int i = 0;
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = _reader.readTree(is);
			if (json != null) {
				String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
				if(DatabaseConnectorConstants.ORACLE.equals(databaseName) && _tableName.contains("/")) {
					_tableName = _tableName.replace("/", "//");
				}
				 if(DatabaseConnectorConstants.ORACLE.equals(databaseName)
						 && _tableName.startsWith(DatabaseConnectorConstants.DOUBLE_QUOTE)) {
					_tableName = _tableName.substring(1, _tableName.length()-1);
				}
				for (String key : _columnNames) {
						JsonNode fieldName = json.get(key);
						i++;
						this.checkDatatype(bstmnt, dataTypes, key, fieldName, i);

					}

			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}

		}

	}

	/**
	 * This method will check the Datatype of the key and append the values to the
	 * prepared statement accordingly.
	 *
	 * @param bstmnt    the bstmnt
	 * @param dataTypes the data types
	 * @param key       the key
	 * @param fieldName the field name
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private void checkDatatype(PreparedStatement bstmnt, Map<String, String> dataTypes, String key, JsonNode fieldName,
			int i) throws SQLException, IOException {
		String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
		boolean hasNonNullField = (fieldName != null && !fieldName.isNull());
		switch (dataTypes.get(key)) {
		case DatabaseConnectorConstants.INTEGER:
			if (hasNonNullField) {
				BigDecimal num = new BigDecimal(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				bstmnt.setBigDecimal(i, num);
			} else {
				bstmnt.setNull(i, Types.INTEGER);
			}
			break;
		case DatabaseConnectorConstants.DATE:
			if (hasNonNullField) {
				if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
					bstmnt.setString(i, fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				} else {
					try {
						bstmnt.setDate(i, Date.valueOf(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
					}catch(IllegalArgumentException e) {
						throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
					}
				}
			} else {
				bstmnt.setNull(i, Types.DATE);
			}
			break;
		case DatabaseConnectorConstants.STRING:
			if (hasNonNullField) {
				bstmnt.setString(i, fieldName.asText());
			} else {
				bstmnt.setNull(i, Types.VARCHAR);
			}
			break;
		case DatabaseConnectorConstants.JSON:
			if (hasNonNullField) {
				processJson(databaseName, bstmnt, fieldName, i);
			} else{
				bstmnt.setNull(i, Types.NULL);
			}
			break;
		case DatabaseConnectorConstants.NVARCHAR:
			if (hasNonNullField) {
				bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldName.toString()
						.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
			} else {
				bstmnt.setNull(i, Types.NVARCHAR);
			}
			break;
		case DatabaseConnectorConstants.TIME:
			if (hasNonNullField) {
				bstmnt.setTime(i, Time.valueOf(fieldName.toString()
						.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
			} else {
				bstmnt.setNull(i, Types.TIME);
			}
			break;
		case DatabaseConnectorConstants.BOOLEAN:
			if (hasNonNullField) {
				boolean flag = Boolean.parseBoolean(fieldName.toString()
						.replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				bstmnt.setBoolean(i, flag);
			} else {
				bstmnt.setNull(i, Types.BOOLEAN);
			}
			break;
		case DatabaseConnectorConstants.LONG:
			if (hasNonNullField) {
				long value = Long.parseLong(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				bstmnt.setLong(i, value);
			} else {
				bstmnt.setNull(i, Types.BIGINT);
			}
			break;
		case DatabaseConnectorConstants.FLOAT:
			if (hasNonNullField) {
				float value = Float.parseFloat(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				bstmnt.setFloat(i, value);
			} else {
				bstmnt.setNull(i, Types.FLOAT);
			}
			break;
		case DatabaseConnectorConstants.DOUBLE:
			if (hasNonNullField) {
				double value = Double.parseDouble(fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
				bstmnt.setDouble(i, value);
			} else {
				bstmnt.setNull(i, Types.DECIMAL);
			}
			break;
		case DatabaseConnectorConstants.BLOB:
			if (hasNonNullField) {
				String value = fieldName.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
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
			if (hasNonNullField) {
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

	/**
	 * Processes the Json field and sets the value to prepared statement based on the database name.
	 *
	 * @param databaseName the database name
	 * @param bstmnt the bstmnt
	 * @param fieldName the field name
	 * @param i the i
	 * @throws SQLException the SQL exception
	 */
	private static void processJson(String databaseName, PreparedStatement bstmnt, JsonNode fieldName, int i)
			throws SQLException {
		if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
			OracleJsonFactory factory = new OracleJsonFactory();
		    OracleJsonObject object = factory.createObject();
		    JSONObject jsonObject = new JSONObject(fieldName.toString());
			Iterator<String> keys = jsonObject.keys();
			while(keys.hasNext()) {
				String jsonKeys = keys.next();
				object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
			}
			bstmnt.setObject(i, object, OracleType.JSON);
		} else {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldName.toString()));
		}
	}

	/**
	 * Builds the prepared statement by taking the 1st request of the tracked data.
	 * if 1st request is not proper or if it throws any exception it will move to
	 * subsequent requests until the query is formed.
	 *
	 * @param batchData the batch data
	 * @param dataTypes the data types
	 * @param primaryKeys the primary keys
	 * @param uniqueKeys the unique keys
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 */
	private StringBuilder buildPreparedStatement(List<ObjectData> batchData, Map<String, String> dataTypes,
			List<String> primaryKeys, Map<String, List<String>> uniqueKeys) throws SQLException {
		StringBuilder query = new StringBuilder();
		for (ObjectData objdata : batchData) {
			try {
				List<String> uniqueKeyConflict =  new ArrayList<>();
				for (Map.Entry<String,List<String>> entry : uniqueKeys.entrySet()) {
					uniqueKeyConflict.addAll(this.checkForVoilation(objdata, entry.getValue(), dataTypes));
				}
				query = this.buildStatements(objdata, this.checkForVoilation(objdata, primaryKeys, dataTypes), uniqueKeyConflict);
			} catch (IOException | IllegalArgumentException e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
			}
			if (query.length() != 0) {
				break;
			}
		}
		return query;
	}

	/**
	 * This method will build the Update Syntax required for prepared statements.
	 *
	 * @param query        the query
	 * @param objdata      the objdata
	 * @param primaryKeyconflict     the primary keys conflict
	 * @param uniqueKeyConflict  the unique keys conflict
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void buildUpdateSyntax(StringBuilder query, ObjectData objdata, List<String> primaryKeyconflict,
			List<String> uniqueKeyConflict) throws SQLException, IOException {
		
		JsonNode json = null;
		boolean parameterSet = false;
		if(_schemaName != null
				&& DatabaseConnectorConstants.MSSQL.equalsIgnoreCase(_sqlConnection.getMetaData().getDatabaseProductName())
				&& !DatabaseConnectorConstants.MSSQL_DEFAULT_SCHEMA.equalsIgnoreCase(_schemaName)) {
			query.append("UPDATE ").append(_schemaName).append(".").append(_tableName).append(" SET ");
		}else {
			query.append("UPDATE ").append(_tableName).append(" SET ");
		}
		
		try (InputStream is = objdata.getData()) {
			json = _reader.readTree(is);
			if (json != null) {
				DatabaseMetaData md = _sqlConnection.getMetaData();
				if(DatabaseConnectorConstants.ORACLE.equals(md.getDatabaseProductName()) && _tableName.contains("/")) {
					_tableName = _tableName.replace("/", "//");
				}
				if ((DatabaseConnectorConstants.ORACLE.equals(md.getDatabaseProductName()))
						&& DatabaseConnectorConstants.DOUBLE_QUOTE.startsWith(_tableName)) {
						_tableName = _tableName.substring(1, _tableName.length()-1);
					}
				for (String key : _columnNames) {
						if (!primaryKeyconflict.contains(key) && !uniqueKeyConflict.contains(key)) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								query.append(key).append("=");
								query.append(DatabaseConnectorConstants.PARAM);
							}
						}
				}
			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}
			query.deleteCharAt(query.length() - 1);
		}
		query.append(DatabaseConnectorConstants.WHERE);
		for (int i = 0; i <= primaryKeyconflict.size() - 1; i++) {
			if (i > 0) {
				query.append(DatabaseConnectorConstants.AND).append(" ");
			}
			String key = primaryKeyconflict.get(i);
			query.append(key).append(" = ");
			query.append("?");
			parameterSet = true;

		}
		for (int i = 0; i <= uniqueKeyConflict.size() - 1; i++) {
			String key = uniqueKeyConflict.get(i);
			JsonNode fieldName = json.get(key);
			if(parameterSet) {
				query.append(DatabaseConnectorConstants.AND).append(" ");
				parameterSet = false;
			}
			if (fieldName != null) {
				if (i > 0 && parameterSet) {
					query.append(DatabaseConnectorConstants.AND).append(" ");
				}
				query.append(key).append(" = ");
				query.append("?");
				parameterSet = true;
			}else if(DatabaseConnectorConstants.MSSQL.equals(_sqlConnection.getMetaData().getDatabaseProductName())){
				if (i > 0) {
					query.append(DatabaseConnectorConstants.AND).append(" ");
				}
				query.append(key).append(" IS NULL ");
			}

		}

	}

	/**
	 * This method will take the current request and check for any Primary key and
	 * Unique Key Violation.
	 *
	 * @param objdata     the objdata
	 * @param primaryKeys the primary keys
	 * @param dataTypes   the data types
	 * @return the list
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	public List<String> checkForVoilation(ObjectData objdata, List<String> primaryKeys, Map<String, String> dataTypes)
			throws SQLException, IOException{
		List<String> conflict = new ArrayList<>();
		JsonNode json = null;
		boolean canExecute = false;
		try (InputStream is = objdata.getData();) {
			json = _reader.readTree(is);
			if (json != null) {
				StringBuilder query = new StringBuilder();
				for (int i = 0; i <= primaryKeys.size() - 1; i++) {
					if(i==0) {
						if((_schemaName != null) && DatabaseConnectorConstants.MSSQL.equalsIgnoreCase(
                                _sqlConnection.getMetaData().getDatabaseProductName())
                                && !DatabaseConnectorConstants.MSSQL_DEFAULT_SCHEMA.equalsIgnoreCase(_schemaName)) {
							query.append("Select ").append(primaryKeys.get(i)).append(" from ")
							.append(_schemaName).append(".").append(_tableName).append(DatabaseConnectorConstants.WHERE);
						}else {
							query.append("Select ").append(primaryKeys.get(i)).append(" from "
							).append(_tableName).append(DatabaseConnectorConstants.WHERE);
						}
					}
					String key = primaryKeys.get(i);
					JsonNode fieldName = json.get(key);
					
					if(fieldName!= null) {
						canExecute = true;
						query.append(primaryKeys.get(i) + " = ?");
						query.append(DatabaseConnectorConstants.AND).append(" ");
					}else if(DatabaseConnectorConstants.MSSQL.equals(_sqlConnection.getMetaData().getDatabaseProductName())){
						canExecute = true;
						query.append(primaryKeys.get(i) + " IS NULL");
						query.append(DatabaseConnectorConstants.AND).append(" ");
					}
				}
				if(query.length()>0) {
					query = query.delete(query.length()-5, query.length());
					try(PreparedStatement stmt = _sqlConnection.prepareStatement(query.toString())){
						int j=1;
						for (int i = 0; i <= primaryKeys.size() - 1; i++) {
							String key = primaryKeys.get(i);
							JsonNode fieldName = json.get(key);
							if(fieldName != null) {
								this.checkDatatype(stmt, dataTypes, key, fieldName, j);
								j++;
							}
						}
						
						if(canExecute) {
							try (ResultSet set = stmt.executeQuery()) {
								if (set.next()) {
									conflict = primaryKeys;
								}
							}
						}
					}
				}
			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}

		}
		catch (IOException | IllegalArgumentException e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
		}
		return conflict;

	}

	/**
	 * This method will build the Insert query based on the Input Request.
	 *
	 * @param query        the query
	 * @param objdata      the objdata
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void buildInsertQuery(StringBuilder query, ObjectData objdata)
			throws SQLException, IOException {
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = _reader.readTree(is);
			if (json != null) {
				DatabaseMetaData md = _sqlConnection.getMetaData();
				if(DatabaseConnectorConstants.ORACLE.equals(md.getDatabaseProductName()) && _tableName.contains("/")) {
					_tableName = _tableName.replace("/", "//");
				}
				if ((DatabaseConnectorConstants.ORACLE.equals(md.getDatabaseProductName()))
						&& DatabaseConnectorConstants.DOUBLE_QUOTE.startsWith(_tableName)) {
						_tableName = _tableName.substring(1, _tableName.length()-1);
					}
				_columnNames.forEach(column -> query.append(DatabaseConnectorConstants.PARAM));

			} else {
				throw new ConnectorException(DatabaseConnectorConstants.INPUT_ERROR);
			}
			query.deleteCharAt(query.length() - 1);
			query.append(")");
		}

	}

	/**
	 * This method will return the primary keys and the Unique Keys in the table.
	 *
	 * @return the primary keys
	 * @throws SQLException the SQL exception
	 */
	private List<String> getPrimaryKeys() throws SQLException {
		if(DatabaseConnectorConstants.ORACLE.equals(_sqlConnection.getMetaData().getDatabaseProductName())) {
			_tableName = QueryBuilderUtil.checkTableName(_tableName, _sqlConnection.getMetaData().getDatabaseProductName(),
					_schemaName);
			if(_tableName.contains("//")) {
				_tableName = _tableName.replace("//", "/");
			}
		}
		List<String> pk = new ArrayList<>();
		try (ResultSet resultSet = _sqlConnection.getMetaData()
				.getPrimaryKeys(_sqlConnection.getCatalog(), _schemaName, _tableName)) {
			while (resultSet.next()) {
				if (resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME) != null
						&& !pk.contains(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME))) {
					pk.add(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME));
				}
			}
		}
		return pk;

	}

	/**
	 * This method will execute the remaining statements of the batch.
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
	 * This method will return the primary keys and the Unique Keys in the table.
	 *
	 * @return the primary keys
	 * @throws SQLException the SQL exception
	 */
	private Map<String, List<String>> getUniqueKeys(List<String> pk) throws SQLException {
		if(DatabaseConnectorConstants.ORACLE.equals(_sqlConnection.getMetaData().getDatabaseProductName())) {
			_tableName = QueryBuilderUtil.checkTableName(_tableName, _sqlConnection.getMetaData().getDatabaseProductName(),
					_schemaName);
			if(_tableName.contains("//")) {
				_tableName = _tableName.replace("//", "/");
			}
		}
		Map<String, List<String>> indexName = new HashMap<>();
		
		try (ResultSet resultSet = _sqlConnection.getMetaData()
				.getIndexInfo(_sqlConnection.getCatalog(), _schemaName, _tableName, true, false)) {
			while (resultSet.next()) {
				if ((null != resultSet.getString(DatabaseConnectorConstants.NON_UNIQUE)) && (resultSet.getString(
                        DatabaseConnectorConstants.NON_UNIQUE).equals("0") || resultSet.getString(
                        DatabaseConnectorConstants.NON_UNIQUE).equals("f")) && (resultSet.getString(
                        DatabaseConnectorConstants.COLUMN_NAME) != null) && !pk.contains(
                        resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME))) {
					if(indexName.get(resultSet.getString(DatabaseConnectorConstants.INDEX_NAME)) != null) {
						List<String> ukindex = indexName.get(resultSet.getString(DatabaseConnectorConstants.INDEX_NAME));
						ukindex.add(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME));
						indexName.replace(resultSet.getString(DatabaseConnectorConstants.INDEX_NAME), ukindex);
					}else {
						List<String> ukindex = new ArrayList<>();
						ukindex.add(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME));
						indexName.putIfAbsent(resultSet.getString(DatabaseConnectorConstants.INDEX_NAME), ukindex);
					}
				}
			}
		}
		return indexName;

	}
}
