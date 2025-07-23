// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.upsert;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.text.StringEscapeUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.model.BatchResponse;
import com.boomi.connector.oracledatabase.model.QueryResponse;
import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * The Class DynamicUpsert.
 *
 * @author swastik.vn
 */
public class DynamicUpsert extends SizeLimitedUpdateOperation{


	/**
	 * Instantiates a new Dynamic upsert.
	 *
	 * @param con          the OracleDatabaseConnection
	 */
	public DynamicUpsert(OracleDatabaseConnection conn) {
	super(conn);
	}

	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(DynamicUpsert.class.getName());
	
	
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		OracleDatabaseConnection conn = getConnection();
		PropertyMap map = getContext().getOperationProperties();
		Long batchCount = map.getLongProperty(OracleDatabaseConstants.BATCH_COUNT);
		String commitOption = map
				.getProperty(OracleDatabaseConstants.COMMIT_OPTION);
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			con.setAutoCommit(false);
			this.executeStatements(request, response, con, batchCount, commitOption, readTimeout);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}

	}
	/**
	 * This method is the entry point for the Upsert Logic. This method will take
	 * the UpdateRequest and builds the SQL Statements and Executes them based on
	 * the Commit Options.
	 *
	 * @param trackedData the tracked data
	 * @param response    the response
	 * @param commitOption 
	 * @param batchCount 
	 * @param commitOption 
	 * @param batchCount 
	 * @param readTimeout
	 * @throws SQLException            the SQL exception
	 * @throws JsonProcessingException the json processing exception
	 */
	public void executeStatements(UpdateRequest trackedData, OperationResponse response, Connection con, Long batchCount, String commitOption, int readTimeout)
			throws SQLException,NumberFormatException {
		// This Map will be getting the data type of the each column associated with the
		// table.
		Map<String, String> dataTypes = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();

		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching and for creating the Query for Prepared Statement.
		List<ObjectData> batchData = new ArrayList<>();
		List<String> primaryKey = MetadataUtil.getPrimaryK(con, getContext().getObjectTypeId());
		Map<String, List<String>> uniqueKeys = getUniqueKeys(primaryKey, con);
		for (ObjectData objdata : trackedData) {
			batchData.add(objdata);
		}
		StringBuilder query = this.buildPreparedStatement(batchData, dataTypes, con, primaryKey, uniqueKeys);
		if (batchCount != null && batchCount > 0 && commitOption.equals(COMMIT_BY_ROWS)) {
			ExecutionParameters executionParameters = new ExecutionParameters(con, response, batchData);
			this.doBatch(executionParameters, dataTypes, query, batchCount, primaryKey, readTimeout, uniqueKeys);
		} else if (batchCount == null || batchCount == 0 || commitOption.equals(COMMIT_BY_PROFILE)) {
				Payload payload = null;
					for (ObjectData objdata : batchData) {
						ExecutionParameters executionParametersForCommitByProfile = new ExecutionParameters(con, response, objdata, payload);
						payload = doCommitByProfile(executionParametersForCommitByProfile, readTimeout, dataTypes, primaryKey, uniqueKeys);
					}
					try {
						con.commit();
					} catch (SQLException e) {
						logger.log(Level.SEVERE, e.getMessage());
					}
		} else if (batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}
	
	/**
	 * 
	 * @param response
	 * @param con
	 * @param readTimeout
	 * @param dataTypes
	 * @param primaryKey
	 * @param uniqueKeys
	 * @param payload
	 * @param objdata
	 * @return
	 */
	private Payload doCommitByProfile(ExecutionParameters executionParameters, int readTimeout,
			Map<String, String> dataTypes, List<String> primaryKey, Map<String, List<String>> uniqueKeys) {
		Connection con = executionParameters.getCon();
		OperationResponse response = executionParameters.getResponse();
		Payload payload = executionParameters.getPayload();
		ObjectData objdata = executionParameters.getObjdata();
		try {
			List<String> primaryKeyConflict = this.checkForViolation(objdata, primaryKey, dataTypes, con);
			List<String> uniqueKeyConflict =  new ArrayList<>();
			for (Map.Entry<String,List<String>> entry : uniqueKeys.entrySet()) {
				uniqueKeyConflict.addAll(this.checkForViolation(objdata, entry.getValue(), dataTypes, con));
			}
			String queryNonBatch = this.buildStatements(objdata, primaryKeyConflict, uniqueKeyConflict, con).toString();
			try (PreparedStatement pstmnt = con.prepareStatement(queryNonBatch)) {
			pstmnt.setQueryTimeout(readTimeout);
			this.appendParams(objdata, dataTypes, pstmnt, primaryKeyConflict, uniqueKeyConflict, con);
			
			int effectedRowCount = pstmnt.executeUpdate();
			
			payload = JsonPayloadUtil
					.toPayload(new QueryResponse(queryNonBatch, effectedRowCount, "Executed Successfully"));
			ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
			con.commit();
			}
		 } catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		} catch (IOException |NumberFormatException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} catch (ConnectorException e) {
			ResponseUtil.addExceptionFailure(response, objdata, e);
		}finally {
			IOUtil.closeQuietly(payload);
		}
		return payload;
	}

	/**
	 * This method will build the SQL Statements based on the conflict. If conflict
	 * is present it will build Insert statement orelse Update
	 *
	 * @param objdata   the objdata
	 * @param primaryKeyConflict
	 * @param uniqueKeyConflict 
	 * @param con
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private StringBuilder buildStatements(ObjectData objdata,List<String> primaryKeyConflict, List<String> uniqueKeyConflict, Connection con)
			throws SQLException, IOException {
		StringBuilder query = new StringBuilder();
		if (primaryKeyConflict.isEmpty() && uniqueKeyConflict.isEmpty()) {
			query = QueryBuilderUtil.buildInitialQuery(con, getContext().getObjectTypeId(), con.getSchema());
			this.buildInsertQuery(query, objdata, con);

		} else {
			this.buildUpdateSyntax(query, objdata, primaryKeyConflict, uniqueKeyConflict, con);

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
	 * @param query the query
	 * @param batchCount 
	 * @param con 
	 * @param primaryKey 
	 * @param readTimeout
	 * @throws SQLException the SQL exception
	 */
	private void doBatch(ExecutionParameters executionParameters, Map<String, String> dataTypes,
			StringBuilder query, Long batchCount, List<String> primaryKey, int readTimeout, Map<String, List<String>> uniqueKeys) throws SQLException {
		OperationResponse response = executionParameters.getResponse();
		List<ObjectData> batchData = executionParameters.getTrackedData();
		Connection con = executionParameters.getCon();
		int batchnum = 0;
		int b = 0;
		boolean shouldExecute = true;
		// Note: Here the PreparedStatement will be held in the memory. This issue has
		// been addressed in dbv2 connector. We will be informing the user to use batch
		// count less than 10 to limit the memory been held for some extent.
		try (PreparedStatement bstmnt = con.prepareStatement(query.toString())) {
			ExecutionParameters executionParametersToCheckBatchData = new ExecutionParameters(dataTypes, response,
					batchData, con, primaryKey, uniqueKeys);
			checkBatchData(executionParametersToCheckBatchData, batchCount, readTimeout, batchnum, b, shouldExecute, bstmnt);
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

	}
	@SuppressWarnings({"java:S3776"})
	private void checkBatchData(ExecutionParameters executionParametersToCheckBatchData, Long batchCount, int readTimeout,
			int batchnum, int b, boolean shouldExecute, PreparedStatement bstmnt)
			throws SQLException {
		Map<String, String> dataTypes = executionParametersToCheckBatchData.getDataTypes();
		OperationResponse response = executionParametersToCheckBatchData.getResponse();
		List<ObjectData> batchData = executionParametersToCheckBatchData.getTrackedData();
		Connection con = executionParametersToCheckBatchData.getCon();
		List<String> primaryKey = executionParametersToCheckBatchData.getPrimaryKey();
		Map<String, List<String>> uniqueKeys = executionParametersToCheckBatchData.getUniqueKeys();

		bstmnt.setQueryTimeout(readTimeout);
		for (ObjectData objdata : batchData) {
			b++;
			Payload payload = null;
			try {
				List<String> uniqueKeyConflict =  new ArrayList<>();
				for (Map.Entry<String,List<String>> entry : uniqueKeys.entrySet()) {
					uniqueKeyConflict.addAll(this.checkForViolation(objdata, entry.getValue(), dataTypes, con));
				}
				this.appendParams(objdata, dataTypes, bstmnt,this.checkForViolation(objdata, primaryKey, dataTypes,con), uniqueKeyConflict, con);
				bstmnt.addBatch();
				if (b == batchCount) {
					batchnum++;
					if (shouldExecute) {
						int res[] = bstmnt.executeBatch();
						bstmnt.clearParameters();
						con.commit();
						response.getLogger().log(Level.INFO, BATCH_NUM, batchnum);
						response.getLogger().log(Level.INFO, BATCH_RECORDS, res.length);
						payload = JsonPayloadUtil
								.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
						response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
								SUCCESS_RESPONSE_MESSAGE, payload);
					} else {
						bstmnt.clearBatch();
						shouldExecute = true;
						CustomResponseUtil.logFailedBatch(response, batchnum, b);
						CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
					}
					b = 0;
				} else if (b < batchCount) {
					ExecutionParameters executionParameters = new ExecutionParameters (con, response, batchData, objdata, payload);
					payload = executeRemainingBatchValue(executionParameters, batchnum, b, bstmnt);
				}

			} catch (BatchUpdateException e) {
				b = batchUpdateError(response, batchnum, b, objdata);
			} catch (SQLException e) {
				b = batchSQLException(response, batchCount, batchnum, b, objdata, e);
			} catch (IOException | IllegalArgumentException | ClassCastException e) {
				shouldExecute = this.checkLastRecord(b, batchCount);
				if (shouldExecute || batchData.lastIndexOf(objdata) == batchData.size() - 1) {
					bstmnt.clearBatch();
					batchnum++;
					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					b = 0;
				}
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, objdata, e);
			}

			finally {
				IOUtil.closeQuietly(payload);
			}
		}
	}
	
	/**
	 * 
	 * @param response
	 * @param batchCount
	 * @param batchnum
	 * @param b
	 * @param objdata
	 * @param e
	 * @return
	 */
	private int batchSQLException(OperationResponse response, Long batchCount, int batchnum, int b, ObjectData objdata,
			SQLException e) {
		boolean shouldExecute;
		CustomResponseUtil.logFailedBatch(response, batchnum, b);
		shouldExecute = this.checkLastRecord(b, batchCount);
		if (shouldExecute) {
			b = 0;
		}
		CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		return b;
	}
	
	/**
	 * 
	 * @param response
	 * @param batchnum
	 * @param b
	 * @param objdata
	 * @return
	 */
	private int batchUpdateError(OperationResponse response, int batchnum, int b, ObjectData objdata) {
		CustomResponseUtil.logFailedBatch(response, batchnum, b);
		CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
		b = 0;
		return b;
	}
	
	
	/**
	 * 
	 * @param response
	 * @param batchData
	 * @param con
	 * @param batchnum
	 * @param b
	 * @param bstmnt
	 * @param objdata
	 * @param payload
	 * @return
	 */
	private Payload executeRemainingBatchValue(ExecutionParameters executionParameters,
			int batchnum, int b, PreparedStatement bstmnt) {
		Connection con = executionParameters.getCon();
		ObjectData objdata = executionParameters.getObjdata();
		Payload payload = executionParameters.getPayload();
		OperationResponse response = executionParameters.getResponse();
		List<ObjectData> batchData = executionParameters.getTrackedData();
		int remainingBatch = batchnum + 1;
		if (batchData.lastIndexOf(objdata) == batchData.size() - 1) {
			this.executeRemaining(objdata, bstmnt, response, remainingBatch, con, b);
		} else {
			payload = JsonPayloadUtil.toPayload(
					new BatchResponse("Record added to batch successfully", remainingBatch, b));
			ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
		}
		return payload;
	}

	/**
	 * This method will check if any violation exists in the table and decides
	 * whether to form insert statement or update.
	 *
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param bstmnt    the bstmnt
	 * @param primaryKey
	 * @param uniqueKeyConflict 
	 * @param con  
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void appendParams(ObjectData objdata, Map<String, String> dataTypes, PreparedStatement bstmnt, List<String> primaryKeyConflict, List<String> uniqueKeyConflict, Connection con)
			throws SQLException, IOException {
		
		if (primaryKeyConflict.isEmpty() && uniqueKeyConflict.isEmpty()) {
			this.appendInsertParams(bstmnt, objdata, dataTypes, con);
		} else {
			this.appendUpdateParams(bstmnt, objdata, dataTypes, primaryKeyConflict, uniqueKeyConflict, con);
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
			List<String> primaryKeyConflict, List<String> uniqueKeyConflict, Connection con) throws IOException, SQLException {

		int i = 0;
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null);) {
					while (resultSet.next()) {
						String key = resultSet.getString(COLUMN_NAME);
						if (!primaryKeyConflict.contains(key) && !uniqueKeyConflict.contains(key)) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								i++;
								this.checkDataType(bstmnt, dataTypes, key, fieldName, i, con);
							}
						}
					}
				}

			} else {
				throw new ConnectorException(INPUT_ERROR);
			}
		}
		for (int j = 0; j <= primaryKeyConflict.size() - 1; j++) {

			i = checkKeyConflict(bstmnt, objdata, dataTypes, primaryKeyConflict, con, i, j);

		}
		
		for (int j = 0; j <= uniqueKeyConflict.size() - 1; j++) {

			i = checkKeyConflict(bstmnt, objdata, dataTypes, uniqueKeyConflict, con, i, j);

		}
	}
	/**
	 * 
	 * @param bstmnt
	 * @param objdata
	 * @param dataTypes
	 * @param primaryKeyConflict
	 * @param con
	 * @param i
	 * @param j
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	private int checkKeyConflict(PreparedStatement bstmnt, ObjectData objdata, Map<String, String> dataTypes,
			List<String> primaryKeyConflict, Connection con, int i, int j) throws IOException, SQLException {
		String key = primaryKeyConflict.get(j);

		JsonNode jsonNode = null;
		try (InputStream is = objdata.getData()) {
			jsonNode = mapper.readTree(is);
			if (jsonNode != null) {
				JsonNode fieldName = jsonNode.get(key);
				if (fieldName != null) {
					i++;
					this.checkDataType(bstmnt, dataTypes, key, fieldName, i, con);
				}
			}

		}
		return i;
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
	private void buildUpdateSyntax(StringBuilder query, ObjectData objdata, List<String> primaryKeyconflict, List<String> uniqueKeyConflict, Connection con)
			throws SQLException, IOException {
		
		JsonNode json = null;
		boolean parameterSet = false;
			query.append("UPDATE ").append(getContext().getObjectTypeId()).append(" SET ");
		try (InputStream is = objdata.getData()) {
			json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null);) {
					while (resultSet.next()) {
						String key = resultSet.getString(COLUMN_NAME);
						if (!primaryKeyconflict.contains(key) && !uniqueKeyConflict.contains(key)) {
							JsonNode fieldName = json.get(key);
							if (fieldName != null) {
								query.append(key).append("=");
								query.append(PARAM);
							}
						}
					}
				}
			} else {
				throw new ConnectorException(INPUT_ERROR);
			}
			query.deleteCharAt(query.length() - 1);
		}
		query.append(OracleDatabaseConstants.WHERE);
		parameterSet = setOnPrimaryKeyConflict(query, primaryKeyconflict, parameterSet);
		setOnUniqueKeyConflict(query, uniqueKeyConflict, json, parameterSet);

	}

	private void setOnUniqueKeyConflict(StringBuilder query, List<String> uniqueKeyConflict, JsonNode json, boolean parameterSet) {
		for (int i = 0; i <= uniqueKeyConflict.size() - 1; i++) {
			String key = uniqueKeyConflict.get(i);
			JsonNode fieldName = json.get(key);
			if(parameterSet) {
				query.append(OracleDatabaseConstants.AND).append(" ");
				parameterSet = false;
			}
			if (fieldName != null) {
				query.append(key).append(" = ");
				query.append("?");
				parameterSet = true;
			}

		}
	}

	private boolean setOnPrimaryKeyConflict(StringBuilder query, List<String> primaryKeyconflict, boolean parameterSet) {
		for (int i = 0; i <= primaryKeyconflict.size() - 1; i++) {
			if (i > 0) {
				query.append(OracleDatabaseConstants.AND).append(" ");
			}
			String key = primaryKeyconflict.get(i);
			query.append(key).append(" = ");
			query.append("?");
			parameterSet = true;

		}
		return parameterSet;
	}

	/**
	 * This method will append the values to the Insert Query formed for the
	 * prepared Statements.
	 *
	 * @param bstmnt    the bstmnt
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void appendInsertParams(PreparedStatement bstmnt, ObjectData objdata, Map<String, String> dataTypes, Connection con)
			throws SQLException, IOException {
		int i = 0;
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null);) {
					while (resultSet.next()) {
						String key = resultSet.getString(COLUMN_NAME);
						JsonNode fieldName = json.get(key);
						i++;
						this.checkDataType(bstmnt, dataTypes, key, fieldName, i, con);

					}
				}
			} else {
				throw new ConnectorException(INPUT_ERROR);
			}

		}

	}

	/**
	 * Builds the prepared statement by taking the 1st request of the tracked data.
	 * if 1st request is not proper or if it throws any exception it will move to
	 * subsequent requests until the query is formed.
	 *
	 * @param batchData the batch data
	 * @param dataTypes the data types
	 * @param primaryKey 
	 * @param con    
	 * @param uniqueKeys
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 */
	private StringBuilder buildPreparedStatement(List<ObjectData> batchData, Map<String, String> dataTypes, Connection con, List<String> primaryKey, Map<String, List<String>> uniqueKeys)
			throws SQLException {
		StringBuilder query = new StringBuilder();
		for (ObjectData objdata : batchData) {
			try {
				List<String> uniqueKeyConflict =  new ArrayList<>();
				for (Map.Entry<String,List<String>> entry : uniqueKeys.entrySet()) {
					uniqueKeyConflict.addAll(this.checkForViolation(objdata, entry.getValue(), dataTypes,con));
				}
				query = this.buildStatements(objdata, this.checkForViolation(objdata, primaryKey, dataTypes,con), uniqueKeyConflict,con);
			} catch (IOException | IllegalArgumentException | ClassCastException e) {
				// moving to next request
				logger.log(Level.SEVERE, e.toString());
			}
			if (query.length() != 0) {
				break;
			}
		}
		return query;
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
	public List<String> checkForViolation(ObjectData objdata, List<String> primaryKeys, Map<String, String> dataTypes, Connection con)
			throws SQLException, IOException{
		List<String> conflict = new ArrayList<>();
		JsonNode json = null;
		boolean canExecute = false;
		try (InputStream is = objdata.getData();) {
			json = mapper.readTree(is);
			if (json != null) {
				StringBuilder query = new StringBuilder();
				for (int i = 0; i <= primaryKeys.size() - 1; i++) {
					if(i==0) {
							query.append("Select ").append(primaryKeys.get(i)).append(" from ").append(getContext().getObjectTypeId().toUpperCase()).append(OracleDatabaseConstants.WHERE);
					}
					String key = primaryKeys.get(i);
					JsonNode fieldName = json.get(key);
					
					if(fieldName!= null) {
						canExecute = true;
						query.append(primaryKeys.get(i)).append(" = ?");
						query.append(OracleDatabaseConstants.AND).append(" ");
					}
				}
				if(query.length()>0) {
					conflict = checkQueryLength(primaryKeys, dataTypes, con, conflict, json, canExecute, query);
				}
			} else {
				throw new ConnectorException(INPUT_ERROR);
			}

		}
		catch (IOException | IllegalArgumentException e) {
			logger.log(Level.SEVERE, e.toString());
		}
		return conflict;

	}
	
	/**
	 * 
	 * @param primaryKeys
	 * @param dataTypes
	 * @param con
	 * @param conflict
	 * @param json
	 * @param canExecute
	 * @param query
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private List<String> checkQueryLength(List<String> primaryKeys, Map<String, String> dataTypes, Connection con,
			List<String> conflict, JsonNode json, boolean canExecute, StringBuilder query)
			throws SQLException, IOException {
		query = query.delete(query.length()-5, query.length());
		try(PreparedStatement stmt = con.prepareStatement(query.toString())){
			int j=1;
			for (int i = 0; i <= primaryKeys.size() - 1; i++) {
				String key = primaryKeys.get(i);
				JsonNode fieldName = json.get(key);
				if(fieldName != null) {
					this.checkDataType(stmt, dataTypes, key, fieldName, j, con);
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
		return conflict;
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
	private void checkDataType(PreparedStatement bstmnt, Map<String, String> dataTypes, String key, JsonNode fieldName,
			int i, Connection con) throws SQLException, IOException {
		String databaseName = con.getMetaData().getDatabaseProductName();
		switch (dataTypes.get(key)) {
		case INTEGER:
			setIntType(bstmnt, fieldName, i);
			break;
		case DATE:
			setDateType(bstmnt, fieldName, i, databaseName);
			break;
		case STRING:
			setStringType(bstmnt, fieldName, i);
			break;
		case NVARCHAR:
			setNvarcharType(bstmnt, fieldName, i);
			break;
		case TIME:
			setTimeType(bstmnt, fieldName, i);
			break;
		case BOOLEAN:
			setBooleanType(bstmnt, fieldName, i);
			break;
		case LONG:
			setLongType(bstmnt, fieldName, i);
			break;
		case DOUBLE:
		case FLOAT:	
			setDecimalType(bstmnt, fieldName, i);
			break;
		case BLOB:
			setBlobType(bstmnt, fieldName, i, databaseName);
			break;
		case TIMESTAMP:
			setTimestampType(bstmnt, fieldName, i);
			break;
		default:
			break;
		}

	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setTimestampType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			QueryBuilderUtil.timeStampDataType(bstmnt,i,fieldName);
		} else {
			bstmnt.setNull(i, Types.TIMESTAMP);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @param databaseName
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setBlobType(PreparedStatement bstmnt, JsonNode fieldName, int i, String databaseName)
			throws SQLException, IOException {
		if (fieldName != null) {
			String value = fieldName.toString().replace(BACKSLASH, "");
			try(InputStream stream = new ByteArrayInputStream(value.getBytes());){
			if(databaseName.equals(POSTGRESQL))
				bstmnt.setBinaryStream(i, stream);
			else
				bstmnt.setBlob(i, stream);	
			}
		} else {
			bstmnt.setNull(i, Types.BLOB);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setDecimalType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			double value = Double.parseDouble(fieldName.toString().replace(BACKSLASH, ""));
			bstmnt.setDouble(i, value);
		} else {
			bstmnt.setNull(i, Types.DECIMAL);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setLongType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			long value = Long.parseLong(fieldName.toString().replace(BACKSLASH, ""));
			bstmnt.setLong(i, value);
		} else {
			bstmnt.setNull(i, Types.BIGINT);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setBooleanType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			boolean flag = Boolean.parseBoolean(fieldName.toString().replace(BACKSLASH, ""));
			bstmnt.setBoolean(i, flag);
		} else {
			bstmnt.setNull(i, Types.BOOLEAN);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setTimeType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setTime(i, Time.valueOf(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			bstmnt.setNull(i, Types.TIME);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setNvarcharType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldName.toString().replace(BACKSLASH, "")));
		} else {
			bstmnt.setNull(i, Types.TIME);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setStringType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			bstmnt.setString(i, fieldName.toString().replace(BACKSLASH, ""));
		} else {
			bstmnt.setNull(i, Types.VARCHAR);
		}
	}
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @param databaseName
	 * @throws SQLException
	 */
	private void setDateType(PreparedStatement bstmnt, JsonNode fieldName, int i, String databaseName)
			throws SQLException {
		if (fieldName != null) {
			if (databaseName.equals(ORACLE)) {
				bstmnt.setString(i, fieldName.toString().replace(BACKSLASH, ""));
			} else {
				try {
					bstmnt.setDate(i, Date.valueOf(fieldName.toString().replace(BACKSLASH, "")));
				}catch(IllegalArgumentException e) {
					throw new IllegalArgumentException(OracleDatabaseConstants.INVALID_ERROR +e);
				}
			}
		} else {
			bstmnt.setNull(i, Types.DATE);
		}
	}
	
	/**
	 * 
	 * @param bstmnt
	 * @param fieldName
	 * @param i
	 * @throws SQLException
	 */
	private void setIntType(PreparedStatement bstmnt, JsonNode fieldName, int i) throws SQLException {
		if (fieldName != null) {
			BigDecimal num = new BigDecimal(fieldName.toString().replace(BACKSLASH, ""));
			bstmnt.setBigDecimal(i, num);
		} else {
			bstmnt.setNull(i, Types.INTEGER);
		}
	}
	
	/**
	 * This method will build the Insert query based on the Input Request.
	 *
	 * @param query     the query
	 * @param objdata   the objdata
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void buildInsertQuery(StringBuilder query, ObjectData objdata, Connection con) throws SQLException, IOException {
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			json = mapper.readTree(is);
			if (json != null) {
				DatabaseMetaData md = con.getMetaData();
				try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), null);) {
					while (resultSet.next()) {
						query.append(PARAM);
					}
				}

			} else {
				throw new ConnectorException(INPUT_ERROR);
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
	private Map<String, List<String>> getUniqueKeys(List<String> pk, Connection con) throws SQLException {
		Map<String, List<String>> indexName = new HashMap<>();
		List<String> uindex = new ArrayList<>();
		try (ResultSet resultSet = con.getMetaData().getIndexInfo(con.getCatalog(), con.getSchema(), getContext().getObjectTypeId(), true, false)) {
			while (resultSet.next()) {
				if (null != resultSet.getString(NON_UNIQUE)
						&& (resultSet.getString(NON_UNIQUE).equals("0") || resultSet.getString(NON_UNIQUE).equals("f"))
						&& resultSet.getString(COLUMN_NAME) != null && !pk.contains(resultSet.getString(COLUMN_NAME))) {
					if(indexName.get(resultSet.getString(OracleDatabaseConstants.INDEX_NAME)) != null) {
						List<String> ukindex = indexName.get(resultSet.getString(OracleDatabaseConstants.INDEX_NAME));
						ukindex.add(resultSet.getString(COLUMN_NAME));
						indexName.replace(resultSet.getString(OracleDatabaseConstants.INDEX_NAME), ukindex);
					}else {
						uindex.add(resultSet.getString(COLUMN_NAME));
						indexName.putIfAbsent(resultSet.getString(OracleDatabaseConstants.INDEX_NAME), uindex);
					}
				}
			}
		}
		return indexName;

	}
	
	

	/**
	 * This method will execute the remaining statements of the batch.
	 *
	 * @param data           the data
	 * @param execStatement  the exec statement
	 * @param response       the response
	 * @param remainingBatch the remaining batch
	 * @param con            the con
	 * @param b              the b
	 */
	private void executeRemaining(ObjectData data, PreparedStatement execStatement, OperationResponse response,
			int remainingBatch, Connection con, int b) {
		Payload payload = null;
		try {
			int[] res = execStatement.executeBatch();
			response.getLogger().log(Level.INFO, BATCH_NUM, remainingBatch);
			response.getLogger().log(Level.INFO, REMAINING_BATCH_RECORDS, res.length);
			payload = JsonPayloadUtil.toPayload(new BatchResponse(
					"Remaining records added to batch and executed successfully", remainingBatch, res.length));
			response.addResult(data, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE, SUCCESS_RESPONSE_MESSAGE, payload);
			con.commit();
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

	private boolean checkLastRecord(int b, Long batchCount) {
		return b == batchCount;
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
