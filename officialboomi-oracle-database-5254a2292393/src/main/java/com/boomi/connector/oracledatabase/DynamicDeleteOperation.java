// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import com.boomi.connector.oracledatabase.model.BatchResponse;
import com.boomi.connector.oracledatabase.model.DeletePojo;
import com.boomi.connector.oracledatabase.model.QueryResponse;
import com.boomi.connector.oracledatabase.model.Where;
import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * The Class DynamicDeleteOperation.
 *
 * @author swastik.vn
 */
public class DynamicDeleteOperation extends SizeLimitedUpdateOperation {
	/**
	 * Instantiates a new dynamic delete operation.
	 *
	 * @param con the con
	 */
	public DynamicDeleteOperation(OracleDatabaseConnection con) {
		super(con);
	}

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(DynamicDeleteOperation.class.getName());

	/**
	 * Execute update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

		OracleDatabaseConnection conn = getConnection();
		Long batchCount = getContext().getOperationProperties().getLongProperty(BATCH_COUNT);
		String commitOption = getContext().getOperationProperties().getProperty(COMMIT_OPTION);
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			con.setAutoCommit(false);
			this.executeStatements(con, request, response, batchCount, commitOption);
			logger.log(Level.INFO, "Statements Executed!!");
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * Execute statements. This Method will take the input params and process the
	 * statements according to the batch count provided
	 *
	 * @param con          the con
	 * @param request      the request
	 * @param response     the response
	 * @param batchCount   the batch count
	 * @param commitOption the commit option
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void executeStatements(Connection con, UpdateRequest request, OperationResponse response, Long batchCount,
			String commitOption) throws  IOException {
		// We are extending SizeLimitUpdate Operation it loads only single document into
		// memory. Hence we are preparing the list of Object Data which will be required
		// for Statement batching.
		List<ObjectData> trackedData = new ArrayList<>();
		for (ObjectData objdata : request) {
			trackedData.add(objdata);
		}
		StringBuilder query = new StringBuilder(DELETE_QUERY + getContext().getObjectTypeId());
		this.appendKeys(trackedData, query);
		Map<String, String> dataTypes = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
		if (batchCount != null && batchCount > 0 && commitOption.equals(COMMIT_BY_ROWS)) {
			this.doBatch(con, dataTypes, batchCount, trackedData, response, readTimeout);
		} else if (COMMIT_BY_PROFILE.equals(commitOption) || batchCount == null || batchCount == 0) {
			doCommitByProfile(con, response, trackedData, query, dataTypes, readTimeout);

		} else if (batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	
	/**
	 * 
	 * @param con
	 * @param response
	 * @param trackedData
	 * @param query
	 * @param dataTypes
	 * @param readTimeout
	 */
	private void doCommitByProfile(Connection con, OperationResponse response, List<ObjectData> trackedData,
			StringBuilder query, Map<String, String> dataTypes, int readTimeout) {
		for (ObjectData objdata : trackedData) {
			Payload payload = null;
			try (PreparedStatement bstmnt = con.prepareStatement(query.toString())) {
				bstmnt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
				this.appendValues(objdata, bstmnt, dataTypes, con, response);
				int rowsEffected = bstmnt.executeUpdate();
				payload = JsonPayloadUtil
						.toPayload(new QueryResponse(query.toString(), rowsEffected, "Executed Successfully"));
				response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
						SUCCESS_RESPONSE_MESSAGE, payload);

			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			} catch (IOException e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, objdata, e);
			} finally {
				IOUtil.closeQuietly(payload);
			}
		}
		try {
			con.commit();
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param con         the con
	 * @param dataTypes   the data types
	 * @param batchCount  the batch count
	 * @param trackedData the tracked data
	 * @param response    the response
	 * @param readTimeout readTimeout
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings({"java:S1141","java:S3776"})
	private void doBatch(Connection con, Map<String, String> dataTypes, Long batchCount, List<ObjectData> trackedData,
			OperationResponse response, int readTimeout) throws IOException {

		int batchnum = 0;
		int b = 0;
		boolean shouldExecute = true;
		StringBuilder query = new StringBuilder(DELETE_QUERY + getContext().getObjectTypeId());
		this.appendKeys(trackedData, query);
		// Note: Here the PreparedStatement will be held in the memory. This issue has
		// been addressed in dbv2 connector. We will be informing the user to use batch
		// count less than 10 to limit the memory been held for some extent.
		try (PreparedStatement bstmnt = con.prepareStatement(query.toString());) {
			bstmnt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
			for (ObjectData objdata : trackedData) {
				b++;
				Payload payload = null;
				try {
					this.appendValues(objdata, bstmnt, dataTypes, con, response);
					bstmnt.addBatch();
					if (b == batchCount) {
						batchnum++;
						if (shouldExecute) {
							payload = checkShouldExecute(con, response, batchnum, bstmnt, objdata);
						} else {
							bstmnt.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, b);
							CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
						}

						b = 0;

					} else if (b < batchCount) {
						ExecutionParameters executionParameters = new ExecutionParameters(con, response, trackedData, objdata, payload);
						payload = executeRemainingBatch(executionParameters, batchnum, b, bstmnt);
					}

				} catch (BatchUpdateException e) {

					b = checkBatchUpdateException(response, batchnum, b, objdata, e);
				} catch (SQLException e) {
					b = checkSQLException(batchCount, response, batchnum, b, objdata, e);
				} catch (IOException | IllegalArgumentException | ClassCastException e) {
					shouldExecute = this.checkLastRecord(b, batchCount);
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
			throw new ConnectorException("Creating the statement got failed from Connection");
		}

	}

	/**
	 * 
	 * @param batchCount
	 * @param response
	 * @param batchnum
	 * @param b
	 * @param objdata
	 * @param e
	 * @return
	 */
	private int checkSQLException(Long batchCount, OperationResponse response, int batchnum, int b, ObjectData objdata,
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
	 * @param e
	 * @return
	 */
	private int checkBatchUpdateException(OperationResponse response, int batchnum, int b, ObjectData objdata,
			BatchUpdateException e) {
		CustomResponseUtil.logFailedBatch(response, batchnum, b);
		CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		b = 0;
		return b;
	}

	/**
	 * 
	 * @param con
	 * @param response
	 * @param batchnum
	 * @param bstmnt
	 * @param objdata
	 * @return
	 * @throws SQLException
	 */
	private Payload checkShouldExecute(Connection con, OperationResponse response, int batchnum,
			PreparedStatement bstmnt, ObjectData objdata) throws SQLException {
		Payload payload;
		int res[] = bstmnt.executeBatch();
		bstmnt.clearParameters();
		con.commit();
		response.getLogger().log(Level.INFO, BATCH_NUM, batchnum);
		response.getLogger().log(Level.INFO, BATCH_RECORDS, res.length);
		payload = JsonPayloadUtil
				.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
		response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
				SUCCESS_RESPONSE_MESSAGE, payload);
		return payload;
	}

	/**
	 *
	 * @param con
	 * @param trackedData
	 * @param response
	 * @param batchnum
	 * @param b
	 * @param bstmnt
	 * @param objdata
	 * @param payload
	 * @return
	 */
	private Payload executeRemainingBatch(ExecutionParameters executionParameters,
			int batchnum, int b, PreparedStatement bstmnt) {
		Connection con = executionParameters.getCon();
		List<ObjectData> trackedData = executionParameters.getTrackedData();
		ObjectData objdata = executionParameters.getObjdata();
		Payload payload = executionParameters.getPayload();
		OperationResponse response = executionParameters.getResponse();

		int remainingBatch = batchnum + 1;
		if (trackedData.lastIndexOf(objdata) == trackedData.size() - 1) {
			this.executeRemaining(objdata, bstmnt, response, remainingBatch, con, b);
		} else {
			payload = JsonPayloadUtil.toPayload(
					new BatchResponse("Record added to batch successfully", remainingBatch, b));
			ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
		}
		return payload;
	}

	/**
	 * This method will append the question mark place holders to the query based on
	 * the column names.
	 *
	 * @param batchData the batch data
	 * @param query     the query
	 * @param response
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void appendKeys(List<ObjectData> batchData, StringBuilder query) {

		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory).disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		for (ObjectData data : batchData) {
			boolean dataConsistent = false;
			try (InputStream is = data.getData()) {
				DeletePojo deletePojo = mapper.readValue(is, DeletePojo.class);
				boolean comma = false;
				if (deletePojo.getWhere() != null) {
					for (Where where : deletePojo.getWhere()) {
						this.whereInitial(comma, query);
						String column = where.getColumn().toUpperCase();
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
				logger.fine("Request improper, moving to next request " + e);
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

	private boolean checkLastRecord(int b, Long batchCount) {
		return b == batchCount;
	}

	/**
	 * This method will append the values to the Prepared Statement Parameters.
	 *
	 * @param data          the data
	 * @param execStatement the exec statement
	 * @param dataType      the data type
	 * @param con           the con
	 * @param response      the response
	 * @param batchCount
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 * @throws IOException          Signals that an I/O exception has occurred.
	 */
	public void appendValues(ObjectData data, PreparedStatement execStatement, Map<String, String> dataType,
			Connection con, OperationResponse response) throws IOException {

		try (InputStream is = data.getData()) {
			ObjectMapper mapper = JSONUtil.getDefaultObjectMapper()
					.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
			DeletePojo deletePojo = mapper.readValue(is, DeletePojo.class);
			int i = 0;

			if (deletePojo.getWhere() != null) {

				for (Where where : deletePojo.getWhere()) {
					i++;
					String column = where.getColumn().toUpperCase();
					String value = where.getValue();
					QueryBuilderUtil.checkDataType(execStatement, dataType, column, value, i);

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
	private void whereInitial(boolean comma, StringBuilder query) {
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
	public OracleDatabaseConnection getConnection() {
		return (OracleDatabaseConnection) super.getConnection();
	}

}
