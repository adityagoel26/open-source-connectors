// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
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
import com.boomi.connector.oracledatabase.model.QueryResponse;
import com.boomi.connector.oracledatabase.model.Set;
import com.boomi.connector.oracledatabase.model.UpdatePojo;
import com.boomi.connector.oracledatabase.model.Where;
import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.util.SchemaBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

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
	public DynamicUpdateOperation(OracleDatabaseConnection connection) {
		super(connection);
	}

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(DynamicUpdateOperation.class.getName());

	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	/**
	 * Execute update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

		OracleDatabaseConnection conn = getConnection();
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName =getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			con.setAutoCommit(false);
			this.executeUpdateOperation(request, response, con);
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
	 * @param con         the con
	 * @throws SQLException            the SQL exception
	 * @throws JsonProcessingException the json processing exception
	 */
	public void executeUpdateOperation(UpdateRequest trackedData, OperationResponse response, Connection con)
			throws SQLException, JsonProcessingException {

		Long batchCount = getContext().getOperationProperties().getLongProperty(BATCH_COUNT);
		String commitOption = getContext().getOperationProperties().getProperty(COMMIT_OPTION);
		// This Map will be getting the datatype of the each column associated with the
		// table.
		Map<String, String> dataType = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
		if (batchCount != null && batchCount > 0 && commitOption.equals(COMMIT_BY_ROWS)) {
			// We are extending SizeLimitUpdate Operation it loads only single document into
			// memory. Hence we are preparing the list of Object Data which will be required
			// for Statement batching.
			List<ObjectData> batchData = new ArrayList<>();
			for (ObjectData objdata : trackedData) {
				batchData.add(objdata);
			}
			this.doBatch(con, dataType, batchCount, batchData, response, readTimeout);
		} else if (commitOption.equals(COMMIT_BY_PROFILE) || batchCount == null || batchCount == 0) {
			doCommitByProfile(trackedData, response, con, dataType, readTimeout);
		} else if (batchCount < 0) {
			throw new ConnectorException("Batch Count Cannot be negative!!!");
		}

	}

	
	/**
	 * 
	 * @param trackedData
	 * @param response
	 * @param con
	 * @param dataType
	 * @param readTimeout
	 * @throws SQLException
	 */
	private void doCommitByProfile(UpdateRequest trackedData, OperationResponse response, Connection con,
			Map<String, String> dataType, int readTimeout) throws SQLException {
		for (ObjectData data : trackedData) {
			Payload payload = null;
			StringBuilder query = this.getInitialQuery(getContext().getObjectTypeId());
			this.appendStatementKeys(data, query, con);
			try (PreparedStatement execStatement = con.prepareStatement(query.toString())) {
				execStatement.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
				this.appendValuesNonBatch(data, execStatement, dataType, con, response);
				int updatedRowCount = execStatement.executeUpdate();
				con.commit();
				payload = JsonPayloadUtil
						.toPayload(new QueryResponse(query.toString(), updatedRowCount, "Executed Successfully"));
				response.addResult(data, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE, SUCCESS_RESPONSE_MESSAGE,
						payload);

			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, data, response);
			} catch (IOException | IllegalArgumentException| ClassCastException e) {
				CustomResponseUtil.writeErrorResponse(e, data, response);
			} catch (ConnectorException e) {
				ResponseUtil.addExceptionFailure(response, data, e);
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
	 * This method will build the Prepared Statements by appending the ? by
	 * processing the input request. For batching of statements this method will
	 * check the input request until the request is consistent.
	 *
	 * @param data  the data
	 * @param query the query
	 * @param con   the con
	 * @return the boolean
	 * @throws SQLException the SQL exception
	 */
	@SuppressWarnings({"java:S3776"})
	private Boolean appendStatementKeys(ObjectData data, StringBuilder query, Connection con) throws SQLException {

		boolean dataConsistent = false;
		String[] value1 = null;
		String[] value2 = null;
		try (InputStream is = data.getData()) {
			UpdatePojo updatePojo = mapper.readValue(is, UpdatePojo.class);
			boolean comma = false;
			for (Set set : updatePojo.getSet()) {
				this.setInitial(comma, query);
				String key = set.getColumn().toUpperCase();
				query.append(key);
				query.append("=");
				if (getContext().getOperationProperties().getBooleanProperty("nestedTable", false).equals(true)) {
					if (null != set.getInnerTableTypeName1()) {
						ArrayDescriptor array = ArrayDescriptor.createDescriptor(
								con.getMetaData().getUserName() + DOT + set.getInnerTableTypeName1(), SchemaBuilderUtil.getUnwrapConnection(con));
						query.append(con.getMetaData().getUserName() + DOT + set.getInnerTableTypeName1() + '('
								+ array.getBaseName() + '(');
						if (null != set.getInnerTableValue1()) {
							value1 = set.getInnerTableValue1();
							for (int i = 0; i <= value1.length - 1; i++) {
								if (value1[i] != null) {
									dataConsistent = true;
									query.append("?,");
								}
							}
						}

					}
					if (null != set.getInnerTableTypeName2()) {
						ArrayDescriptor array = ArrayDescriptor.createDescriptor(
								con.getMetaData().getUserName() + DOT + set.getInnerTableTypeName2(), SchemaBuilderUtil.getUnwrapConnection(con));

						query.append(con.getMetaData().getUserName() + DOT + set.getInnerTableTypeName2() + '('
								+ array.getBaseName() + '(');
					}
					if (null != set.getInnerTableValue2()) {
						value2 = set.getInnerTableValue2();
						for (int i = 0; i <= value2.length - 1; i++) {
							if (set.getInnerTableValue2()[i] != null) {
								query.append("?,");
							}
						}

					}
					query.deleteCharAt(query.length() - 1);
					if (null != value1 && value1.length != 0) {
						query.append("))");
					}
					if (null != value2 && value2.length != 0) {
						query.append("))");
					}
				} else {
					String[] value = set.getValue();
					if (value != null) {
						dataConsistent = true;
						query.append('?');
					}
				}
				comma = true;
			}
			comma = false;
			if (updatePojo.getWhere() != null) {
				for (Where where : updatePojo.getWhere()) {
					this.whereInitial(comma, query);
					String column = where.getColumn().toUpperCase();
					query.append(column);
					String operator = where.getOperator();
					query.append(operator);
					String value = where.getValue();
					if (value != null) {
						query.append('?');
					}
					comma = true;
				}
			}
		} catch (IOException e) {
			logger.fine("moving to next request " + e);
		}
		return dataConsistent;
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param con        the con
	 * @param dataType   the data type
	 * @param batchCount the batch count
	 * @param batchData  the tracked data
	 * @param response   the response
	 * @param readTimeout
	 */
	@SuppressWarnings({"java:S1141","java:S3776"})
	private void doBatch(Connection con, Map<String, String> dataType, Long batchCount, List<ObjectData> batchData,
			OperationResponse response, int readTimeout) {
		int b = 0;
		int batchnum = 0;
		boolean shouldExecute = true;
		StringBuilder query = this.getInitialQuery(getContext().getObjectTypeId());
		this.appendKeysBatch(batchData, query, response, con);
		// Note: Here the PreparedStatement will be held in the memory. This issue has
		// been addressed in dbv2 connector. We will be informing the user to use batch
		// count less than 10 to limit the memory been held for some extent.
		try (PreparedStatement execStatement = con.prepareStatement(query.toString())) {
			execStatement.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
			for (ObjectData data : batchData) {
				Payload payload = null;
				try {
					b++;
					this.appendValuesNonBatch(data, execStatement, dataType, con, response);
					execStatement.addBatch();
					if (b == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int res[] = execStatement.executeBatch();
							con.commit();
							response.getLogger().log(Level.INFO, BATCH_NUM,batchnum);
							response.getLogger().log(Level.INFO, BATCH_RECORDS,res.length);
							payload = JsonPayloadUtil
									.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
							response.addResult(data, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
									SUCCESS_RESPONSE_MESSAGE, payload);
						} else {
							execStatement.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, b);
							CustomResponseUtil.batchExecuteError(data, response, batchnum, b);
						}
						b = 0;
					} else if (b < batchCount) {
						ExecutionParameters executionParameters = new ExecutionParameters(con, response, batchData, data, payload);
						payload = executeRemaingBatchValue(executionParameters, b, batchnum, execStatement);
					}
				} catch (BatchUpdateException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					CustomResponseUtil.batchExecuteError(data, response, batchnum, b);
					b = 0;
				} catch (SQLException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					shouldExecute = this.checkLastRecord(b, batchCount);
					if (shouldExecute) {
						b = 0;
					}
					CustomResponseUtil.writeSqlErrorResponse(e, data, response);
				} catch (IOException | IllegalArgumentException | ClassCastException e) {
					shouldExecute = this.checkLastRecord(b, batchCount);
					if (shouldExecute || batchData.lastIndexOf(data) == batchData.size() - 1) {
						execStatement.clearBatch();
						batchnum++;
						CustomResponseUtil.logFailedBatch(response, batchnum, b);
						b = 0;
					}
					CustomResponseUtil.writeErrorResponse(e, data, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, data, e);
				} finally {
					IOUtil.closeQuietly(payload);
				}
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

	}

	
	/**
	 * 
	 * @param con
	 * @param batchData
	 * @param response
	 * @param b
	 * @param batchnum
	 * @param execStatement
	 * @param data
	 * @param payload
	 * @return
	 */
	private Payload executeRemaingBatchValue(ExecutionParameters executionParameters,
			int b, int batchnum, PreparedStatement execStatement) {
		Connection con = executionParameters.getCon();
		List<ObjectData> batchData = executionParameters.getTrackedData();
		OperationResponse response = executionParameters.getResponse();
		ObjectData data = executionParameters.getObjdata();
		Payload payload = executionParameters.getPayload();

		int remainingBatch = batchnum + 1;
		if (batchData.lastIndexOf(data) == batchData.size() - 1) {
			this.executeRemaining(data, execStatement, response, remainingBatch, con, b);
		} else {
			payload = JsonPayloadUtil.toPayload(
					new BatchResponse("Record added to batch successfully", remainingBatch, b));
			ResponseUtil.addSuccess(response, data, SUCCESS_RESPONSE_CODE, payload);
		}
		return payload;
	}

	/**
	 * Check data type set.
	 *
	 * @param dataType the data type
	 * @param key      the key
	 * @param elements the elements
	 * @param bstmnt   the bstmnt
	 * @param i        the i
	 * @param con      the con
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private void checkDataTypeSet(Map<String, String> dataType, String key, String[] elements, PreparedStatement bstmnt,
			int i, Connection con) throws SQLException, IOException {

		if (!dataType.get(key).equals(ARRAY)) {
			String value = elements[0];
			QueryBuilderUtil.checkDataType(bstmnt, dataType, key.toUpperCase(), value, i);

		} else {
			if (elements != null) {
				ARRAY array = this.getArrayType(con, key, elements);
				bstmnt.setArray(i, array);
			} else {
				bstmnt.setNull(i, Types.ARRAY);
			}
		}
	}

	/**
	 * This method will take the values from the input requests and prepare the
	 * ARRAY object which is required to set the Array in Prepared Statements.
	 *
	 * @param con      the con
	 * @param key      the key
	 * @param elements the elements
	 * @return the array type
	 * @throws SQLException the SQL exception
	 */
	private ARRAY getArrayType(Connection con, String key, String[] elements) throws SQLException {
		String typeName = this.getTypeName(con, key);
		ArrayDescriptor des = ArrayDescriptor.createDescriptor(typeName, SchemaBuilderUtil.getUnwrapConnection(con));

		Object[] array = new Object[elements.length];
		int k = 0;
		for (int j = 1; j <= elements.length; j++) {
			if (elements[k].equals("")) {
				array[k] = null;
			} else {
				array[k] = elements[k].replace("\"", "");
			}
			k++;
		}
		return new ARRAY(des, SchemaBuilderUtil.getUnwrapConnection(con), array);
	}

	/**
	 * Gets the type name of the Column.
	 *
	 * @param con the con
	 * @param key the key
	 * @return the type name
	 */
	private String getTypeName(Connection con, String key) {
		String typeName = null;
		try (ResultSet rs = con.getMetaData().getColumns(null, null, getContext().getObjectTypeId(), null)) {
			while (rs.next()) {
				if (rs.getString("COLUMN_NAME").equals(key)) {
					typeName = rs.getString("TYPE_NAME");
				}
			}
		} catch (SQLException e) {
			throw new ConnectorException("Error while getting Type name of the column!!");
		}
		return typeName;
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
			response.getLogger().log(Level.INFO, BATCH_NUM,remainingBatch);
			response.getLogger().log(Level.INFO, REMAINING_BATCH_RECORDS,res.length);
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
	 * This Method will Build the Initial String required for the Update Statement.
	 *
	 * @param objectTypeId the object type id
	 * @return new StringBuilder with initial string required for Update Operation
	 */
	private StringBuilder getInitialQuery(String objectTypeId) {
		return new StringBuilder("UPDATE " + objectTypeId + " SET ");
	}

	/**
	 * This method will build the prepared statement query required for Update
	 * Operation based on the SET and WHERE parameters in the Requests.
	 *
	 * @param batchData the data
	 * @param query     the query
	 * @param response  the response
	 * @param con       the con
	 */
	public void appendKeysBatch(List<ObjectData> batchData, StringBuilder query, OperationResponse response,
			Connection con) {

		for (ObjectData data : batchData) {
			try {
				if (appendStatementKeys(data, query, con).equals(true)) {
					break;
				}
			} catch (SQLException e) {
				ResponseUtil.addExceptionFailure(response, data, e);
			}

		}

	}

	/**
	 * This method will build the final query required for Update Operation based on
	 * the SET and WHERE parameters in the Requests.
	 *
	 * @param data          the data
	 * @param execStatement the query
	 * @param dataType      the data type
	 * @param con           the con
	 * @param response      the response
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void appendValuesNonBatch(ObjectData data, PreparedStatement execStatement, Map<String, String> dataType,
			Connection con, OperationResponse response) throws IOException {
		try (InputStream is = data.getData()) {
			mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
			UpdatePojo updatePojo = mapper.readValue(is, UpdatePojo.class);
			int i = 1;
			for (Set set : updatePojo.getSet()) {
				String key = set.getColumn().toUpperCase();
				if (getContext().getOperationProperties().getBooleanProperty("nestedTable", false).equals(true)) {
					if (null != set.getInnerTableValue1()) {
						i = getInnerTableValue1(execStatement, i, set);
					}
					if (null != set.getInnerTableValue2()) {
						i = getInnerTableValue2(execStatement, i, set.getInnerTableValue2());
					}
				} else {
					String[] value = set.getValue();
					this.checkDataTypeSet(dataType, key, value, execStatement, i, con);
					i++;
				}
			}
			setDataTypesOnWhereClause(execStatement, dataType, updatePojo, i);
		} catch (SQLException e) {
			ResponseUtil.addExceptionFailure(response, data, e);
		}
	}

	/**
	 * 
	 * @param execStatement
	 * @param i
	 * @param innerTableValue2
	 * @return
	 * @throws SQLException
	 */
	private int getInnerTableValue2(PreparedStatement execStatement, int i, String[] innerTableValue2) throws SQLException {
		for (int j = 0; j <= innerTableValue2.length - 1; j++) {
			if (innerTableValue2[j] != null) {
				execStatement.setString(i, innerTableValue2[j]);
				i++;
			}
		}
		return i;
	}

	/**
	 * 
	 * @param execStatement
	 * @param dataType
	 * @param updatePojo
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setDataTypesOnWhereClause(PreparedStatement execStatement, Map<String, String> dataType, UpdatePojo updatePojo, int i) throws SQLException, IOException {
		if (updatePojo.getWhere() != null) {
			for (Where where : updatePojo.getWhere()) {
				String column = where.getColumn().toUpperCase();
				String value = where.getValue();
				QueryBuilderUtil.checkDataType(execStatement, dataType, column.toUpperCase(), value, i);
				i++;
			}
		}
	}

	/**
	 * 
	 * @param execStatement
	 * @param i
	 * @param set
	 * @return
	 * @throws SQLException
	 */
	private int getInnerTableValue1(PreparedStatement execStatement, int i, Set set) throws SQLException {
		String[] value1 = set.getInnerTableValue1();
		i = getInnerTableValue2(execStatement, i, value1);
		return i;
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
	 * This method will append the ',' after each parameter in the SET clause are
	 * set.
	 *
	 * @param comma the comma
	 * @param query the query
	 */
	private void setInitial(boolean comma, StringBuilder query) {
		if (comma) {
			query.append(COMMA);
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
