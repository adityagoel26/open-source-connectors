// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
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
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.model.BatchResponse;
import com.boomi.connector.oracledatabase.model.QueryResponse;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.StructDescriptor;

/**
 * The Class DynamicInsertOperation.
 *
 * @author swastik.vn
 */
public class DynamicInsertOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(DynamicInsertOperation.class.getName());
	
	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/**
	 * Instantiates a new dynamic insert operation.
	 *
	 * @param conn the conn
	 */
	public DynamicInsertOperation(OracleDatabaseConnection conn) {
		super(conn);
	}

	/**
	 * Overridden method of SizeLimitUpdateOperation.
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
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
	}

	/**
	 * This method will identify whether batching is required based on the input and
	 * process the statements accordingly.
	 *
	 * @param con          the con
	 * @param trackedData  the tracked data
	 * @param response     the response
	 * @param batchCount   the batch count
	 * @param commitOption the commit option
	 * @throws SQLException            the SQL exception
	 * @throws JsonProcessingException the json processing exception
	 */
	public void executeStatements(Connection con, UpdateRequest trackedData, OperationResponse response,
			Long batchCount, String commitOption) throws SQLException {
		// This Map will be getting the datatype of the each column associated with the
		// table.
		MetadataUtil meta = new MetadataUtil(con, getContext().getObjectTypeId());
		Map<String, String> dataTypes = meta.getDataType();
		Map<String, String> typeNames = meta.getTypeNames();
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
		StringBuilder query = QueryBuilderUtil.buildInitialPstmntInsert(con, getContext().getObjectTypeId());
		if (batchCount != null && batchCount > 0 && commitOption.equals(COMMIT_BY_ROWS)) {
			// We are extending SizeLimitUpdate Operation it loads only single document into
			// memory. Hence we are preparing the list of Object Data which will be required
			// for Statement batching.
			List<ObjectData> batchedData = new ArrayList<>();
			for (ObjectData objdata : trackedData) {
				batchedData.add(objdata);
			}
			ExecutionParameters executionParameters = new ExecutionParameters(con, response, batchedData);
			this.doBatch(executionParameters, dataTypes, batchCount, query, typeNames,readTimeout);
		} else if (commitOption.equals(COMMIT_BY_PROFILE) || batchCount == null || batchCount == 0) {
			doCommitByProfile(con, trackedData, response, dataTypes, typeNames, readTimeout, query);

		} else if (batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative");
		}

	}

	
	/**
	 * 
	 * @param con
	 * @param trackedData
	 * @param response
	 * @param dataTypes
	 * @param typeNames
	 * @param readTimeout
	 * @param query
	 * @throws SQLException
	 */
	private void doCommitByProfile(Connection con, UpdateRequest trackedData, OperationResponse response,
			Map<String, String> dataTypes, Map<String, String> typeNames, int readTimeout, StringBuilder query)
			throws SQLException {
		Payload payload = null;
		try (PreparedStatement pstmnt = con.prepareStatement(query.toString())) {
			pstmnt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
			for (ObjectData objdata : trackedData) {
				try {
					this.buildFinalQuery(con, pstmnt, objdata, dataTypes, typeNames);
					int effectedRowCount = pstmnt.executeUpdate();
					pstmnt.clearParameters();
					payload = JsonPayloadUtil.toPayload(
							new QueryResponse(query.toString(), effectedRowCount, "Executed Successfully"));
					ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
				} catch (SQLException e) {
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (IllegalArgumentException | IOException | ClassCastException e) {
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, objdata, e);
				}
			}
		} finally {
			IOUtil.closeQuietly(payload);
		}

		try {
			con.commit();
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
	}

	
	/**
	 * 
	 * @param con
	 * @param batchedData
	 * @param response
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
		List<ObjectData> batchedData = executionParameters.getTrackedData();
		OperationResponse response = executionParameters.getResponse();
		ObjectData objdata = executionParameters.getObjdata();
		Payload payload =executionParameters.getPayload();

		int remainingBatch = batchnum + 1;
		if (batchedData.lastIndexOf(objdata) == batchedData.size() - 1) {
			this.executeRemaining(objdata, bstmnt, response, remainingBatch, con, b);
		} else {
			payload = JsonPayloadUtil.toPayload(
					new BatchResponse("Record added to batch successfully", remainingBatch, b));
			ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
		}
		return payload;
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param con         the con
	 * @param dataTypes   the data types
	 * @param batchCount  the batch count
	 * @param batchedData the batched data
	 * @param response    the response
	 * @param query       the query
	 * @param typeNames type Names
	 * @param readTimeout readTimeout
	 */
	@SuppressWarnings({"java:S1141","java:S3776"})
	private void doBatch(ExecutionParameters executionParameters, Map<String, String> dataTypes, Long batchCount,
			StringBuilder query, Map<String, String> typeNames, int readTimeout) {
		Connection con = executionParameters.getCon();
		OperationResponse response = executionParameters.getResponse();
		List<ObjectData> batchedData = executionParameters.getTrackedData();

		int batchnum = 0;
		int b = 0;
		boolean shouldExecute = true;
		// Note: Here the PreparedStatement will be held in the memory. This issue has
		// been addressed in dbv2 connector. We will be informing the user to use batch
		// count less than 10 to limit the memory been held for some extent.
		try (PreparedStatement bstmnt = con.prepareStatement(query.toString())) {
			bstmnt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
			for (ObjectData objdata : batchedData) {
				b++;
				Payload payload = null;
				try {
					this.buildFinalQuery(con, bstmnt, objdata, dataTypes, typeNames);
					bstmnt.addBatch();
					if (b == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int[] res = bstmnt.executeBatch();
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
						ExecutionParameters executionParametersForRemainingBatchValue = new ExecutionParameters(con,
								response, batchedData, objdata, payload);
						payload = executeRemainingBatchValue(executionParametersForRemainingBatchValue, batchnum, b, bstmnt);
					}
				} catch (BatchUpdateException e) {

					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
					b = 0;
				} catch (SQLException e) {
					CustomResponseUtil.logFailedBatch(response, batchnum, b);
					shouldExecute = this.checkLastRecord(b, batchCount);
					if (shouldExecute) {
						b = 0;
					}
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				}
				catch (IOException | IllegalArgumentException | ClassCastException e) {
					shouldExecute = this.checkLastRecord(b, batchCount);
					if (shouldExecute || batchedData.lastIndexOf(objdata) == batchedData.size() - 1) {
						bstmnt.clearBatch();
						batchnum++;
						CustomResponseUtil.logFailedBatch(response, batchnum, b);
						b = 0;
					}
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, objdata, e);
				} finally {
					IOUtil.closeQuietly(payload);
				}
			}

			logger.info("Batching statements processed Successfully!!");
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

	}

	/**
	 * This method will get the values from the input request and Sets the value to
	 * the Prepared Statements in the Base Table.
	 *
	 * @param con       the con
	 * @param bstmnt    the bstmnt
	 * @param objdata   the objdata
	 * @param dataTypes the data types
	 * @param typeNames
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void buildFinalQuery(Connection con, PreparedStatement bstmnt, ObjectData objdata,
			Map<String, String> dataTypes, Map<String, String> typeNames) throws IOException, SQLException {
		JsonNode json = null;
		try (InputStream is = objdata.getData()) {
			// After filtering out the inputs (which are more than 1MB) we are loading the
			// inputstream to memory here.
			json = mapper.readTree(is);
			if (json != null) {
				int i= 0;
				for (Map.Entry<String, String> entries : typeNames.entrySet()) {
					i++;
					String key = entries.getKey();
					JsonNode fieldName = json.get(key);
					if (dataTypes.containsKey(key)) {
						buildFinalQueryValue(con, bstmnt, dataTypes, i, entries, key, fieldName);
					}

				}
			} else {
				throw new ConnectorException("Please check the input data!!");
			}
		}
	}

	
	/**
	 * 
	 * @param con
	 * @param bstmnt
	 * @param dataTypes
	 * @param i
	 * @param entries
	 * @param key
	 * @param fieldName
	 * @throws SQLException
	 * @throws IOException
	 */
	private void buildFinalQueryValue(Connection con, PreparedStatement bstmnt, Map<String, String> dataTypes, int i,
			Map.Entry<String, String> entries, String key, JsonNode fieldName) throws SQLException, IOException {
		if (dataTypes.get(key).equals(ARRAY)) {
			if (fieldName != null) {
				this.iterrateOverNestedTable2(fieldName, bstmnt, con, i, entries.getValue());
			} else {
				bstmnt.setNull(i, Types.ARRAY, entries.getValue());
			}
		} else {
			QueryBuilderUtil.checkDataType(bstmnt, dataTypes, key, fieldName, i);
		}
	}

	/**
	 * This method will iterate over Level 2 nested tables and fetch the values from
	 * each column and adds the value to the prepared Statement.
	 *
	 * @param fieldName the field name
	 * @param bstmnt    the bstmnt
	 * @param con       the con
	 * @param i         the i
	 * @param typeName  the type name
	 * @return 
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private void iterrateOverNestedTable2(JsonNode fieldName, PreparedStatement bstmnt, Connection con, int i,
			String typeName) throws SQLException, IOException {
		ArrayDescriptor arrayLevel1 = ArrayDescriptor.createDescriptor(con.getMetaData().getUserName() + DOT + typeName,
				SchemaBuilderUtil.getUnwrapConnection(con));
		boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel1);
		if(type)
		{
			ARRAY array = this.iterateOverVarray(con, fieldName, arrayLevel1);
			bstmnt.setArray(i, array);
		} else {
			StructDescriptor structLevel1 = StructDescriptor.createDescriptor(arrayLevel1.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
			ResultSetMetaData rsmd1 = structLevel1.getMetaData();
			for (int j = 1; j <= rsmd1.getColumnCount(); j++) {
				JsonNode level1 = fieldName.get(rsmd1.getColumnName(j));
				switch (rsmd1.getColumnType(j)) {
				case 2:
				case 4:
				case 3:
					setIntType(bstmnt, i, level1);
					break;
				case 91:
					setDateType(bstmnt, i, level1);
					break;
				case 12:
					setVarcharType(bstmnt, i, level1);
					break;
				case 1:
				case -15:
					setCharType(bstmnt, i, level1);
					break;
				case 92:
					setTimeType(bstmnt, i, level1);
					break;
				case -9:
					setNvarcharType(bstmnt, i, level1);
					break;
				case 16:
					setBooleanType(bstmnt, i, level1);
					break;
				case 6:
					setFloatType(bstmnt, i, level1);
					break;
				case 8:
					setDoubleType(bstmnt, i, level1);
					break;
				case -5:
					setBigintType(bstmnt, i, level1);
					break;
				case 2004:
					setBlobType(bstmnt, i, level1);
					break;
				case 93:
					setTimestampType(bstmnt, i, level1);
					break;
				case 2003:
					this.iterrateOverNestedTable3(level1, bstmnt, rsmd1, con, i, j);
					break;

				default:
					break;
				}

				i++;
			}
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 */
	private void setTimestampType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.TIMESTAMP);
		} else {
			String timeStamp =QueryBuilderUtil.timeStampNestedType(level1);
			bstmnt.setString(i, timeStamp);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setBlobType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException, IOException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.BLOB);
		} else {
			String value = level1.toString().replace(BACKSLASH, "");
			try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
				bstmnt.setBlob(i, stream);
			}
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 */
	private void setBigintType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.BIGINT);
		} else {
			long value = Long.parseLong(level1.toString().replace(BACKSLASH, ""));
			bstmnt.setLong(i, value);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 */
	private void setDoubleType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.DOUBLE);
		} else {
			double value = Double.parseDouble(level1.toString().replace(BACKSLASH, ""));
			bstmnt.setDouble(i, value);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 */
	private void setFloatType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.FLOAT);
		} else {
			float value = Float.parseFloat(level1.toString().replace(BACKSLASH, ""));
			bstmnt.setFloat(i, value);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 */
	private void setBooleanType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.BOOLEAN);
		} else {
			Boolean flag = Boolean.valueOf(level1.toString().replace(BACKSLASH, ""));
			bstmnt.setBoolean(i, flag);
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level1
	 * @throws SQLException
	 */
	private void setNvarcharType(PreparedStatement bstmnt, int i, JsonNode level1) throws SQLException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.NVARCHAR);
		} else {
			bstmnt.setString(i, StringEscapeUtils.unescapeJava(level1.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * This method will iterate over Level 3 nested tables and fetch the values from
	 * each column and adds the value to the prepared Statement. If there is a
	 * nested table above Level 3 this method will throw Connector Exception.
	 *
	 * @param level1 the level 1
	 * @param bstmnt the bstmnt
	 * @param rsmd1  the rsmd 1
	 * @param con    the con
	 * @param i      the i
	 * @param j      the j
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private void iterrateOverNestedTable3(JsonNode level1, PreparedStatement bstmnt, ResultSetMetaData rsmd1,
			Connection con, int i, int j) throws SQLException, IOException {
		if (level1 == null) {
			bstmnt.setNull(i, Types.ARRAY, rsmd1.getColumnTypeName(j));
		} else {
			ArrayDescriptor arrayLevel2 = ArrayDescriptor.createDescriptor(rsmd1.getColumnTypeName(j), SchemaBuilderUtil.getUnwrapConnection(con));
			boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel2);
			if(type)
			 {
				ARRAY arr = this.iterateOverVarray(con, level1, arrayLevel2);
				bstmnt.setArray(i, arr);
			} else {
				StructDescriptor structLevel2 = StructDescriptor.createDescriptor(arrayLevel2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
				ResultSetMetaData rsmd2 = structLevel2.getMetaData();
				for (int k = 1; k <= rsmd2.getColumnCount(); k++) {
					i = iterrateOverNestedTable3Values(level1, bstmnt, con, i, rsmd2, k);
				}

			}
		}

	}

	
	/**
	 * 
	 * @param level1
	 * @param bstmnt
	 * @param con
	 * @param i
	 * @param rsmd2
	 * @param k
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private int iterrateOverNestedTable3Values(JsonNode level1, PreparedStatement bstmnt, Connection con, int i,
			ResultSetMetaData rsmd2, int k) throws SQLException, IOException {
		JsonNode level2 = level1.get(rsmd2.getColumnName(k));
		switch (rsmd2.getColumnType(k)) {
		case 2:
		case 4:
		case 3:
			setIntType(bstmnt, i, level2);
			break;
		case 91:
			setDateType(bstmnt, i, level2);
			break;
		case 12:
			setVarcharType(bstmnt, i, level2);
			break;
		case 1:
			setCharType(bstmnt, i, level2);
			break;
		case 92:
			setTimeType(bstmnt, i, level2);
			break;
		case -9:
			setNvarcharType(bstmnt, i, level2);
			break;
		case 16:
			setBooleanType(bstmnt, i, level2);
			break;
		case 6:
			setFloatType(bstmnt, i, level2);
			break;
		case 8:
			setDoubleType(bstmnt, i, level2);
			break;
		case -5:
			setBigintType(bstmnt, i, level2);
			break;
		case 2004:
			setBlobType(bstmnt, i, level2);
			break;
		case 93:
			setTimestampType(bstmnt, i, level2);
			break;
		case 2003:
			ArrayDescriptor arrayLevel3 = ArrayDescriptor.createDescriptor(rsmd2.getColumnTypeName(k), SchemaBuilderUtil.getUnwrapConnection(con));
			boolean type3 = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
			if(type3)
			  {
					if (level2 == null) {
						bstmnt.setNull(i, Types.ARRAY, rsmd2.getColumnTypeName(k));
					} else {
						ARRAY arr = this.iterateOverVarray(con, level2, arrayLevel3);
						bstmnt.setArray(i, arr);
					}
			} else {
				throw new ConnectorException("Nested Level Exhausted!!!");
			}
			break;
		default:
			break;
		}
		i++;
		return i;
	}

	
	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level2
	 * @throws SQLException
	 */
	private void setTimeType(PreparedStatement bstmnt, int i, JsonNode level2) throws SQLException {
		if (level2 == null) {
			bstmnt.setNull(i, Types.TIME);
		} else {
			String time = level2.toString().replace(BACKSLASH, "");
			bstmnt.setTime(i, Time.valueOf(time));
		}
	}

	
	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level2
	 * @throws SQLException
	 */
	private void setCharType(PreparedStatement bstmnt, int i, JsonNode level2) throws SQLException {
		if (level2 == null) {
			bstmnt.setNull(i, Types.CHAR);
		} else {
			bstmnt.setString(i, level2.toString().replace(BACKSLASH, ""));
		}
	}

	
	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level2
	 * @throws SQLException
	 */
	private void setVarcharType(PreparedStatement bstmnt, int i, JsonNode level2) throws SQLException {
		if (level2 == null) {
			bstmnt.setNull(i, Types.VARCHAR);
		} else {
			bstmnt.setString(i, level2.toString().replace(BACKSLASH, ""));
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level2
	 * @throws SQLException
	 */
	private void setDateType(PreparedStatement bstmnt, int i, JsonNode level2) throws SQLException {
		if (level2 == null) {
			bstmnt.setNull(i, Types.DATE);
		} else {
			bstmnt.setString(i, level2.toString().replace(BACKSLASH, ""));
		}
	}

	/**
	 * 
	 * @param bstmnt
	 * @param i
	 * @param level2
	 * @throws SQLException
	 */
	private void setIntType(PreparedStatement bstmnt, int i, JsonNode level2) throws SQLException {
		if (level2 == null) {
			bstmnt.setNull(i, Types.INTEGER);
		} else {
			BigDecimal num = new BigDecimal(level2.toString().replace(BACKSLASH, ""));
			bstmnt.setBigDecimal(i, num);
		}
	}

	/**
	 * This method will iterate over VARRAY elements and return the Array object
	 * which is required to set the values for prepared statement.
	 *
	 * @param con         the con
	 * @param fieldName   the field name
	 * @param arrayLevel3 the array level 3
	 * @return the array type
	 * @throws SQLException the SQL exception
	 * @throws IOException 
	 */
	private ARRAY iterateOverVarray(Connection con, JsonNode fieldName, ArrayDescriptor arrayDes) throws SQLException, IOException {

		Object[] array = new Object[(int) arrayDes.getMaxLength()];
		int k = 0;
		for (int j = 1; j <= arrayDes.getMaxLength(); j++) {
			JsonNode elements = fieldName.get(ELEMENT + j);
			if(arrayDes.getBaseName().equalsIgnoreCase(TIMESTAMP)) {
				String arr = "\""+QueryBuilderUtil.setDateTimeStampVarrayType(elements)+"\"";
				elements = mapper.readTree(arr);
			}
			if (elements != null && !elements.toString().replace(BACKSLASH, "").equals("")) {
				array[k++] = elements.toString().replace(BACKSLASH, "");
			} else {
				array[k++] = null;
			}
		}
		return new ARRAY(arrayDes, SchemaBuilderUtil.getUnwrapConnection(con), array);

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
	 * Gets the Connection instance.
	 *
	 * @return the connection
	 */
	@Override
	public OracleDatabaseConnection getConnection() {
		return (OracleDatabaseConnection) super.getConnection();
	}

}