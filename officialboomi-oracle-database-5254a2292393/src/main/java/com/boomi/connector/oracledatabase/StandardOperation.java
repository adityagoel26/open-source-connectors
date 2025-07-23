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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.MetadataUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.util.RequestUtil;
import com.boomi.connector.oracledatabase.util.SchemaBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.StructDescriptor;

/**
 * The Class StandardOperation.
 *
 * @author swastik.vn
 */
public class StandardOperation extends SizeLimitedUpdateOperation {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(StandardOperation.class.getName());
	
	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/**
	 * Instantiates a new standard operation.
	 *
	 * @param connection the connection
	 */
	public StandardOperation(OracleDatabaseConnection connection) {
		super(connection);
	}

	/**
	 * Execute size limited update.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		OracleDatabaseConnection conn = getConnection();
		Long batchCount = getContext().getOperationProperties().getLongProperty(BATCH_COUNT);
		String commitOption = getContext().getOperationProperties().getProperty(COMMIT_OPTION);
		String query = getContext().getOperationProperties().getProperty(QUERY);
		int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
		if (commitOption.equals(COMMIT_BY_ROWS) && batchCount != null && batchCount > 0) {
			try (Connection con = conn.getOracleConnection();
					PreparedStatement pstmnt = con.prepareStatement(query)) {
				pstmnt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
				String schemaName = getContext().getOperationProperties()
						.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
				QueryBuilderUtil.setSchemaName(con, conn, schemaName);
				Map<String, String> dataTypes = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();
				con.setAutoCommit(false);
				// We are extending SizeLimitUpdate Operation it loads only single document into
				// memory. Hence we are preparing the list of Object Data which will be required
				// for Statement batching.
				List<ObjectData> batchData = new ArrayList<>();
				for (ObjectData objdata : request) {
					batchData.add(objdata);
				}
				this.executeBatch(con, batchData, response, batchCount, pstmnt, dataTypes);

			} catch (Exception e) {
				ResponseUtil.addExceptionFailures(response, request, e);
			}
		} else if (commitOption.equals(COMMIT_BY_PROFILE) || batchCount == null || batchCount <= 0) {
			try (Connection con = conn.getOracleConnection()) {
				if (con == null) {
					throw new ConnectorException("connection failed , please check connection details");
				}
				String schemaName = getContext().getOperationProperties()
						.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
				QueryBuilderUtil.setSchemaName(con, conn, schemaName);
				Map<String, String> dataTypes = new MetadataUtil(con, getContext().getObjectTypeId()).getDataType();
				this.executeNonBatch(con, request, response, query, dataTypes);
			} catch (Exception e) {
				ResponseUtil.addExceptionFailures(response, request, e);
			}
		}

	}

	/**
	 * This method will form the statements by taking query as input parameter and
	 * executes the statement.
	 *
	 * @param con         the con
	 * @param trackedData the tracked data
	 * @param response    the response
	 * @param query       the query
	 * @param dataTypes   the data types
	 */
	private void executeNonBatch(Connection con, UpdateRequest trackedData, OperationResponse response, String query,
			Map<String, String> dataTypes) {

		for (ObjectData objdata : trackedData) {
			Payload payload = null;
			try (InputStream is = objdata.getData()) {
				Map<String, Object> userData = RequestUtil.getUserData(is);
				if (userData != null) {
					payload = executeNonBatchValue(con, response, query, dataTypes, objdata, payload, userData);
				} else if (query != null) {
					payload = executeNonBatchQuery(con, response, query, objdata, payload);
				} else {
					throw new ConnectorException("Please enter SQLQuery");
				}

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
	 * 
	 * @param con
	 * @param response
	 * @param query
	 * @param objdata
	 * @param payload
	 * @return
	 */
	private Payload executeNonBatchQuery(Connection con, OperationResponse response, String query, ObjectData objdata,
			Payload payload) {
		try (PreparedStatement stmnt = con.prepareStatement(query)) {
			int updatedRowCount = stmnt.executeUpdate();
			payload = JsonPayloadUtil
					.toPayload(new QueryResponse(query, updatedRowCount, "Executed Successfully"));
			response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
					SUCCESS_RESPONSE_MESSAGE, payload);
		} catch (IllegalArgumentException| ClassCastException e) {
			CustomResponseUtil.writeErrorResponse(e, objdata, response);
		} catch (SQLException e) {
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		}
		return payload;
	}

	/**
	 * 
	 * @param con
	 * @param response
	 * @param query
	 * @param dataTypes
	 * @param objdata
	 * @param payload
	 * @param userData
	 * @return
	 * @throws IOException
	 */
	private Payload executeNonBatchValue(Connection con, OperationResponse response, String query,
			Map<String, String> dataTypes, ObjectData objdata, Payload payload, Map<String, Object> userData)
			throws IOException {
		String finalQuery = userData.get(SQL_QUERY) == null ? query : (String) userData.get(SQL_QUERY);
		if (finalQuery != null) {
			try (PreparedStatement stmnt = con.prepareStatement(finalQuery)) {
				stmnt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0));
				this.prepareStatement(con, userData, dataTypes, stmnt, objdata);
				int updatedRowCount = stmnt.executeUpdate();
				payload = JsonPayloadUtil
						.toPayload(new QueryResponse(finalQuery, updatedRowCount, "Executed Successfully"));
				response.addResult(objdata, OperationStatus.SUCCESS, SUCCESS_RESPONSE_CODE,
						SUCCESS_RESPONSE_MESSAGE, payload);
			} catch (IllegalArgumentException | ClassCastException e) {
				CustomResponseUtil.writeErrorResponse(e, objdata, response);
			} catch (SQLException e) {
				CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
			}
		} else {
			throw new ConnectorException("Please enter SQLQuery");
		}
		return payload;
	}

	/**
	 * This method will batch the jdbc statements according to the batch count
	 * specified by the user.
	 *
	 * @param con        the con
	 * @param batchData  the tracked data
	 * @param response   the response
	 * @param batchCount the batch count
	 * @param pstmnt     the pstmnt
	 * @param dataTypes  the data types
	 * @throws SQLException
	 */
	@SuppressWarnings({"java:S3776"})
	private void executeBatch(Connection con, List<ObjectData> batchData, OperationResponse response, Long batchCount,
			PreparedStatement pstmnt, Map<String, String> dataTypes) throws SQLException {
		int b = 0;
		int batchnum = 0;
		boolean shouldExecute = true;
		for (ObjectData objdata : batchData) {
			Payload payload = null;
			b++;
			try (InputStream is = objdata.getData()) {
				// Here we are storing the Object data in MAP, Since the input request is not
				// having the fixed number of fields and Keys are unknown to extract the Json
				// Values.
				Map<String, Object> userData = RequestUtil.getUserData(is);
				if (userData != null) {
					if (userData.containsKey(SQL_QUERY)) {
						throw new ConnectorException("Commit by rows doesnt support SQLQuery field in request profile");
					}
					this.prepareStatement(con, userData, dataTypes, pstmnt, objdata);
					// Note: Here the PreparedStatement will be held in the memory. This issue has
					// been addressed in dbv2 connector. We will be informing the user to use batch
					// count less than 10 to limit the memory been held for some extent.
					pstmnt.addBatch();
					if (b == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int res[] = pstmnt.executeBatch();
							con.commit();
							response.getLogger().log(Level.INFO, BATCH_NUM, batchnum);
							response.getLogger().log(Level.INFO, BATCH_RECORDS, res.length);
							payload = JsonPayloadUtil
									.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
							ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
						} else {
							pstmnt.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, b);
							CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
						}

						b = 0;
					} else if (b < batchCount) {
						int remainingBatch = batchnum + 1;
						if (batchData.lastIndexOf(objdata) == batchData.size() - 1) {
							this.executeRemaining(objdata, pstmnt, response, remainingBatch, con, b);
						} else {
							payload = JsonPayloadUtil.toPayload(
									new BatchResponse("Record added to batch successfully", remainingBatch, b));
							ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
						}
					}
				} else {
					pstmnt.execute();
					con.commit();
					response.addResult(objdata, OperationStatus.SUCCESS, OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
							OracleDatabaseConstants.SUCCESS_RESPONSE_MESSAGE, null);

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
			} catch (IOException | IllegalArgumentException | ClassCastException e) {
				shouldExecute = this.checkLastRecord(b, batchCount);
				if (shouldExecute || batchData.lastIndexOf(objdata) == batchData.size() - 1) {
					pstmnt.clearBatch();
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
	 * @param objdata        the objdata
	 * @param pstmnt         the pstmnt
	 * @param response       the response
	 * @param remainingBatch the remaining batch
	 * @param con            the con
	 * @param b              the b
	 */
	private void executeRemaining(ObjectData objdata, PreparedStatement pstmnt, OperationResponse response,
			int remainingBatch, Connection con, int b) {

		Payload payload = null;
		try {
			int res[] = pstmnt.executeBatch();
			response.getLogger().log(Level.INFO, BATCH_NUM, remainingBatch);
			response.getLogger().log(Level.INFO, REMAINING_BATCH_RECORDS, res.length);
			payload = JsonPayloadUtil.toPayload(new BatchResponse(
					"Remaining records added to batch and executed successfully", remainingBatch, res.length));
			ResponseUtil.addSuccess(response, objdata, SUCCESS_RESPONSE_CODE, payload);
			con.commit();
		} catch (SQLException e) {
			CustomResponseUtil.logFailedBatch(response, remainingBatch, b);
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
		} finally {
			IOUtil.closeQuietly(payload);
		}

	}

	/**
	 * This method will take the input requests and set the values to the Prepared
	 * statement provided by the user.
	 *
	 * @param con       the con
	 * @param userData  the user data
	 * @param dataTypes the data types
	 * @param pstmnt    the pstmnt
	 * @param objdata   the objdata
	 * @return true if the input request exists or else false.
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void prepareStatement(Connection con, Map<String, Object> userData, Map<String, String> dataTypes,
			PreparedStatement pstmnt, ObjectData objdata) throws SQLException, IOException {

		int i = 0;
		for (Map.Entry<String, Object> entry : userData.entrySet()) {
			String key = entry.getKey();
			if (!key.equals(SQL_QUERY)) {
				i++;
				if (dataTypes.containsKey(key)) {
					if(dataTypes.get(key).equals(ARRAY)) {
 						ARRAY array = this.getArrayType(con, entry, objdata);
							pstmnt.setArray(i, array);
					}else {
						QueryBuilderUtil.checkDataType(pstmnt, dataTypes, key, entry, i);
					}
				}

			}

		}

		logger.log(Level.INFO, "Values appeneded for prepared statement");
	}

	/**
	 * This method will take the values from the input requests and prepare the
	 * ARRAY object which is required to set the Array in Prepared Statements.
	 *
	 * @param con     the con
	 * @param entry   the entry
	 * @param objdata the objdata
	 * @return
	 * @throws SQLException
	 * @throws IOException 
	 */
	private ARRAY getArrayType(Connection con, Entry<String, Object> entry, ObjectData objdata) throws SQLException, IOException {
		String typeName = this.getTypeName(con, entry.getKey());
		ArrayDescriptor des = ArrayDescriptor.createDescriptor(typeName, SchemaBuilderUtil.getUnwrapConnection(con));
		JsonNode value = getValue(objdata, entry.getKey());

		Object[] array = new Object[(int) des.getMaxLength()];
		int k = 0;
		for (int j = 1; j <= des.getMaxLength(); j++) {
			JsonNode elements = value.get(ELEMENT + j);
			if(des.getBaseName().equalsIgnoreCase(TIMESTAMP)) {
				String arr = "\""+QueryBuilderUtil.setDateTimeStampVarrayType(elements)+"\"";
				elements = mapper.readTree(arr);
			}
			if (elements != null) {
				array[k] = elements.toString().replace("\"", "");
				k++;
			}
		}
		// Eliminating null values from the array
		Object[] values = new Object[k];
		int l = 0;
		for (Object value1 : array) {
			if (value1.equals("")) {
				values[l] = null;
			} else {
				values[l] = value1;
			}
			l++;
		}

		return new ARRAY(des, SchemaBuilderUtil.getUnwrapConnection(con), values);

	}

	/**
	 * Gets the value.
	 *
	 * @param objdata the objdata
	 * @param key     the key
	 * @return the value
	 */
	private JsonNode getValue(ObjectData objdata, String key) {
		JsonNode node = null;
		try (InputStream is = objdata.getData()) {
			node = JSONUtil.getDefaultObjectMapper().readTree(is).get(key);
		} catch (Exception e) {
			throw new ConnectorException("error while getting value!!");
		}
		return node;
	}

	/**
	 * Gets the type name.
	 *
	 * @param con the con
	 * @param key the key
	 * @return the type name
	 * @throws SQLException 
	 */
	private String getTypeName(Connection con, String key) throws SQLException {
		String typeName = null;
		try (ResultSet rs = con.getMetaData().getColumns(null, null, getContext().getObjectTypeId(), null)) {
			while (rs.next()) {
				if (rs.getString(COLUMN_NAME).equals(key)) {
					typeName = con.getMetaData().getUserName() + "." + rs.getString(TYPE_NAME);
					break;
				} else if (rs.getString(DATA_TYPE).equals("2003")) {
					ArrayDescriptor des1 = ArrayDescriptor
							.createDescriptor(con.getMetaData().getUserName() + "." + rs.getString(TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
					boolean type = QueryBuilderUtil.checkArrayDataType(des1);
					if(type)
						   {
						continue;
					}
					StructDescriptor struct1 = StructDescriptor.createDescriptor(des1.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
					typeName = getNestedTypeName(con, key, typeName, struct1);
				}
			}
		} catch (SQLException e) {
			throw new ConnectorException("Error while getting Type name of the column!!");
		}
		return typeName;
	}

	/**
	 * 
	 * @param con
	 * @param key
	 * @param typeName
	 * @param struct1
	 * @return
	 * @throws SQLException
	 */
	private String getNestedTypeName(Connection con, String key, String typeName, StructDescriptor struct1)
			throws SQLException {
		for (int i = 1; i <= struct1.getMetaData().getColumnCount(); i++) {
			if (struct1.getMetaData().getColumnName(i).equals(key)) {
				typeName = struct1.getMetaData().getColumnTypeName(i);
				break;
			} else if (struct1.getMetaData().getColumnType(i) == 2003) {
				ArrayDescriptor des2 = ArrayDescriptor
						.createDescriptor(struct1.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
				StructDescriptor struct2 = StructDescriptor.createDescriptor(des2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
				for (int j = 1; j <= struct1.getMetaData().getColumnCount(); j++) {
					if (struct2.getMetaData().getColumnName(j).equals(key)) {
						typeName = struct2.getMetaData().getColumnTypeName(j);
						break;
					}
				}
			}
		}
		return typeName;
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
