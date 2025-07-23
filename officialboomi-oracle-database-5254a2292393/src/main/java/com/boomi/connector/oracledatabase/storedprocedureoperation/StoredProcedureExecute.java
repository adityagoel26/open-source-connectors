// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.storedprocedureoperation;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.text.StringEscapeUtils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.model.BatchResponse;
import com.boomi.connector.oracledatabase.model.ErrorDetails;
import com.boomi.connector.oracledatabase.model.ProcedureResponseNonBatch;
import com.boomi.connector.oracledatabase.util.CustomPayloadUtil;
import com.boomi.connector.oracledatabase.util.CustomResponseUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.ProcedureMetaDataUtil;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.util.SchemaBuilderUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.Datum;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import oracle.xdb.XMLType;

/**
 * The Class StoredProcedureExecute.
 *
 * @author swastik.vn
 */
public class StoredProcedureExecute extends ProcedureMetaDataUtil {

	/** The List of Parameters present in the procedure. */
	List<String> params;

	/** The List of only IN Parameters present in the procedure. */
	List<String> inParams;

	/** The List of only OUT parameters present in the procedure. */
	List<String> outParams;

	/** The data type. */
	Map<String, Integer> dataType;

	/** The tracked data. */
	UpdateRequest trackedData;

	/** The response. */
	OperationResponse response;

	/** The con. */
	Connection con;

	/** The procedure name. */
	String procedureName;

	/** The procedure name. */
	String procedureNameWithPackage;

	/** The operation context. */
	OperationContext operationContext;

	/** The mapper. */
	ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/** The Constant JSON_FACTORY. */
	private static final JsonFactory JSON_FACTORY = new JsonFactory();

	/** The Constant TABLETYPE. */
	private static final int TABLETYPE = 2010;

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(StoredProcedureExecute.class.getName());

	/**
	 * Instantiates a new stored procedure helper.
	 *
	 * @param con              the database Connection Object
	 * @param procedureName    the procedure name
	 * @param trackedData      the tracked data
	 * @param response         the response
	 * @param operationContext the operation context
	 * @throws SQLException
	 */
	public StoredProcedureExecute(Connection con, String procedureName, String procedureNameWithPackage,
			UpdateRequest trackedData, OperationResponse response, OperationContext operationContext)
			throws SQLException {
		super(con, procedureName, procedureNameWithPackage);
		this.params = getParams();
		this.inParams = getInParams();
		this.outParams = getOutParams();
		this.dataType = getDataType();
		this.trackedData = trackedData;
		this.response = response;
		this.con = con;
		this.procedureName = procedureName;
		this.procedureNameWithPackage = procedureNameWithPackage;
		this.operationContext = operationContext;

	}

	/**
	 * This method will create the Callable statement and provide the neccessary
	 * parameters from both inside and outside the package and execute the
	 * statements.
	 *
	 * @param batchCount   the batch count
	 * @param maxFieldSize the max field size
	 * @param readTimeout
	 * @param fetchSize
	 * @throws SQLException the SQL exception
	 */
	public void executeStatements(Long batchCount, Long maxFieldSize, int readTimeout, Long fetchSize)
			throws SQLException {
		String fun = null;
		StringBuilder query = null;
		String packageName = SchemaBuilderUtil.getProcedurePackageName(procedureNameWithPackage);
		String schema = null;
		if (packageName != null) {
			schema = packageName;
		} else {
			schema = con.getCatalog();
		}
		try (ResultSet resultSet = con.getMetaData().getProcedures(schema, con.getSchema(), procedureName);) {
			while (resultSet.next()) {
				if (resultSet.getString(PROCEDURE_TYPE) != null) {
					fun = resultSet.getString(PROCEDURE_TYPE);
				}
				if (fun != null && fun.equals("1")) {
					query = QueryBuilderUtil.buildProcedureQuery(params, procedureNameWithPackage);
				} else if (fun != null && fun.equals("2")) {
					query = QueryBuilderUtil.buildFunctionQuery(params, procedureNameWithPackage);
				}
			}
		}
		if (batchCount != null && batchCount > 0) {
			doBatchValue(batchCount, readTimeout, query);

		} else if (batchCount == null || batchCount == 0) {
			doWithoutBatch(maxFieldSize, readTimeout, fetchSize, query);

		} else if (batchCount < 0) {
			throw new ConnectorException("Batch count cannot be negative!!");
		}

	}

	/**
	 * 
	 * @param maxFieldSize
	 * @param readTimeout
	 * @param fetchSize
	 * @param query
	 * @throws SQLException
	 */
	private void doWithoutBatch(Long maxFieldSize, int readTimeout, Long fetchSize, StringBuilder query)
			throws SQLException {
		try (OracleCallableStatement csmt = (OracleCallableStatement) SchemaBuilderUtil.getUnwrapConnection(con).prepareCall(query.toString())) {
			csmt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
			this.registerParams(csmt);
			for (ObjectData objdata : trackedData) {
				try (InputStream is = objdata.getData();) {
					if (!inParams.isEmpty()) {
						this.prepareStatements(csmt, is);
					}
					this.callProcedure(csmt, objdata, maxFieldSize, fetchSize);
				} catch (IOException | IllegalArgumentException | ClassCastException e) {
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} catch (SQLException e) {
					CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);
				} catch (ConnectorException e) {
					ResponseUtil.addExceptionFailure(response, objdata, e);
				}

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
	 * @param batchCount
	 * @param readTimeout
	 * @param query
	 * @throws SQLException
	 */
	private void doBatchValue(Long batchCount, int readTimeout, StringBuilder query) throws SQLException {
		if (!inParams.isEmpty() && outParams.isEmpty()) {
			// We are extending SizeLimitUpdate Operation it loads only single document into
			// memory. Hence we are preparing the list of Object Data which will be required
			// for Statement batching.
			List<ObjectData> batchData = new ArrayList<>();
			for (ObjectData objdata : trackedData) {
				batchData.add(objdata);
			}
			this.doBatch(batchCount, query, batchData, readTimeout);
		} else {
			throw new ConnectorException("Batching cannot be applied for non input parameter procedures");
		}
	}

	/**
	 * This method will register the out parameters for the Callable statement
	 *
	 * @param csmt the Callable Statement
	 * @throws SQLException the SQL exception
	 */
	private void registerParams(OracleCallableStatement csmt) throws SQLException {

		if (!params.isEmpty()) {
			for (int i = 1; i <= params.size(); i++) {
				registerParamsValue(csmt, i);
			}
		}

	}

	/**
	 * 
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void registerParamsValue(OracleCallableStatement csmt, int i) throws SQLException {
		if (outParams.contains(params.get(i - 1)) && dataType.get(params.get(i - 1)).equals(2012)) {
			csmt.registerOutParameter(params.indexOf(params.get(i - 1)) + 1, OracleTypes.CURSOR);
		} else if (outParams.contains(params.get(i - 1)) && (dataType.get(params.get(i - 1)).equals(2003)
				|| dataType.get(params.get(i - 1)).equals(2010))) {
			String typeName = QueryBuilderUtil.getTypeName(con, procedureName, params.get(i - 1));
			csmt.registerOutParameter(params.indexOf(params.get(i - 1)) + 1, 2003, typeName);
		} else if (outParams.contains(params.get(i - 1)) && dataType.get(params.get(i - 1)).equals(2002)) {
			String typeName = QueryBuilderUtil.getTypeName(con, procedureName, params.get(i - 1));
			csmt.registerOutParameter(params.indexOf(params.get(i - 1)) + 1, 2002, typeName);
		} else if (outParams.contains(params.get(i - 1)) && dataType.get(params.get(i - 1)).equals(2009)) {
			csmt.registerOutParameter(params.indexOf(params.get(i - 1)) + 1, Types.SQLXML);
		} else if (outParams.contains(params.get(i - 1)) && !dataType.get(params.get(i - 1)).equals(2003)
				&& !dataType.get(params.get(i - 1)).equals(2012)) {
			csmt.registerOutParameter(params.indexOf(params.get(i - 1)) + 1, dataType.get(params.get(i - 1)));
		}
	}

	/**
	 * Do batch.
	 *
	 * @param batchCount  the batch count
	 * @param query       the query
	 * @param batchData   the batch data
	 * @param readTimeout
	 * @throws SQLException the SQL exception
	 */
	@SuppressWarnings({"java:S3776"})
	private void doBatch(Long batchCount, StringBuilder query, List<ObjectData> batchData, int readTimeout)
			throws SQLException {
		int batchnum = 0;
		int b = 0;
		boolean shouldExecute = true;
		// Note: Here the CallableStatement will be held in the memory. This issue has
		// been addressed in dbv2 connector. We will be informing the user to use batch
		// count less than 10 to limit the memory been held for some extent.
		try (CallableStatement csmt = SchemaBuilderUtil.getUnwrapConnection(con).prepareCall(query.toString())) {
			csmt.setQueryTimeout(QueryBuilderUtil.convertMsToSeconds(readTimeout));
			for (ObjectData objdata : batchData) {
				Payload payload = null;
				b++;
				try (InputStream is = objdata.getData();) {
					this.prepareStatements((OracleCallableStatement) csmt, is);
					csmt.addBatch();
					if (b == batchCount) {
						batchnum++;
						if (shouldExecute) {
							int[] res = csmt.executeBatch();
							con.commit();
							csmt.clearBatch();
							response.getLogger().log(Level.INFO, OracleDatabaseConstants.BATCH_NUM, batchnum);
							response.getLogger().log(Level.INFO, OracleDatabaseConstants.BATCH_RECORDS, res.length);
							payload = JsonPayloadUtil
									.toPayload(new BatchResponse("Batch executed successfully", batchnum, res.length));
							ResponseUtil.addSuccess(response, objdata, OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
									payload);
						} else {
							csmt.clearBatch();
							shouldExecute = true;
							CustomResponseUtil.logFailedBatch(response, batchnum, b);
							CustomResponseUtil.batchExecuteError(objdata, response, batchnum, b);
						}
						b = 0;
					} else if (b < batchCount) {
						payload = executeRemainingBatch(batchData, batchnum, b, csmt, objdata, payload);

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
						csmt.clearBatch();
						batchnum++;
						CustomResponseUtil.logFailedBatch(response, batchnum, b);
						b = 0;
					}
					CustomResponseUtil.writeErrorResponse(e, objdata, response);
				} finally {
					IOUtil.closeQuietly(payload);
				}
			}
		}

	}

	/**
	 * 
	 * @param batchData
	 * @param batchnum
	 * @param b
	 * @param csmt
	 * @param objdata
	 * @param payload
	 * @return
	 */
	private Payload executeRemainingBatch(List<ObjectData> batchData, int batchnum, int b, CallableStatement csmt,
										  ObjectData objdata, Payload payload) {
		int remainingBatch = batchnum + 1;
		if (batchData.lastIndexOf(objdata) == batchData.size() - 1) {
			this.executeRemaining(objdata, csmt, remainingBatch, b);
		} else {
			payload = JsonPayloadUtil.toPayload(
					new BatchResponse("Record added to batch successfully", remainingBatch, b));
			ResponseUtil.addSuccess(response, objdata, OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
					payload);
		}
		return payload;
	}

	/**
	 * This method will provide the necessary parameters required for the Callable
	 * statement based on the incoming requests.
	 *
	 * @param csmt the Callable Statement
	 * @param is   the input(inputstream)
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void prepareStatements(OracleCallableStatement csmt, InputStream is) throws SQLException, IOException {
		JsonNode json = null;
		if (is.available() != 0) {
			// After filtering out the inputs (which are more than 1MB) we are loading the
			json = mapper.readTree(is);
			for (int i = 1; i <= inParams.size(); i++) {
				JsonNode node = json.get(inParams.get(i - 1));
				switch (dataType.get(inParams.get(i - 1))) {
				case Types.VARCHAR:
				case Types.CHAR:
				case Types.LONGVARCHAR:
				case Types.NCHAR:
				case Types.LONGNVARCHAR:
					setStringDataType(node, csmt, i);
					break;
				case Types.TIMESTAMP:
					setDateTimStampDataType(node, csmt, i);
					break;
				case Types.DATE:
					setDateDataType(node, csmt, i);
					break;
				case Types.NVARCHAR:
					setNvarcharDataType(node, csmt, i);
					break;
				case Types.CLOB:
					setClobDataType(node, csmt, i);
					break;
				case Types.INTEGER:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.DECIMAL:
				case Types.NUMERIC:
					setNumericDataType(node, csmt, i);
					break;
				case Types.TIME:
					setTimeDataType(node, csmt, i);
					break;
				case Types.BOOLEAN:
				case Types.BIT:
					setBooleanDataType(node, csmt, i);
					break;
				case Types.BIGINT:
					setBigIntDataType(node, csmt, i);
					break;
				case Types.DOUBLE:
				case Types.FLOAT:
					setDoubleDataType(node, csmt, i);
					break;
				case Types.REAL:
					setRealDataType(node, csmt, i);
					break;
				case Types.BLOB:
				case Types.BINARY:
				case Types.LONGVARBINARY:
				case Types.VARBINARY:
					setBlobDataType(node, csmt, i);
					break;
				case Types.ARRAY:
					setArrayDataType(node, csmt, i);
					break;
				case Types.SQLXML:
					if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
						csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.SQLXML, "XMLTYPE");
					} else {
						String xml = node.toString().replace(BACKSLASH, "");
						String jsonSt = unEscapeString(xml);
						XMLType x = XMLType.createXML(con, jsonSt);
						csmt.setSQLXML(params.indexOf(inParams.get(i - 1)) + 1, x);
					}
					break;
				case Types.STRUCT:
					setStructType(csmt, i, node);
					break;
				case TABLETYPE:
					setTableType(csmt, i, node);
					break;
				default:
					break;
				}
			}

		}

	}

	/**
	 * 
	 * @param csmt
	 * @param i
	 * @param node
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setTableType(OracleCallableStatement csmt, int i, JsonNode node) throws SQLException, IOException {
		if (node == null) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.ARRAY,
					QueryBuilderUtil.getTypeName(con, procedureName, inParams.get(i - 1)));
		} else {
			csmt.setARRAY(params.indexOf(inParams.get(i - 1)) + 1,
					this.getStructData(node, inParams.get(i - 1)));
		}
	}

	/**
	 * 
	 * @param csmt
	 * @param i
	 * @param node
	 * @throws SQLException
	 */
	private void setStructType(OracleCallableStatement csmt, int i, JsonNode node) throws SQLException {
		if (node == null) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.STRUCT,
					QueryBuilderUtil.getTypeName(con, procedureName, inParams.get(i - 1)));
		} else {
			csmt.setSTRUCT(params.indexOf(inParams.get(i - 1)) + 1,
					this.getStructObjectData(node, inParams.get(i - 1)));
		}
	}

	/**
	 * This method will build the Struct data required for object type parameters in
	 * Stored procedure from the Json node.
	 *
	 * @param node     Json input
	 * @param argument parameter name of the SP
	 * @return the STRUCT
	 * @throws SQLException the SQL exception
	 * @throws SQLException
	 */
	private STRUCT getStructObjectData(JsonNode node, String argument) throws SQLException {
		StructDescriptor structDesc = StructDescriptor.createDescriptor(
				QueryBuilderUtil.getTypeName(con, procedureName, argument), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDesc.getMetaData();
		Object[] obj11 = new Object[md.getColumnCount()];
		for (int k = 1; k <= md.getColumnCount(); k++) {
			getStructObjectData2Value(node, md, obj11, k);

		}
		return new STRUCT(structDesc, SchemaBuilderUtil.getUnwrapConnection(con), obj11);
	}

	/**
	 * 
	 * @param node
	 * @param md
	 * @param obj11
	 * @param k
	 * @throws SQLException
	 */
	@SuppressWarnings({"java:S3776"})
	private void getStructObjectData2Value(JsonNode node, ResultSetMetaData md, Object[] obj11, int k)
			throws SQLException {
		JsonNode child = node.get(md.getColumnName(k));
		if (child == null || child.toString().equals("null")
				|| child.toString().replace(BACKSLASH, "").equals("")) {
			obj11[k - 1] = null;
		}

		else if (NUMBER.equalsIgnoreCase(md.getColumnTypeName(k))
				|| INTEGER.equalsIgnoreCase(md.getColumnTypeName(k))
				|| TINYINT.equalsIgnoreCase(md.getColumnTypeName(k))
				|| SMALLINT.equalsIgnoreCase(md.getColumnTypeName(k))
				|| DECIMAL.equalsIgnoreCase(md.getColumnTypeName(k))
				|| NUMERIC.equalsIgnoreCase(md.getColumnTypeName(k))) {
			BigDecimal num = new BigDecimal(child.toString().replace(BACKSLASH, ""));
			obj11[k - 1] = num;
		} else if (BOOLEAN.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = Boolean.valueOf(child.toString().replace(BACKSLASH, ""));
		} else if (VARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
				|| CHAR.equalsIgnoreCase(md.getColumnTypeName(k))
				|| LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
				|| NCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
				|| LONGNVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = child.toString().replace(BACKSLASH, "");
		} else if (NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = (StringEscapeUtils.unescapeJava(child.toString()).replace(BACKSLASH, ""));
		} else if (TIME.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = Time.valueOf(child.toString().replace(BACKSLASH, ""));
		} else if (BIGINT.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = Long.parseLong(child.toString().replace(BACKSLASH, ""));
		} else if (DOUBLE.equalsIgnoreCase(md.getColumnTypeName(k))
				|| FLOAT.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = Double.parseDouble(child.toString().replace(BACKSLASH, ""));
		} else if (REAL.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = Float.parseFloat(child.toString().replace(BACKSLASH, ""));
		} else if (BLOB.equalsIgnoreCase(md.getColumnTypeName(k))
				|| BINARY.equalsIgnoreCase(md.getColumnTypeName(k))
				|| LONGVARBINARY.equalsIgnoreCase(md.getColumnTypeName(k))
				|| VARBINARY.equalsIgnoreCase(md.getColumnTypeName(k))) {
			String blobData = child.toString().replace(BACKSLASH, "");
			byte[] byteContent = blobData.getBytes();
			Blob blob = con.createBlob();// Where connection is the connection to db object.
			blob.setBytes(1, byteContent);
			obj11[k - 1] = blob;
		} else if (RAW.equalsIgnoreCase(md.getColumnTypeName(k))) {

			String blobData = child.toString().replace(BACKSLASH, "");
			byte[] byteContent = blobData.getBytes();
			obj11[k - 1] = byteContent;

		} else if (DATE.equalsIgnoreCase(md.getColumnTypeName(k))
				|| TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(k))) {
			obj11[k - 1] = Timestamp.valueOf(QueryBuilderUtil.setDateTimeStampVarrayType(child));
		}
	}

	/**
	 * This method will build the Struct data required for table type parameters in
	 * Stored procedure from the Json node.
	 *
	 * @param node     Json input
	 * @param argument parameter name of the SP
	 * @return the ARRAY
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private ARRAY getStructData(JsonNode node, String argument) throws SQLException, IOException {
		ArrayDescriptor arrayDesc = ArrayDescriptor.createDescriptor(
				QueryBuilderUtil.getTypeName(con, procedureName, argument), SchemaBuilderUtil.getUnwrapConnection(con));
		if (VARCHAR.equalsIgnoreCase(arrayDesc.getBaseName()) || DATE.equalsIgnoreCase(arrayDesc.getBaseName())
				|| CHAR.equalsIgnoreCase(arrayDesc.getBaseName()) || NCHAR.equalsIgnoreCase(arrayDesc.getBaseName())
				|| CLOB.equalsIgnoreCase(arrayDesc.getBaseName()) || TIME.equalsIgnoreCase(arrayDesc.getBaseName())
				|| DOUBLE.equalsIgnoreCase(arrayDesc.getBaseName()) || FLOAT.equalsIgnoreCase(arrayDesc.getBaseName())
				|| DECIMAL.equalsIgnoreCase(arrayDesc.getBaseName())
				|| NUMERIC.equalsIgnoreCase(arrayDesc.getBaseName()) || REAL.equalsIgnoreCase(arrayDesc.getBaseName())
				|| BLOB.equalsIgnoreCase(arrayDesc.getBaseName()) || TIMESTAMP.equalsIgnoreCase(arrayDesc.getBaseName())
				|| INTEGER.equalsIgnoreCase(arrayDesc.getBaseName())
				|| SMALLINT.equalsIgnoreCase(arrayDesc.getBaseName())
				|| NUMBER.equalsIgnoreCase(arrayDesc.getBaseName())) {
			return setPrimitiveArray(con, node, arrayDesc);

		} else if (arrayDesc.getBaseType() == 2002) {
			STRUCT[] structArray = new STRUCT[node.size()];
			StructDescriptor structDesc = StructDescriptor.createDescriptor(arrayDesc.getBaseName(),
					SchemaBuilderUtil.getUnwrapConnection(con));
			ResultSetMetaData md = structDesc.getMetaData();
			for (int i = 0; i < node.size(); i++) {
				JsonNode child = node.get(i);
				Object[] obj = new Object[md.getColumnCount()];
				for (int j = 1; j <= md.getColumnCount(); j++) {
					if (child.get(md.getColumnName(j)) == null
							|| child.get(md.getColumnName(j)).toString().equals("null")
							|| child.get(md.getColumnName(j)).toString().replace(BACKSLASH, "").equals("")) {
						obj[j - 1] = null;
					} else {
						setStructDataValue(structDesc, md, child, obj, j);
					}
				}
				structArray[i] = new STRUCT(structDesc, SchemaBuilderUtil.getUnwrapConnection(con), obj);
			}

			return new ARRAY(arrayDesc, SchemaBuilderUtil.getUnwrapConnection(con), structArray);
		}
		return null;
	}

	/**
	 * 
	 * @param structDesc
	 * @param md
	 * @param child
	 * @param obj
	 * @param j
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setStructDataValue(StructDescriptor structDesc, ResultSetMetaData md, JsonNode child, Object[] obj,
			int j) throws SQLException, IOException {
		switch (md.getColumnType(j)) {
		case Types.INTEGER:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.DECIMAL:
		case Types.NUMERIC:
			obj[j - 1] = new BigDecimal(
					child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
			break;
		case Types.BOOLEAN:
		case Types.BIT:
			obj[j - 1] = Boolean
					.valueOf(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
			break;
		case Types.ARRAY:
			ArrayDescriptor arrayLevel3 = ArrayDescriptor.createDescriptor(
					structDesc.getMetaData().getColumnTypeName(j),
					SchemaBuilderUtil.getUnwrapConnection(con));
			boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
			if (type) {
				obj[j - 1] = this.getVarrayType(con, child.get(md.getColumnName(j)), arrayLevel3);
			} else {
				obj[j - 1] = this.getStructDataLevel2(structDesc, child.get(md.getColumnName(j)),
						md.getColumnName(j), j);
			}
			break;
		case Types.STRUCT:
			obj[j - 1] = this.getStructObjectData2(structDesc, child.get(md.getColumnName(j)), j);
			break;
		case Types.VARCHAR:
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.NCHAR:
		case Types.LONGNVARCHAR:
		case Types.CLOB:
			obj[j - 1] = child.get(md.getColumnName(j)).toString().replace(BACKSLASH, "");
			break;
		case Types.NVARCHAR:
			obj[j - 1] = child
					.get(StringEscapeUtils.unescapeJava(md.getColumnName(j)).replace(BACKSLASH, ""));
			break;
		case Types.TIME:
			obj[j - 1] = Time.valueOf(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
			break;
		case Types.BIGINT:
			obj[j - 1] = Long
					.parseLong(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
			break;
		case Types.DOUBLE:
		case Types.FLOAT:
			obj[j - 1] = Double
					.parseDouble(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
			break;
		case Types.REAL:
			obj[j - 1] = Float
					.parseFloat(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
			break;
		case Types.BLOB:
		case Types.BINARY:
		case Types.LONGVARBINARY:
		case Types.VARBINARY:
			String blobData = child.get(md.getColumnName(j)).toString().replace(BACKSLASH, "");
			byte[] byteContent = blobData.getBytes();
			Blob blob = con.createBlob();// Where connection is the connection to db object.
			blob.setBytes(1, byteContent);
			obj[j - 1] = blob;
			break;
		case Types.DATE:
		case Types.TIMESTAMP:
			obj[j - 1] = Timestamp.valueOf(
					QueryBuilderUtil.setDateTimeStampVarrayType(child.get(md.getColumnName(j))));
			break;
		default:
			break;
		}
	}

	/**
	 * This method will build the Struct data required for object type parameters in
	 * Stored procedure from the Json node.
	 * 
	 * @param structDescriptor1
	 * @param node              Json input
	 * @param i
	 * @return the STRUCT
	 * @throws SQLException the SQL exception
	 */
	private STRUCT getStructObjectData2(StructDescriptor structDescriptor1, JsonNode node, int i) throws SQLException {
		StructDescriptor structDesc = StructDescriptor.createDescriptor(
				structDescriptor1.getOracleTypeADT().getAttributeType(i), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDesc.getMetaData();
		Object[] obj11 = new Object[md.getColumnCount()];
		for (int k = 1; k <= md.getColumnCount(); k++) {
			getStructObjectData2Value(node, md, obj11, k);

		}
		return new STRUCT(structDesc, SchemaBuilderUtil.getUnwrapConnection(con), obj11);
	}

	/**
	 * This method will build the Struct data required for nested table type
	 * parameters in Stored procedure from the Json node.
	 *
	 * @param StructDescriptor
	 * @param JsonNode
	 * @param argument         parameter name of the SP
	 * @param index            i
	 * @return the ARRAY
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private ARRAY getStructDataLevel2(StructDescriptor structDesc, JsonNode node, String argument, int i)
			throws SQLException, IOException {
		STRUCT[] structArray = new STRUCT[node.size()];
		ArrayDescriptor arrayLevel2 = ArrayDescriptor.createDescriptor(structDesc.getMetaData().getColumnTypeName(i),
				SchemaBuilderUtil.getUnwrapConnection(con));
		StructDescriptor structDescLevel1 = StructDescriptor.createDescriptor(arrayLevel2.getBaseName(),
				SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDescLevel1.getMetaData();
		for (int l = 0; l < node.size(); l++) {
			JsonNode child = node.get(l);
			Object[] obj = new Object[md.getColumnCount()];
			for (int k = 1; k <= md.getColumnCount(); k++) {
				if (child.get(md.getColumnName(k)) == null || child.get(md.getColumnName(k)).toString().equals("null")
						|| child.get(md.getColumnName(k)).toString().replace(BACKSLASH, "").equals("")) {
					obj[k - 1] = null;
				} else {
					switch (md.getColumnType(k)) {
					case Types.INTEGER:
					case Types.TINYINT:
					case Types.SMALLINT:
						obj[k - 1] = new BigDecimal(child.get(md.getColumnName(k)).toString().replace(BACKSLASH, ""));
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						obj[k - 1] = Boolean.valueOf(child.get(md.getColumnName(k)).toString().replace(BACKSLASH, ""));
						break;
					case Types.STRUCT:
						obj[k - 1] = this.getStructObjectData2(structDescLevel1, child.get(md.getColumnName(k)), k);
						break;
					case Types.ARRAY:
						getArrayType(argument, structDescLevel1, md, child, obj, k);
						break;
					case Types.VARCHAR:
					case Types.CHAR:
					case Types.LONGVARCHAR:
					case Types.NCHAR:
					case Types.LONGNVARCHAR:
					case Types.CLOB:
						obj[k - 1] = child.get(md.getColumnName(k)).toString().replace(BACKSLASH, "");
						break;
					case Types.NVARCHAR:
						obj[k - 1] = (StringEscapeUtils
								.unescapeJava(child.get(md.getColumnName(k)).toString().replace(BACKSLASH, "")));
						break;
					case Types.TIME:
						obj[k - 1] = Time.valueOf(child.get(md.getColumnName(k)).toString().replace(BACKSLASH, ""));
						break;
					case Types.BIGINT:
						obj[k - 1] = Long.parseLong(child.get(md.getColumnName(k)).toString().replace(BACKSLASH, ""));
						break;
					case Types.DOUBLE:
					case Types.FLOAT:
					case Types.DECIMAL:
					case Types.NUMERIC:
					case Types.REAL:
						obj[k - 1] = Double
								.parseDouble(child.get(md.getColumnName(k)).toString().replace(BACKSLASH, ""));
						break;
					case Types.BLOB:
					case Types.BINARY:
					case Types.LONGVARBINARY:
					case Types.VARBINARY:
						String blobData = child.get(md.getColumnName(k)).toString().replace(BACKSLASH, "");
						byte[] byteContent = blobData.getBytes();
						Blob blob = con.createBlob();
						blob.setBytes(1, byteContent);
						obj[k - 1] = blob;
						break;
					case Types.TIMESTAMP:
					case Types.DATE:
						obj[k - 1] = Timestamp
								.valueOf(QueryBuilderUtil.setDateTimeStampVarrayType(child.get(md.getColumnName(k))));
						break;
					default:
						break;
					}
				}
			}

			structArray[l] = new STRUCT(structDescLevel1, SchemaBuilderUtil.getUnwrapConnection(con), obj);
		}
		return new ARRAY(arrayLevel2, SchemaBuilderUtil.getUnwrapConnection(con), structArray);
	}

	/**
	 * 
	 * @param argument
	 * @param structDescLevel1
	 * @param md
	 * @param child
	 * @param obj
	 * @param k
	 * @throws SQLException
	 * @throws IOException
	 */
	private void getArrayType(String argument, StructDescriptor structDescLevel1, ResultSetMetaData md, JsonNode child,
			Object[] obj, int k) throws SQLException, IOException {
		ArrayDescriptor arrayLevel3 = ArrayDescriptor.createDescriptor(
				structDescLevel1.getMetaData().getColumnTypeName(k),
				SchemaBuilderUtil.getUnwrapConnection(con));
		if (QueryBuilderUtil.checkArrayDataType(arrayLevel3)) {
			obj[k - 1] = this.getVarrayType(con, child.get(md.getColumnName(k)), arrayLevel3);
		} else {
			obj[k - 1] = getStructDataLevel2(structDescLevel1, child.get(md.getColumnName(k)), argument,
					k);
		}
	}

	/**
	 * This method will enscape the characters from the xml string and prepare the
	 * input accordingly.
	 *
	 * @param s the input xml string
	 * @return the escaped string
	 */
	public static String unEscapeString(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++)
			switch (s.charAt(i)) {
			case '\n':
				sb.append("\\n");
				break;
			case '\"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\"");
				break;
			// ... rest of escape characters
			default:
				sb.append(s.charAt(i));
			}
		return sb.toString();
	}

	/**
	 * Gets the Array Object for inserting into prepared statenents.
	 *
	 * @param con       the java.sql.Connection
	 * @param key       the param name
	 * @param fieldName the value
	 * @return the ARRAY
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private ARRAY getArrayType(Connection con, String key, JsonNode fieldName) throws SQLException, IOException {
		String typeName = QueryBuilderUtil.getTypeName(con, procedureName, key);
		ArrayDescriptor des = ArrayDescriptor.createDescriptor(typeName, SchemaBuilderUtil.getUnwrapConnection(con));
		Object[] array = new Object[fieldName.size()];
		int k = 0;
		if (des.getBaseType() == 2002) {
			return this.getObjVarrayType(con, fieldName, des,k);
		} else {
			int m = 0;
			for (int l = 1; l <= fieldName.size(); l++) {
				JsonNode elements = fieldName.get(ELEMENT + l);
				if (des.getBaseName().equalsIgnoreCase(TIMESTAMP)) {
					String arr = "\"" + QueryBuilderUtil.setDateTimeStampVarrayType(elements) + "\"";
					elements = mapper.readTree(arr);
				}
				if (elements != null) {
					if (elements.toString().replace("\"", "").equals("")) {
						array[m] = null;
					} else {
						array[m] = elements.toString().replace("\"", "");
					}
					m++;
				}
			}
		}
		return new ARRAY(des, SchemaBuilderUtil.getUnwrapConnection(con), array);

	}

	/**
	 * 
	 * @param con
	 * @param fieldName
	 * @param arrayLevel3
	 * @param k
	 * @return ARRAY
	 * @throws SQLException
	 * @throws IOException
	 */
	private ARRAY getObjVarrayType(Connection con, JsonNode fieldName, ArrayDescriptor arrayLevel3,int k) throws SQLException	{
		STRUCT[] structArray = new STRUCT[fieldName.size()];
		StructDescriptor structDesc;
			structDesc = StructDescriptor.createDescriptor(arrayLevel3.getBaseName(),
					SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDesc.getMetaData();
		for (int i = 0; i < fieldName.size(); i++) {
			k++;
			JsonNode child = fieldName.get(ELEMENT + k);
			if(child==null) {
				throw new SQLException("Kindly check the Input");
			}
			Object[] obj = new Object[md.getColumnCount()];
			for (int j = 1; j <= md.getColumnCount(); j++) {
				if (child.get(md.getColumnName(j)) == null
						|| child.get(md.getColumnName(j)).toString().equals("null")
						|| child.get(md.getColumnName(j)).toString().replace(BACKSLASH, "").equals("")) {
					obj[j - 1] = null;
				} else {
					switch (md.getColumnType(j)) {
					case Types.INTEGER:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.DECIMAL:
					case Types.NUMERIC:
						obj[j - 1] = new BigDecimal(
								child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						obj[j - 1] = Boolean
								.valueOf(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
						break;
					case Types.NVARCHAR:
						obj[j - 1] = child
								.get(StringEscapeUtils.unescapeJava(md.getColumnName(j)).replace(BACKSLASH, ""));
						break;
					case Types.TIME:
						obj[j - 1] = Time.valueOf(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
						break;
					case Types.BIGINT:
						obj[j - 1] = Long
								.parseLong(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
						break;
					case Types.DOUBLE:
					case Types.FLOAT:
						obj[j - 1] = Double
								.parseDouble(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
						break;
					case Types.VARCHAR:
					case Types.CHAR:
					case Types.LONGVARCHAR:
					case Types.NCHAR:
					case Types.LONGNVARCHAR:
					case Types.CLOB:
						obj[j - 1] = child.get(md.getColumnName(j)).toString().replace(BACKSLASH, "");
						break;
					case Types.REAL:
						obj[j - 1] = Float
								.parseFloat(child.get(md.getColumnName(j)).toString().replace(BACKSLASH, ""));
						break;
					case Types.BLOB:
					case Types.BINARY:
					case Types.LONGVARBINARY:
					case Types.VARBINARY:
						String blobData = child.get(md.getColumnName(j)).toString().replace(BACKSLASH, "");
						byte[] byteContent = blobData.getBytes();
						Blob blob = con.createBlob();// Where connection is the connection to db object.
						blob.setBytes(1, byteContent);
						obj[j - 1] = blob;
						break;
					case Types.DATE:
					case Types.TIMESTAMP:
						obj[j - 1] = Timestamp.valueOf(
								QueryBuilderUtil.setDateTimeStampVarrayType(child.get(md.getColumnName(j))));
						break;
					default:
						break;
					}
				}
			}
			structArray[i] = new STRUCT(structDesc, SchemaBuilderUtil.getUnwrapConnection(con), obj);
		}

		return new ARRAY(arrayLevel3, SchemaBuilderUtil.getUnwrapConnection(con), structArray);
	}
	
	
	/**
	 * Gets the VArray Object for inserting into prepared statenents.
	 *
	 * @param con       the java.sql.Connection
	 * @param key       the param name
	 * @param fieldName the value
	 * @return the ARRAY
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	private ARRAY getVarrayType(Connection con, JsonNode fieldName, ArrayDescriptor arrayLevel3)
			throws SQLException, IOException {

		Object[] array = new Object[fieldName.size()];
		int k = 0;
		JsonNode elements = null;
		for (int j = 1; j <= fieldName.size(); j++) {
			elements = fieldName.get(ELEMENT + j);
			if (arrayLevel3.getBaseName().equalsIgnoreCase(TIMESTAMP)) {
				elements = mapper.readTree("\"" + QueryBuilderUtil.setDateTimeStampVarrayType(elements) + "\"");
			}
			if (elements != null) {
				if (elements.toString().replace("\"", "").equals("")) {
					array[k] = null;
				} else {
					array[k] = elements.toString().replace("\"", "");
				}
				k++;
			}
		}
		return new ARRAY(arrayLevel3, SchemaBuilderUtil.getUnwrapConnection(con), array);

	}

	/**
	 * Method that will execute remaining records in the batch.
	 *
	 * @param objdata  the objdata
	 * @param csmt     the csmt
	 * @param batchnum the batchnum
	 * @param b        the b
	 */
	private void executeRemaining(ObjectData objdata, CallableStatement csmt, int batchnum, int b) {
		Payload payload = null;
		try {
			int[] res = csmt.executeBatch();
			response.getLogger().log(Level.INFO, OracleDatabaseConstants.BATCH_NUM, batchnum);
			response.getLogger().log(Level.INFO, OracleDatabaseConstants.REMAINING_BATCH_RECORDS, res.length);
			payload = JsonPayloadUtil.toPayload(new BatchResponse(
					"Remaining records added to batch and executed successfully", batchnum, res.length));
			ResponseUtil.addSuccess(response, objdata, OracleDatabaseConstants.SUCCESS_RESPONSE_CODE, payload);
			con.commit();

		} catch (SQLException e) {

			CustomResponseUtil.logFailedBatch(response, batchnum, b);
			CustomResponseUtil.writeSqlErrorResponse(e, objdata, response);

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
	 * This method will call the procedure and process the resultset based on the
	 * OUT param.
	 *
	 * @param csmt         the Callable Statement
	 * @param objdata      the objdata
	 * @param maxFieldSize the max field size
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void callProcedure(CallableStatement csmt, ObjectData objdata, Long maxFieldSize, Long fetchSize)
			throws SQLException, IOException {
		if (maxFieldSize != null && maxFieldSize > 0) {
			csmt.setMaxFieldSize(maxFieldSize.intValue());
		}
		if (fetchSize != null && fetchSize > 0) {
			csmt.setFetchSize(fetchSize.intValue());
		}
		boolean result = csmt.execute();
		this.processOutParams(csmt, result, objdata);
	}

	/**
	 * This method will process the Out params based on the OUT Param Datatypes and
	 * values.
	 *
	 * @param csmt    the Callable Statement
	 * @param result  boolean to detrmine whether executed procedure has resulset
	 * @param objdata the objdata
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processOutParams(CallableStatement csmt, boolean result, ObjectData objdata)
			throws SQLException, IOException {
		InputStream tempInputStream = null;
		OutputStream out = null;
		// temporary outputStream to flush the content of JsonGenerator.
		out = operationContext.createTempOutputStream();
		JsonGenerator generator = JSON_FACTORY.createGenerator(out);
		try (ResultSet rs = csmt.getResultSet();) {
			if (outParams != null && !outParams.isEmpty() && !result) {
				generator.writeStartObject();
				for (int i = 0; i <= outParams.size() - 1; i++) {
					writeProcessOutValues(csmt, objdata, generator, i);
				}
				generator.writeEndObject();
				generator.flush();
				tempInputStream = operationContext.tempOutputStreamToInputStream(out);
				try (Payload payload = ResponseUtil.toPayload(tempInputStream)) {
					response.addResult(objdata, OperationStatus.SUCCESS, OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
							OracleDatabaseConstants.SUCCESS_RESPONSE_MESSAGE, payload);
				}
			} else if (result && rs != null) {
				this.processResultset(objdata, rs);
			} else if (!result && rs == null) {
				try (Payload payload = JsonPayloadUtil
						.toPayload(new ProcedureResponseNonBatch(200, "Procedure Executed Successfully!!"))) {
					response.addResult(objdata, OperationStatus.SUCCESS, OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
							OracleDatabaseConstants.SUCCESS_RESPONSE_MESSAGE, payload);
				}
			}
			logger.info("Procedure called Successfully!!!");
		} finally {
			IOUtil.closeQuietly(tempInputStream, out, generator);
		}

	}

	/**
	 * 
	 * @param csmt
	 * @param objdata
	 * @param generator
	 * @param i
	 * @throws IOException
	 * @throws SQLException
	 */
	private void writeProcessOutValues(CallableStatement csmt, ObjectData objdata, JsonGenerator generator, int i)
			throws IOException, SQLException {
		if (dataType.get(outParams.get(i)) == 2003) {
			this.processVarrays(generator, csmt, i);
		} else if (dataType.get(outParams.get(i)).equals(2012)) {
			this.processRefCursors(generator, csmt, i);
		} else if (dataType.get(outParams.get(i)).equals(2010)) {
			writeStructValue(csmt, generator, i);
		}

		else if (dataType.get(outParams.get(i)).equals(2002)) {
			this.processObjectStruct(generator, (OracleCallableStatement) csmt,
					params.indexOf(outParams.get(i)) + 1, outParams.get(i));
		} else if (dataType.get(outParams.get(i)).equals(2009)) {
			writeXMLvalue(csmt, generator, i);
		} else if (csmt.getObject(params.indexOf(outParams.get(i)) + 1) instanceof java.sql.Blob) {
			Blob b = csmt.getBlob(params.indexOf(outParams.get(i)) + 1);
			byte[] byteArray = b.getBytes(1, (int) b.length());
			String data = new String(byteArray, StandardCharsets.UTF_8);
			generator.writeStringField(outParams.get(i), data);
			generator.flush();
		} else if (null == csmt.getString(params.indexOf(outParams.get(i)) + 1)
				|| csmt.getString(params.indexOf(outParams.get(i)) + 1).isEmpty()) {
			generator.writeNullField(outParams.get(i));
			generator.flush();
		} else if (null != csmt.getString(params.indexOf(outParams.get(i)) + 1)
				|| !csmt.getString(params.indexOf(outParams.get(i)) + 1).isEmpty()) {
			generator.writeStringField(outParams.get(i),
					csmt.getString(params.indexOf(outParams.get(i)) + 1));
			generator.flush();
		} else {
			try (Payload payload = JsonPayloadUtil
					.toPayload(new ErrorDetails(405, "Value from Out parameter is Null"))) {
				response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
						OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
						OracleDatabaseConstants.SUCCESS_RESPONSE_MESSAGE, payload);
			}
		}
	}

	/**
	 * 
	 * @param csmt
	 * @param generator
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeStructValue(CallableStatement csmt, JsonGenerator generator, int i)
			throws SQLException, IOException {
		if (csmt.getArray(params.indexOf(outParams.get(i)) + 1) == null) {
			generator.writeNullField(outParams.get(i));
		} else {
			this.processStruct(generator, (OracleCallableStatement) csmt,
					params.indexOf(outParams.get(i)) + 1, outParams.get(i));
		}
	}

	/**
	 * 
	 * @param csmt
	 * @param generator
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeXMLvalue(CallableStatement csmt, JsonGenerator generator, int i)
			throws SQLException, IOException {
		if (csmt.getSQLXML(params.indexOf(outParams.get(i)) + 1) == null) {
			generator.writeNullField(outParams.get(i));
		} else {
			generator.writeStringField(outParams.get(i),
					csmt.getSQLXML(params.indexOf(outParams.get(i)) + 1).getString());
		}
		generator.flush();
	}

	/**
	 * Method to process STRUCT Data for object type parameters in Stored Procedure.
	 *
	 * @param generator the generator
	 * @param csmt      the csmt
	 * @param i         the i
	 * @param outParam  the out param
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processObjectStruct(JsonGenerator generator, OracleCallableStatement csmt, int i, String outParam)
			throws SQLException, IOException {
		java.sql.Struct array = (java.sql.Struct) csmt.getObject(i);
		Datum[] data = ((oracle.sql.STRUCT) array).getOracleAttributes();
		StructDescriptor structDesc = StructDescriptor.createDescriptor(
				QueryBuilderUtil.getTypeName(con, procedureName, outParam), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDesc.getMetaData();
		generator.writeArrayFieldStart(outParam);
		generator.writeStartObject();
		int k = 0;
		for (Object elements1 : data) {
			k++;
			if (elements1 == null) {
				generator.writeNullField(md.getColumnName(k));
				generator.flush();
			} else if (TINYINT.equalsIgnoreCase(md.getColumnTypeName(k))
					|| SMALLINT.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NUMERIC.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DECIMAL.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DOUBLE.equalsIgnoreCase(md.getColumnTypeName(k))) {
				generator.writeNumberField(md.getColumnName(k), ((oracle.sql.NUMBER) elements1).doubleValue());
				generator.flush();
			} else if (VARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| CHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| LONGNVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DATE.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k)) || elements1 instanceof String
					|| elements1 instanceof Timestamp) {
				generator.writeStringField(md.getColumnName(k), elements1.toString().replace(BACKSLASH, ""));
				generator.flush();
			} else if (REAL.equalsIgnoreCase(md.getColumnTypeName(k))
					|| FLOAT.equalsIgnoreCase(md.getColumnTypeName(k))) {
				generator.writeNumberField(md.getColumnName(k), ((oracle.sql.NUMBER) elements1).floatValue());
				generator.flush();
			} else if (elements1 instanceof java.math.BigDecimal) {
				generator.writeNumberField(md.getColumnName(k), ((BigDecimal) elements1).intValue());
				generator.flush();
			} else if (NUMBER.equalsIgnoreCase(md.getColumnTypeName(k))
					|| INTEGER.equalsIgnoreCase(md.getColumnTypeName(k)) || elements1 instanceof oracle.sql.NUMBER) {
				generator.writeNumberField(md.getColumnName(k), ((oracle.sql.NUMBER) elements1).intValue());
				generator.flush();
			} else if (elements1 instanceof java.sql.Blob) {
				Blob b = (Blob) elements1;
				byte[] byteArray = b.getBytes(1, (int) b.length());
				String data1 = new String(byteArray, StandardCharsets.UTF_8);
				generator.writeStringField(md.getColumnName(k), data1);
				generator.flush();
			} else if (elements1 instanceof oracle.sql.RAW) {
				oracle.sql.RAW b = (oracle.sql.RAW) elements1;
				byte[] byteArray = b.getBytes();
				String data1 = new String(byteArray, StandardCharsets.UTF_8);
				generator.writeStringField(md.getColumnName(k), data1);
				generator.flush();
			}
		}
		generator.writeEndObject();
		generator.writeEndArray();
	}

	/**
	 * Method to process STRUCT Data for table type parameters in Stored Procedure.
	 *
	 * @param generator the generator
	 * @param csmt      the csmt
	 * @param i         the i
	 * @param outParam  the out param
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processStruct(JsonGenerator generator, OracleCallableStatement csmt, int i, String outParam)
			throws SQLException, IOException {
		ARRAY array = csmt.getARRAY(i);
		ArrayDescriptor arrayDescriptor = new ArrayDescriptor(
				QueryBuilderUtil.getTypeName(con, procedureName, outParam), con);
		Datum[] data = array.getOracleArray();
		generator.writeArrayFieldStart(outParam);
		if (VARCHAR.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| DATE.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| CHAR.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| NCHAR.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| DOUBLE.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| FLOAT.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| DECIMAL.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| NUMERIC.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| REAL.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| BLOB.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| TIMESTAMP.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| INTEGER.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| SMALLINT.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| NUMBER.equalsIgnoreCase(arrayDescriptor.getBaseName())) {
			for (Object elements1 : data) {
				processStructValue(generator, outParam, arrayDescriptor, elements1);
			}
		} else if (arrayDescriptor.getBaseType() == 2002) {
			writeProcessStruct(generator, arrayDescriptor, data);
		}
		generator.writeEndArray();

	}

	/**
	 * 
	 * @param generator
	 * @param outParam
	 * @param arrayDescriptor
	 * @param elements1
	 * @throws IOException
	 * @throws SQLException
	 */
	private void processStructValue(JsonGenerator generator, String outParam, ArrayDescriptor arrayDescriptor,
			Object elements1) throws IOException, SQLException {
		if (elements1 == null) {
			generator.writeNullField(outParam);
			generator.flush();
		} else if (VARCHAR.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| DATE.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| CHAR.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| NCHAR.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| TIMESTAMP.equalsIgnoreCase(arrayDescriptor.getBaseName())) {
			generator.writeString(elements1.toString());
			generator.flush();
		} else if (REAL.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| FLOAT.equalsIgnoreCase(arrayDescriptor.getBaseName())) {
			generator.writeNumber(((oracle.sql.NUMBER) elements1).floatValue());
			generator.flush();
		} else if (DECIMAL.equalsIgnoreCase(arrayDescriptor.getBaseName())
				|| DOUBLE.equalsIgnoreCase(arrayDescriptor.getBaseName())) {
			generator.writeNumber(((oracle.sql.NUMBER) elements1).doubleValue());
			generator.flush();
		} else if (elements1 instanceof java.math.BigDecimal) {
			generator.writeNumber(((BigDecimal) elements1).intValue());
			generator.flush();
		} else if (elements1 instanceof oracle.sql.NUMBER) {
			generator.writeNumber(((oracle.sql.NUMBER) elements1).intValue());
			generator.flush();
		} else if (elements1 instanceof java.sql.Blob) {
			Blob b = (Blob) elements1;
			byte[] byteArray = b.getBytes(1, (int) b.length());
			String data1 = new String(byteArray, StandardCharsets.UTF_8);
			generator.writeString(data1);
			generator.flush();
		}
	}

	/**
	 * 
	 * @param generator
	 * @param arrayDescriptor
	 * @param data
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeProcessStruct(JsonGenerator generator, ArrayDescriptor arrayDescriptor, Datum[] data)
			throws SQLException, IOException {
		for (int j = 0; j < data.length; j++) {
			StructDescriptor structDescriptor = StructDescriptor.createDescriptor(arrayDescriptor.getBaseName(),
					SchemaBuilderUtil.getUnwrapConnection(con));
			ResultSetMetaData md = structDescriptor.getMetaData();
			generator.writeStartObject();
			Object[] elements = ((STRUCT) data[j]).getAttributes();
			int k = 1;
			for (Object element : elements) {
				if (element == null) {
					generator.writeNullField(md.getColumnName(k++));
					generator.flush();
				} else if (element instanceof String || element instanceof Timestamp) {
					generator.writeStringField(md.getColumnName(k++), element.toString().replace(BACKSLASH, ""));
					generator.flush();
				} else if (element instanceof BigDecimal) {
					generator.writeNumberField(md.getColumnName(k++), ((BigDecimal) element).floatValue());
					generator.flush();
				} else if (element instanceof java.sql.Blob) {
					Blob b = (Blob) element;
					byte[] byteArray = b.getBytes(1, (int) b.length());
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(md.getColumnName(k++), data1);
					generator.flush();
				} else if (element instanceof oracle.sql.STRUCT) {
					this.processObjectStruct2(k, generator, element, md.getColumnName(k), structDescriptor);
					k++;
				} else if (element instanceof oracle.sql.ARRAY) {

					ArrayDescriptor arrayLevel3 = ArrayDescriptor.createDescriptor(
							structDescriptor.getMetaData().getColumnTypeName(k),
							SchemaBuilderUtil.getUnwrapConnection(con));
					this.arrayElement(arrayLevel3, k, generator, md.getColumnName(k), element, structDescriptor);
					k++;

				}
			}

			generator.writeEndObject();
		}
	}

	/**
	 * Method to process STRUCT Data for object type parameters in Stored Procedure.
	 *
	 * @param generator the generator
	 * @param csmt      the csmt
	 * @param i         the i
	 * @param outParam  the out param
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processObjectStruct2(int i, JsonGenerator generator, Object element, String argument,
			StructDescriptor structDescriptor1) throws SQLException, IOException {
		StructDescriptor structDescriptor = StructDescriptor.createDescriptor(
				structDescriptor1.getOracleTypeADT().getAttributeType(i), SchemaBuilderUtil.getUnwrapConnection(con));
		java.sql.Struct array = (java.sql.Struct) element;
		Datum[] data = ((oracle.sql.STRUCT) array).getOracleAttributes();
		ResultSetMetaData md = structDescriptor.getMetaData();
		generator.writeArrayFieldStart(argument);
		generator.writeStartObject();
		int k = 0;
		for (Object elements1 : data) {
			k++;
			if (elements1 == null) {
				generator.writeNullField(md.getColumnName(k));
				generator.flush();
			} else if (TINYINT.equalsIgnoreCase(md.getColumnTypeName(k))
					|| SMALLINT.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NUMERIC.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DECIMAL.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DOUBLE.equalsIgnoreCase(md.getColumnTypeName(k))) {
				generator.writeNumberField(md.getColumnName(k), ((oracle.sql.NUMBER) elements1).doubleValue());
				generator.flush();
			} else if (VARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| CHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| LONGNVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k))
					|| TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(k))
					|| DATE.equalsIgnoreCase(md.getColumnTypeName(k))
					|| NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(k)) || elements1 instanceof String
					|| elements1 instanceof Timestamp) {
				generator.writeStringField(md.getColumnName(k), elements1.toString().replace(BACKSLASH, ""));
				generator.flush();
			} else if (REAL.equalsIgnoreCase(md.getColumnTypeName(k))
					|| FLOAT.equalsIgnoreCase(md.getColumnTypeName(k))) {
				generator.writeNumberField(md.getColumnName(k), ((oracle.sql.NUMBER) elements1).floatValue());
				generator.flush();
			} else if (elements1 instanceof java.math.BigDecimal) {
				generator.writeNumberField(md.getColumnName(k), ((BigDecimal) elements1).intValue());
				generator.flush();
			} else if (NUMBER.equalsIgnoreCase(md.getColumnTypeName(k))
					|| INTEGER.equalsIgnoreCase(md.getColumnTypeName(k)) || elements1 instanceof oracle.sql.NUMBER) {
				generator.writeNumberField(md.getColumnName(k), ((oracle.sql.NUMBER) elements1).intValue());
				generator.flush();
			} else if (elements1 instanceof java.sql.Blob) {
				Blob b = (Blob) elements1;
				byte[] byteArray = b.getBytes(1, (int) b.length());
				String data1 = new String(byteArray, StandardCharsets.UTF_8);
				generator.writeStringField(md.getColumnName(k), data1);
				generator.flush();
			} else if (elements1 instanceof oracle.sql.RAW) {
				oracle.sql.RAW b = (oracle.sql.RAW) elements1;
				byte[] byteArray = b.getBytes();
				String data1 = new String(byteArray, StandardCharsets.UTF_8);
				generator.writeStringField(md.getColumnName(k), data1);
				generator.flush();
			}
		}
		generator.writeEndObject();
		generator.writeEndArray();
	}

	/**
	 * Method to process STRUCT Data for nested table type parameters in Stored
	 * Procedure.
	 *
	 * @param generator        the generator
	 * @param i                the i
	 * @param element          the element
	 * @param argument         the out argument
	 * @param structDescriptor
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processStruct2(JsonGenerator generator, Integer i, Object element, String argument,
			StructDescriptor structDescriptor) throws SQLException, IOException {
		Object[] array = (Object[]) ((oracle.sql.ARRAY) element).getArray();
		ArrayDescriptor arrayDescriptor2 = ArrayDescriptor.createDescriptor(
				structDescriptor.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
		StructDescriptor structDescriptor1 = StructDescriptor.createDescriptor(arrayDescriptor2.getBaseName(),
				SchemaBuilderUtil.getUnwrapConnection(con));
		generator.writeArrayFieldStart(argument);
		STRUCT rowLevel2 = null;
		ResultSetMetaData md = structDescriptor1.getMetaData();
		for (int j = 0; j < array.length; j++) {
			rowLevel2 = (STRUCT) array[j];
			generator.writeStartObject();
			int k = 0;
			for (Object element1 : rowLevel2.getAttributes()) {
				k++;
				if (element1 == null) {
					generator.writeNullField(md.getColumnName(k));
				} else if (NUMBER.equalsIgnoreCase(md.getColumnTypeName(k))
						|| INTEGER.equalsIgnoreCase(md.getColumnTypeName(k))) {
					generator.writeNumberField(md.getColumnName(k), ((BigDecimal) element1).intValue());
					generator.flush();
				} else if (element1 instanceof BigDecimal) {
					generator.writeNumberField(md.getColumnName(k), ((BigDecimal) element1).floatValue());
				} else if (element1 instanceof String || element1 instanceof Timestamp) {
					generator.writeStringField(md.getColumnName(k), element1.toString().replace(BACKSLASH, ""));
				} else if (element1 instanceof java.sql.Blob) {
					Blob b = (Blob) element1;
					byte[] byteArray = b.getBytes(1, (int) b.length());
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(md.getColumnName(k), data1);
					generator.flush();
				} else if (element1 instanceof Timestamp) {
					generator.writeStringField(md.getColumnName(k), element1.toString());
				} else if (element1 instanceof oracle.sql.STRUCT) {
					this.processObjectStruct2(k, generator, element1, md.getColumnName(k), structDescriptor1);
					k++;

				} else if (element1 instanceof Array) {
					ArrayDescriptor arrayLevel3 = ArrayDescriptor.createDescriptor(
							structDescriptor1.getMetaData().getColumnTypeName(k),
							SchemaBuilderUtil.getUnwrapConnection(con));
					this.arrayElement(arrayLevel3, k, generator, md.getColumnName(k), element1, structDescriptor1);
				}
			}
			generator.writeEndObject();
		}
		generator.writeEndArray();
	}

	/**
	 * This method will process VARRAYS from the OUT Params and add the values to
	 * JsonGenerator based on the datatypes.
	 *
	 * @param generator the generator
	 * @param element   the element
	 * @param i         the i
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void processVarraysElement(JsonGenerator generator, Object element, String argument)
			throws IOException, SQLException {
		Object[] array = (Object[]) ((oracle.sql.ARRAY) element).getArray();
		generator.writeFieldName(argument);
		generator.writeStartObject();
		int k = 0;
		for (Object element1 : array) {
			k++;
			if (element1 instanceof BigDecimal) {
				generator.writeNumberField(ELEMENT + k, ((BigDecimal) element1).floatValue());
			} else if (element1 instanceof String) {
				generator.writeStringField(ELEMENT + k, (String) element1);
			} else if (element1 instanceof Timestamp) {
				generator.writeStringField(ELEMENT + k, element1.toString());
			}

		}
		generator.writeEndObject();
		generator.flush();

	}

	/**
	 * This method will process OUT params of type REFCURSORS. This method will add
	 * the values from the refcursors to the JsonGenerator and flush each column
	 * value content to the temporary OutputStream.
	 *
	 * @param generator the generator
	 * @param csmt      the csmt
	 * @param i         the i
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */
	private void processRefCursors(JsonGenerator generator, CallableStatement csmt, int i)
			throws SQLException, IOException {
		try (ResultSet set = ((OracleCallableStatement) csmt).getCursor(params.indexOf(outParams.get(i)) + 1)) {
			generator.writeArrayFieldStart(outParams.get(i));
			while (set.next()) {
				generator.writeStartObject();
				for (int l = 1; l <= set.getMetaData().getColumnCount(); l++) {
					switch (set.getMetaData().getColumnType(l)) {
					case Types.INTEGER:
					case Types.TINYINT:
					case Types.SMALLINT:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getInt(set.getMetaData().getColumnName(l)));
						break;
					case Types.NUMERIC:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getBigDecimal(set.getMetaData().getColumnName(l)));
							break;
					case Types.VARCHAR:
					case Types.CLOB:
					case Types.DATE:
					case Types.TIME:
					case Types.CHAR:
					case Types.NCHAR:
					case Types.LONGVARCHAR:
					case Types.LONGNVARCHAR:
					case Types.NVARCHAR:
					case Types.TIMESTAMP:
						generator.writeStringField(set.getMetaData().getColumnName(l),
								set.getString(set.getMetaData().getColumnName(l)));
						break;
					case Types.REAL:
					case Types.FLOAT:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getFloat(set.getMetaData().getColumnName(l)));
						break;
					case Types.DECIMAL:
					case Types.DOUBLE:
						generator.writeNumberField(set.getMetaData().getColumnName(l),
								set.getDouble(set.getMetaData().getColumnName(l)));
						break;
					case Types.BOOLEAN:
						generator.writeBooleanField(set.getMetaData().getColumnName(l),
								set.getBoolean(set.getMetaData().getColumnName(l)));
						break;
					default:
						break;
					}
					generator.flush();
				}
				generator.writeEndObject();
			}
			generator.writeEndArray();
			generator.flush();
		}
	}

	/**
	 * This method will process VARRAYS from the OUT Params and add the values to
	 * JsonGenerator based on the datatypes.
	 *
	 * @param generator the generator
	 * @param csmt      the csmt
	 * @param i         the i
	 * @throws IOException  Signals that an I/O exception has occurred.
	 * @throws SQLException the SQL exception
	 */
	private void processVarrays(JsonGenerator generator, CallableStatement csmt, int i)
			throws IOException, SQLException {
		  generator.writeFieldName(outParams.get(i)); 
		  generator.writeStartObject();
		if (csmt.getArray(params.indexOf(outParams.get(i)) + 1) != null) {
			Array arr = csmt.getArray(params.indexOf(outParams.get(i)) + 1);
			switch (arr.getBaseType()) {
			case 2:
			case 4:
			case 8:
			case 6:
			case 7:
			case 3:
				writeNumberField(generator, arr);
				break;
			case 12:
			case 1:
			case -15:
			case -9:
				writeStringField(generator, arr);
				break;
			case 91:
			case 93:
				writeTimestampField(generator, arr);
				break;
			case 2002:
				this.processObjVarrays(generator, csmt, i);
				
				break;
			default:
				break;
			}

		}
		generator.writeEndObject();
		generator.flush();

	}

	/**
	 * 
	 * @param generator
	 * @param arr
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeTimestampField(JsonGenerator generator, Array arr) throws SQLException, IOException {
		Timestamp[] date = (Timestamp[]) arr.getArray();
		for (int j = 1; j <= date.length; j++) {
			if (null == date[j - 1]) {
				generator.writeNullField(ELEMENT + j);
			} else {
				generator.writeStringField(ELEMENT + j, date[j - 1].toString());
			}
		}
	}

	/**
	 * 
	 * @param generator
	 * @param arr
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeStringField(JsonGenerator generator, Array arr) throws SQLException, IOException {
		String[] chars = (String[]) arr.getArray();
		for (int j = 1; j <= chars.length; j++) {
			if (null == chars[j - 1]) {
				generator.writeNullField(ELEMENT + j);
			} else {
				generator.writeStringField(ELEMENT + j, chars[j - 1]);
			}
		}
	}

	/**
	 * 
	 * @param generator
	 * @param arr
	 * @throws SQLException
	 * @throws IOException
	 */
	private void writeNumberField(JsonGenerator generator, Array arr) throws SQLException, IOException {
		BigDecimal[] nos = (BigDecimal[]) arr.getArray();
		for (int j = 1; j <= nos.length; j++) {
			generator.writeNumberField(ELEMENT + j, nos[j - 1]);
		}
	}

	/**
	 * 
	 * @param generator
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private void processObjVarrays(JsonGenerator generator, CallableStatement csmt, int i) throws SQLException, IOException {
		ARRAY array = ((OracleCallableStatement) csmt).getARRAY(params.indexOf(outParams.get(i)) + 1);
		ArrayDescriptor arrayDescriptor = new ArrayDescriptor(
				QueryBuilderUtil.getTypeName(con, procedureName, outParams.get(i)), SchemaBuilderUtil.getUnwrapConnection(con));
		Datum[] data = array.getOracleArray();
		int l = 1;
		for (int j = 0; j < data.length; j++) {
			StructDescriptor structDescriptor = StructDescriptor.createDescriptor(arrayDescriptor.getBaseName(),
					SchemaBuilderUtil.getUnwrapConnection(con));
			ResultSetMetaData md = structDescriptor.getMetaData();
			generator.writeFieldName(ELEMENT + l++);
			generator.writeStartObject();
			Object[] elements = ((STRUCT) data[j]).getAttributes();
			int k = 1;
			for (Object element : elements) {
				if (element == null) {
					generator.writeNullField(md.getColumnName(k++));
					generator.flush();
				}else if (NUMBER.equalsIgnoreCase(md.getColumnTypeName(k))
						|| INTEGER.equalsIgnoreCase(md.getColumnTypeName(k)) || element instanceof oracle.sql.NUMBER) {
					generator.writeNumberField(md.getColumnName(k++), ((BigDecimal) element).intValue());
					generator.flush(); 
				}
				else if (element instanceof String || element instanceof Timestamp) {
					generator.writeStringField(md.getColumnName(k++),
							element.toString().replace(BACKSLASH, ""));
					generator.flush();
				} else if (element instanceof BigDecimal) {
					generator.writeNumberField(md.getColumnName(k++), ((BigDecimal) element).floatValue());
					generator.flush();
				} else if (element instanceof java.sql.Blob) {
					Blob b = (Blob) element;
					byte[] byteArray = b.getBytes(1, (int) b.length());
					String data1 = new String(byteArray, StandardCharsets.UTF_8);
					generator.writeStringField(md.getColumnName(k++), data1);
					generator.flush();
				}
			}
			generator.writeEndObject();
		}
		
	}
	
	/**
	 * Sets the String value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setStringDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.VARCHAR);
		} else {
			csmt.setString(params.indexOf(inParams.get(i - 1)) + 1, node.toString().replace("\"", ""));
		}
	}

	/**
	 * Sets the Nvarchar value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setNvarcharDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.NVARCHAR);
		} else {
			csmt.setString(params.indexOf(inParams.get(i - 1)) + 1,
					StringEscapeUtils.unescapeJava(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the Clob value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setClobDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.CLOB);
		} else {
			csmt.setString(params.indexOf(inParams.get(i - 1)) + 1, node.toString().replace("\"", ""));
		}
	}

	/**
	 * Sets the Numeric value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setNumericDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.NUMERIC);
		} else {
			csmt.setBigDecimal(params.indexOf(inParams.get(i - 1)) + 1, new BigDecimal(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the Time value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setTimeDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.TIME);
		} else {
			csmt.setTime(params.indexOf(inParams.get(i - 1)) + 1, Time.valueOf(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the BigInt value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setBigIntDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.NUMERIC);
		} else {
			csmt.setLong(params.indexOf(inParams.get(i - 1)) + 1,
					Long.parseLong(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the Boolean value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setBooleanDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.BOOLEAN);
		} else {
			csmt.setBoolean(params.indexOf(inParams.get(i - 1)) + 1,
					Boolean.valueOf(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the Double value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setDoubleDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.DOUBLE);
		} else {
			csmt.setDouble(params.indexOf(inParams.get(i - 1)) + 1,
					Double.parseDouble(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the Real value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setRealDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.REAL);
		} else {
			csmt.setFloat(params.indexOf(inParams.get(i - 1)) + 1,
					Float.parseFloat(node.toString().replace(BACKSLASH, "")));
		}
	}

	/**
	 * Sets the Blob value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setBlobDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException, IOException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.BLOB);
		} else {
			String blobData = node.toString().replace(BACKSLASH, "");
			try (InputStream stream = new ByteArrayInputStream(blobData.getBytes());) {
				{
					csmt.setBlob(params.indexOf(inParams.get(i - 1)) + 1, stream);
				}
			}
		}
	}

	/**
	 * This method will check for the valid timestamp format.
	 *
	 * @param node the node
	 * @param i    the i
	 * @param csmt the csmt
	 * @throws SQLException
	 */
	private void setDateTimStampDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {

		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.TIMESTAMP);
		} else {
			try {
				setDateTimStampDataType1(node, csmt, i);

			} catch (Exception e) {
				csmt.setString(params.indexOf(inParams.get(i - 1)) + 1, node.toString().replace(BACKSLASH, ""));
			}
		}
	}

	/**
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setDateTimStampDataType1(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		String reformattedStr;
		SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME);
		SimpleDateFormat myFormat2 = new SimpleDateFormat(DATETIME_FORMAT);

		try {
			reformattedStr = myFormat2.format(fromUser1.parse(node.toString().replace(BACKSLASH, "")));
		} catch (ParseException e) {

			SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME_FORMAT1);
			SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT);
			try {
				reformattedStr = myFormat.format(fromUser.parse(node.toString().replace(BACKSLASH, "")));
			} catch (ParseException e1) {
				reformattedStr = node.toString().replace(BACKSLASH, "");
			}

		}
		csmt.setString(params.indexOf(inParams.get(i - 1)) + 1, reformattedStr);
	}

	/**
	 * This method will check for the valid timestamp format.
	 *
	 * @param node the node
	 * @param i    the i
	 * @param csmt the csmt
	 * @param type th type
	 * @throws SQLException
	 */
	private void setDateDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {

		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.DATE);
		} else {
			try {
				setDateDataType1(node, csmt, i);

			} catch (Exception e) {
				csmt.setString(params.indexOf(inParams.get(i - 1)) + 1, node.toString().replace(BACKSLASH, ""));
			}
		}
	}

	/**
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 */
	private void setDateDataType1(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException {
		String reformattedStr;
		SimpleDateFormat fromUser1 = new SimpleDateFormat(DATETIME);
		SimpleDateFormat myFormat2 = new SimpleDateFormat(DATETIME_FORMAT3);

		try {
			reformattedStr = myFormat2.format(fromUser1.parse(node.toString().replace(BACKSLASH, "")));
		} catch (ParseException e) {

			SimpleDateFormat fromUser = new SimpleDateFormat(DATETIME_FORMAT1);
			SimpleDateFormat myFormat = new SimpleDateFormat(DATETIME_FORMAT3);
			try {
				reformattedStr = myFormat.format(fromUser.parse(node.toString().replace(BACKSLASH, "")));
			} catch (ParseException e1) {
				reformattedStr = node.toString().replace(BACKSLASH, "");
			}

		}
		csmt.setString(params.indexOf(inParams.get(i - 1)) + 1, reformattedStr);
	}

	/**
	 * Sets the Array value
	 * 
	 * @param node
	 * @param csmt
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private void setArrayDataType(JsonNode node, OracleCallableStatement csmt, int i) throws SQLException, IOException {
		if (node == null || node.toString().replace(BACKSLASH, "").equals("")) {
			csmt.setNull(params.indexOf(inParams.get(i - 1)) + 1, Types.ARRAY,
					QueryBuilderUtil.getTypeName(con, procedureName, inParams.get(i - 1)));
		} else {
			ARRAY array = this.getArrayType(con, inParams.get(i - 1), node);
			csmt.setArray(params.indexOf(inParams.get(i - 1)) + 1, array);
		}
	}

	/**
	 * This method will set Array of values
	 * 
	 * @param bstmnt
	 * @param fieldname
	 * @param con
	 * @param ArrayDescriptor
	 * @return Array
	 * @throws SQLException
	 */
	private ARRAY setPrimitiveArray(Connection con, JsonNode fieldName, ArrayDescriptor arrayLevel3)
			throws SQLException {
		Object[] array = new Object[fieldName.size()];
		int k = 0;
		if (fieldName.toString().replace("\"", "").equals("")) {
			array = null;
		} else {
			if (fieldName.isArray()) {
				setPrimitiveArrayValue(con, fieldName, arrayLevel3, array, k);

			}
		}
		return new ARRAY(arrayLevel3, SchemaBuilderUtil.getUnwrapConnection(con), array);

	}

	/**
	 * 
	 * @param con
	 * @param fieldName
	 * @param arrayLevel3
	 * @param array
	 * @param k
	 * @throws SQLException
	 */
	private void setPrimitiveArrayValue(Connection con, JsonNode fieldName, ArrayDescriptor arrayLevel3, Object[] array,
			int k) throws SQLException {
		for (JsonNode objNode : fieldName) {
			if (arrayLevel3.getBaseName().equalsIgnoreCase(VARCHAR)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(CHAR)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(LONGVARCHAR)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(NCHAR)) {
				array[k] = objNode.toString().replace("\"", "");
			} else if (arrayLevel3.getBaseName().equalsIgnoreCase(NUMBER)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(INTEGER)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(SMALLINT)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(DECIMAL)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(NUMERIC)) {
				array[k] = new BigDecimal(objNode.toString().replace("\"", ""));
			} else if (arrayLevel3.getBaseName().equalsIgnoreCase(TIME)) {
				array[k] = Time.valueOf(objNode.toString().replace("\"", ""));
			} else if (arrayLevel3.getBaseName().equalsIgnoreCase(TIMESTAMP)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(DATE)) {
				array[k] = Timestamp.valueOf(QueryBuilderUtil.setDateTimeStampVarrayType(objNode));
			} else if (arrayLevel3.getBaseName().equalsIgnoreCase(DOUBLE)
					|| arrayLevel3.getBaseName().equalsIgnoreCase(FLOAT)) {
				array[k] = Double.parseDouble(objNode.toString().replace("\"", ""));
			}

			else if (arrayLevel3.getBaseName().equalsIgnoreCase(BLOB)) {

				String blobData = objNode.toString().replace("\"", "");
				byte[] byteContent = blobData.getBytes();
				Blob blob = con.createBlob();
				blob.setBytes(1, byteContent);
				array[k] = blob;
			}

			k++;
		}
	}

	/**
	 * 
	 * @param arrayLevel3
	 * @param k
	 * @param generator
	 * @param columnName
	 * @param element1
	 * @param structDescriptor1
	 * @throws SQLException
	 * @throws IOException
	 */
	private void arrayElement(ArrayDescriptor arrayLevel3, int k, JsonGenerator generator, String columnName,
			Object element1, StructDescriptor structDescriptor1) throws SQLException, IOException {

		boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
		if (type) {
			this.processVarraysElement(generator, element1, columnName);
		} else {
			this.processStruct2(generator, k, element1, columnName, structDescriptor1);
		}
	}

	/**
	 * This method will process the resultset and Writes each field from the
	 * resultset to the payload.
	 *
	 * @param objdata the objdata
	 * @param rs      the rs
	 * @throws IOException
	 */
	private void processResultset(ObjectData objdata, ResultSet rs) throws IOException {
		try {
			while (rs.next()) {
				try (CustomPayloadUtil load = new CustomPayloadUtil(rs, con)) {
					response.addPartialResult(objdata, OperationStatus.SUCCESS,
							OracleDatabaseConstants.SUCCESS_RESPONSE_CODE,
							OracleDatabaseConstants.SUCCESS_RESPONSE_MESSAGE, load);
				}
			}
			response.finishPartialResult(objdata);
		} catch (SQLException e) {
			throw new ConnectorException(e.toString());
		}

	}

}
