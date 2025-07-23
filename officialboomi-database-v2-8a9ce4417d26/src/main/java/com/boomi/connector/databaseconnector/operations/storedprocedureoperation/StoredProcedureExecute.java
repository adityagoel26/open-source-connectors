// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.storedprocedureoperation;

import oracle.jdbc.OracleType;
import oracle.jdbc.OracleTypes;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.BatchResponse;
import com.boomi.connector.databaseconnector.model.ProcedureResponseNonBatch;
import com.boomi.connector.databaseconnector.util.CustomPayloadUtil;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DBv2JsonUtil;
import com.boomi.connector.databaseconnector.util.ProcedureMetaDataUtil;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.databaseconnector.util.SchemaBuilderUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import org.json.JSONObject;
import org.postgresql.util.PGobject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * The Class StoredProcedureExecute.
 *
 * @author swastik.vn
 */
public class StoredProcedureExecute {

    /**
     * The List of Parameters present in the procedure.
     */
    private final List<String> _params;

    /**
     * The List of only IN Parameters present in the procedure.
     */
    private final List<String> _inParams;

    /**
     * The List of only OUT parameters present in the procedure.
     */
    private final List<String> _outParams;

    /**
     * The data type.
     */
    private final Map<String, Integer> _dataType;

    /**
     * The tracked data.
     */
    private final UpdateRequest _trackedData;

    /**
     * The response.
     */
    private final OperationResponse _response;

    /**
     * The sqlConnection.
     */
    private final Connection _sqlConnection;

    /**
     * The procedure name.
     */
    private final String _procedureNameWithPackage;

    /**
     * The schema name.
     */
    private final String _schemaName;

    /**
     * The operation context.
     */
    private final OperationContext _operationContext;

    /**
     * The database metaData.
     */
    private final DatabaseMetaData _databaseMetaData;

    /**
     * The reader.
     */
    private final ObjectReader _reader = DBv2JsonUtil.getObjectReader();

    /**
     * The Constant JSON_FACTORY.
     */
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    /**
     * The Constant logger.
     */
    private static final Logger LOG = Logger.getLogger(StoredProcedureExecute.class.getName());

    /**
     * Instantiates a new stored procedure helper.
     *
     * @param sqlConnection the sqlConnection
     * @param procedureName the table name
     * @param trackedData   the tracked data
     * @param response      the response
     * @param schemaName    the schema Name
     * @throws SQLException
     */
    public StoredProcedureExecute(Connection sqlConnection, String procedureName, UpdateRequest trackedData,
            OperationResponse response, OperationContext operationContext, String schemaName) throws SQLException {

        String procedure = SchemaBuilderUtil.getProcedureName(procedureName);
        String packageName = SchemaBuilderUtil.getProcedurePackageName(procedureName);
        _params = ProcedureMetaDataUtil.getProcedureParams(sqlConnection, procedure, packageName, schemaName);
        _inParams = ProcedureMetaDataUtil.getInputParams(sqlConnection, procedure, packageName, schemaName);
        _outParams = ProcedureMetaDataUtil.getOutputParams(sqlConnection, procedure, packageName, schemaName);
        _dataType = ProcedureMetaDataUtil.getProcedureMetadata(sqlConnection, procedure, packageName, schemaName);
        _trackedData = trackedData;
        _response = response;
        _sqlConnection = sqlConnection;
        _procedureNameWithPackage = procedureName;
        _operationContext = operationContext;
        _schemaName = schemaName;
        _databaseMetaData = sqlConnection.getMetaData();
    }

    /**
     * This method will create the Callable statement and provide the neccessary
     * parameters and execute the statements.
     *
     * @param batchCount   the batch count
     * @param maxFieldSize the max field size
     * @param fetchSize    the query fetch size
     * @param readTimeout  the read timeout
     * @throws JsonProcessingException the json processing exception
     * @throws SQLException            the SQL exception
     */
    public void executeStatements(Long batchCount, Long maxFieldSize, Long fetchSize, int readTimeout)
            throws JsonProcessingException, SQLException {
        StringBuilder query = getQuery();

        if ((batchCount != null) && (batchCount > 0)) {
            if (!_inParams.isEmpty() && _outParams.isEmpty()) {
                // We are extending SizeLimitUpdate Operation it loads only single document into memory
                // Hence, we are preparing the list of Object Data which will be required
                // for Statement batching and for creating the Query for Prepared Statement.
                List<ObjectData> batchData = new ArrayList<>();
                for (ObjectData objdata : _trackedData) {
                    batchData.add(objdata);
                }
                doBatch(batchCount, query, batchData, readTimeout);
            } else {
                throw new ConnectorException("Batching cannot be applied for non input parameter procedures");
            }
        } else if ((batchCount == null) || (batchCount == 0)) {
            doNonBatch(query, readTimeout, maxFieldSize, fetchSize, null);
            commitStoredProcedure();
        } else if (batchCount < 0) {
            throw new ConnectorException(DatabaseConnectorConstants.BATCH_COUNT_CANNOT_BE_NEGATIVE);
        }
    }

    /**
     * This method will construct the query based on the database type.
     * @return
     * @throws SQLException
     */
    public StringBuilder getQuery() throws SQLException {
        StringBuilder query;
        if (DatabaseConnectorConstants.MSSQLSERVER.equals(getDatabaseMetaData().getDatabaseProductName())) {
            // if MSSQL server then query constructing both IN and OUT params.
            query = QueryBuilderUtil.buildInitialQuerySqlDB(getParams(), getProcedureNameWithPackage(), _schemaName);
        } else {
            //  Except MSSQL database then query constructing both IN and OUT params.
            query = QueryBuilderUtil.buildProcedureQuery(getParams(), getProcedureNameWithPackage());
        }
        return query;
    }

    /**
     * This method will batch the jdbc statements according to the batch count
     * specified by the user.
     *
     * @param batchCount the batch count
     * @param query      the query
     * @param batchData  the batch data
     * @throws SQLException the SQL exception
     */
    private void doBatch(Long batchCount, StringBuilder query, List<ObjectData> batchData, int readTimeout)
            throws SQLException {
        int batchnum = 0;
        int objDataCount = 0;
        int currentDocIndex = 0;
        boolean shouldExecute = true;
        List<CallableStatement> cstmtList = new ArrayList<>();
        CallableStatement callableStatement = null;
        ObjectData trackedObjectData = null;
        try {
            for (ObjectData objectData : batchData) {
                Payload payload = null;
                objDataCount++;
                currentDocIndex++;
                trackedObjectData = objectData;
                try (InputStream inputStream = objectData.getData()) {
                    StringBuilder initialQuery = new StringBuilder(query);
                    // Here removing the constructed questions which is not passed from the process int json node
                    // parameters.
                    handleOracleQuestionMarks(inputStream, initialQuery);

                    callableStatement = _sqlConnection.prepareCall(initialQuery.toString());
                    callableStatement.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));

                    //Here we are verifying whether the process has passed the objectData size,
                    // and if not, we don't need to use the prepareStatements method.
                    if (objectData.getDataSize() > 0) {
                        prepareStatements(callableStatement, inputStream);
                    }
                    callableStatement.addBatch();
                    cstmtList.add(callableStatement);

                    if (objDataCount == batchCount) {
                        batchnum++;
                        if (shouldExecute) {
                            executeBatch(cstmtList, batchnum, objectData);
                        } else {
                            shouldExecute = true;
                            CustomResponseUtil.logFailedBatch(_response, batchnum, objDataCount);
                            CustomResponseUtil.batchExecuteError(objectData, _response, batchnum, objDataCount);
                        }
                        objDataCount = 0;
                    } else if (objDataCount < batchCount) {
                        int remainingBatch = batchnum + 1;
                        if (currentDocIndex == batchData.size()) {
                            executeRemaining(objectData, cstmtList, remainingBatch, objDataCount);
                            cstmtList.clear();
                        } else {
                            payload = JsonPayloadUtil.toPayload(
                                    new BatchResponse("Record added to batch successfully", remainingBatch,
                                            objDataCount));
                            ResponseUtil.addSuccess(_response, objectData,
                                    DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
                        }
                    }
                } catch (BatchUpdateException batchUpdateException) {
                    CustomResponseUtil.logFailedBatch(_response, batchnum, objDataCount);
                    CustomResponseUtil.batchExecuteError(batchUpdateException, objectData, _response, batchnum,
                            objDataCount);
                    objDataCount = 0;
                } catch (SQLException e) {
                    CustomResponseUtil.logFailedBatch(_response, batchnum, objDataCount);
                    shouldExecute = checkLastRecord(objDataCount, batchCount);
                    if (shouldExecute) {
                        objDataCount = 0;
                    }
                    CustomResponseUtil.writeSqlErrorResponse(e, objectData, _response);
                } catch (IOException | IllegalArgumentException e) {
                    shouldExecute = checkLastRecord(objDataCount, batchCount);
                    if (shouldExecute || (currentDocIndex == batchData.size())) {
                        callableStatement.clearBatch();
                        batchnum++;
                        CustomResponseUtil.logFailedBatch(_response, batchnum, objDataCount);
                        objDataCount = 0;
                    }
                    CustomResponseUtil.writeErrorResponse(e, objectData, _response);
                } finally {
                    IOUtil.closeQuietly(payload);
                }
            }
        } finally {
            gracefullyCloseCallableStatement(callableStatement, trackedObjectData);
            gracefullyCloseCallableStmtList(cstmtList, trackedObjectData);
        }
    }

    /**
     * Closes the CallableStatement and handles any exceptions that may occur during
     * the closing process.
     *
     * @param callableStatement The CallableStatement object to be closed.
     * @param trackedObjectData The ObjectData object being tracked.
     */
    private void gracefullyCloseCallableStatement(CallableStatement callableStatement, ObjectData trackedObjectData) {
        if (callableStatement != null) {
            try {
                callableStatement.close();
            } catch (SQLException e) {
                CustomResponseUtil.writeSqlErrorResponse(e, trackedObjectData, _response);
            }
        }
    }

    /**
     * Closes the List of CallableStatements and handles any exceptions that may occur during
     * the closing process.
     *
     * @param cstmtList         The List of CallableStatements object to be closed.
     * @param trackedObjectData The ObjectData object being tracked.
     */
    private void gracefullyCloseCallableStmtList(List<CallableStatement> cstmtList, ObjectData trackedObjectData) {
        for (CallableStatement cstmt : cstmtList) {
            try {
                cstmt.close();
            } catch (SQLException e) {
                CustomResponseUtil.writeSqlErrorResponse(e, trackedObjectData, _response);
            }
        }
    }

    /**
     * Executes the batch of CallableStatements and handles the response.
     *
     * @param cstmtList The list of CallableStatements to be executed in the batch.
     * @param batchnum The batch number.
     * @param objectData The ObjectData object containing the input parameters.
     * @throws SQLException
     */
    private void executeBatch(List<CallableStatement> cstmtList, int batchnum, ObjectData objectData)
            throws SQLException {
        int resLength = executeBatchRecords(cstmtList);
        if (!DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(_databaseMetaData.getDatabaseProductName())) {
            _sqlConnection.commit();
        }
        Payload payload = JsonPayloadUtil.toPayload(
                new BatchResponse("Batch executed successfully", batchnum, resLength));
        ResponseUtil.addSuccess(_response, objectData,
                DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
        cstmtList.clear();
    }

    /**
     * Handles the removal of constructed question marks from the SQL query when
     * the database product is Oracle, and the number of input parameters in the
     * JSON data is less than the number of question marks in the query.
     *
     * @param inputStream  The InputStream containing the input parameters.
     * @param initialQuery The StringBuilder containing the initial SQL query.
     * @throws SQLException
     * @throws IOException
     */
    private void handleOracleQuestionMarks(InputStream inputStream, StringBuilder initialQuery)
            throws SQLException, IOException {
        int inParamSize = _inParams.size();

        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(
                _databaseMetaData.getDatabaseProductName())) {
            JsonNode jsonNode = _reader.readTree(inputStream);
            int numOfRemove = inParamSize - jsonNode.size();
            QueryBuilderUtil.removeQuestionMarks(initialQuery, numOfRemove);
            inputStream.reset();
        }
    }

    /**
     * This method will execute all batch data
     * statement based on the incoming requests.
     *
     * @param cstmtList the list of CallableStatement
     * @return returns number of execution
     * @throws SQLException the SQL exception
     */
    private static int executeBatchRecords(List<CallableStatement> cstmtList) throws SQLException {
        int resLength = 0;
        for (CallableStatement callableStatement : cstmtList) {
            try {
                callableStatement.executeBatch();
                resLength++;
            } finally {
                callableStatement.clearBatch();
                callableStatement.close();
            }
        }
        return resLength;
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the csmt
     * @param inputStream       the input stream
     * @throws SQLException the SQL exception
     * @throws IOException  Signals that an I/O exception has occurred.
     */

    private void prepareStatements(CallableStatement callableStatement, InputStream inputStream)
            throws SQLException, IOException, DateTimeParseException {
        String databaseName = _databaseMetaData.getDatabaseProductName();
        JsonNode json;
        // After filtering out the inputs (which are more than 1MB) we are loading the
        // inputStream to memory here.
        json = _reader.readTree(inputStream);
        int inParamsIndex = 1;
            while (inParamsIndex <= _inParams.size()) {

                String inputColumnName = _inParams.get(inParamsIndex - 1);
                JsonNode jsonNode = json.get(inputColumnName);
                int inputColumnIndex = _params.indexOf(inputColumnName) + 1;

                // Here checking the input column is present or not for setting the value to prepared statement
                // and added else if condition for backward compatibility
                // if parameter not passed from the process other than Oracle database
                if (jsonNode != null) {
                    setPreparedStatementsForQueryBasedOnInputData(callableStatement, databaseName, inParamsIndex,
                            jsonNode, inputColumnName, inputColumnIndex);
                } else if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
                    callableStatement.setNull(inParamsIndex, Types.NULL);
                } else if (!DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
                    callableStatement.setNull(inputColumnIndex, Types.VARCHAR);
                }
                inParamsIndex++;
            }
    }

    /**
     * Method that will execute remaining records in the batch.
     *
     * @param objdata      the objdata
     * @param cstmtList    the list of csmt
     * @param batchnum     the batchnum
     * @param objDataCount the object data count
     */
    private void executeRemaining(ObjectData objdata, List<CallableStatement> cstmtList, int batchnum,
            int objDataCount) {

        Payload payload = null;
        try {
            int resLength = executeBatchRecords(cstmtList);
            payload = JsonPayloadUtil.toPayload(
                    new BatchResponse("Remaining records added to batch and executed successfully", batchnum,
                            resLength));
            ResponseUtil.addSuccess(_response, objdata, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, payload);
            if (!DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(_databaseMetaData.getDatabaseProductName())) {
                _sqlConnection.commit();
            }
        } catch (SQLException e) {
            CustomResponseUtil.logFailedBatch(_response, batchnum, objDataCount);
            CustomResponseUtil.writeSqlErrorResponse(e, objdata, _response);
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
     * This method will call the procedure and process the resultset based on the
     * OUT param.
     *
     * @param csmt            the csmt
     * @param objdata         the objdata
     * @param maxFieldSize    the max field size
     * @param payloadMetadata the Metadata for payload.
     * @throws SQLException the SQL exception
     * @throws IOException
     */
    private void callProcedure(CallableStatement csmt, ObjectData objdata, Long maxFieldSize, Long fetchSize,
            PayloadMetadata payloadMetadata)
            throws SQLException, IOException {

        if ((maxFieldSize != null) && (maxFieldSize > 0)) {
            csmt.setMaxFieldSize(maxFieldSize.intValue());
        }
        if ((fetchSize != null) && (fetchSize > 0)) {
            csmt.setFetchSize(fetchSize.intValue());
        }

        boolean result = csmt.execute();
        InputStream tempInputStream = null;
        Payload payload = null;

        try (ResultSet rs = csmt.getResultSet();
             // temporary outputStream to flush the content of JsonGenerator.
             OutputStream out = _operationContext.createTempOutputStream();
             JsonGenerator generator = JSON_FACTORY.createGenerator(out)) {
            if ((_outParams != null) && !_outParams.isEmpty()) {
                writeOutputParamsTo(generator, csmt);
                tempInputStream = _operationContext.tempOutputStreamToInputStream(out);
                _response.addResult(objdata, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                        DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                        PayloadUtil.toPayload(tempInputStream, payloadMetadata));
            } else if (result && (rs != null)) {
                processResultset(objdata, rs, payloadMetadata);
            } else if (!result && (rs == null)) {
                CustomResponseUtil.handleSuccess(objdata, _response, payloadMetadata, new ProcedureResponseNonBatch(
                        Integer.parseInt(DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE),
                        "Procedure Executed Successfully!!"));
            }

            LOG.info("Procedure called Successfully!!!");
        } finally {
            IOUtil.closeQuietly(tempInputStream, payload);
        }
    }

    /**
     * Writes the output parameters of a stored procedure or function to the JSON generator.
     *
     * @param generator The JsonGenerator object used to write the output parameters to the JSON response.
     * @param csmt      The CallableStatement object representing the executed stored procedure or function.
     * @throws IOException
     * @throws SQLException
     */
    private void writeOutputParamsTo(JsonGenerator generator, CallableStatement csmt)
            throws IOException, SQLException {
        generator.writeStartObject();
        String databaseProductName = _databaseMetaData.getDatabaseProductName();

        for (int i = 0; i <= (_outParams.size() - 1); i++) {
            String outParamName = _outParams.get(i);
            int outParamIndex = _params.indexOf(outParamName);
            int outParamIndexForPostgre = outParamIndex + 1;
            if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseProductName)) {
                if (csmt.getObject(outParamIndexForPostgre) != null) {
                    if (csmt.getObject(outParamIndexForPostgre) instanceof Blob) {
                        Blob b = csmt.getBlob(outParamIndexForPostgre);
                        byte[] byteArray = b.getBytes(1, (int) b.length());
                        String data = new String(byteArray, StandardCharsets.UTF_8);
                        generator.writeStringField(outParamName, data);
                        generator.flush();
                    } else if (Objects.equals(_dataType.get(outParamName), Types.REF_CURSOR)) {
                        processRefCursors(generator, csmt, i, databaseProductName);
                    }else {
                        generator.writeStringField(outParamName, csmt.getObject(outParamIndexForPostgre)
                                .toString().trim());
                        generator.flush();
                    }
                } else {
                    generator.writeStringField(outParamName, "");
                    generator.flush();
                }
            } else if (DatabaseConnectorConstants.ORACLE.equals(databaseProductName)) {
                if (Objects.equals(_dataType.get(outParamName),Types.REF_CURSOR)) {
                    processRefCursors(generator, csmt, i,databaseProductName);
                }
                // Here based on parameter name checking the condition and appending the value to the output.
                else{
                    writeResponseResultSetForOracle(csmt, generator, i);
                }
            } else {
                if (_dataType.get(outParamName) == Types.LONGVARBINARY) {
                    generator.writeStringField(outParamName,
                            new String(csmt.getBytes(outParamIndexForPostgre),
                                    StandardCharsets.UTF_8));
                    generator.flush();
                } else {
                    generator.writeStringField(outParamName,
                            csmt.getString(outParamIndexForPostgre));
                    generator.flush();
                }
            }
        }
        generator.writeEndObject();
        generator.flush();
    }

    /**
     * Processes a REF_CURSOR output parameter from a CallableStatement and writes the result set as a JSON array.
     * Each row is written as a JSON object, with each column serialized according to its SQL type.
     *
     * @param generator           the {@link JsonGenerator} used to write JSON output
     * @param csmt                the {@link CallableStatement} that contains the stored procedure output
     * @param outParamIndex       the index of the output parameter in the _outParams list
     * @param databaseProductName the name of the database product (used to apply database-specific cursor handling)
     * @throws SQLException       if a database access error occurs
     * @throws IOException        if an I/O error occurs while writing the JSON output
     */
    private void processRefCursors(JsonGenerator generator, CallableStatement csmt, int outParamIndex,
            String databaseProductName) throws SQLException, IOException {
        String outParam = _outParams.get(outParamIndex);
        try (ResultSet resultSet = DatabaseConnectorConstants.ORACLE.equals(databaseProductName)
                ? (ResultSet) csmt.getObject(outParam) : (ResultSet) csmt.getObject(
                _params.indexOf(outParam) + 1)) {
            // no data to process
            if (resultSet == null) {
                return;
            }
            generator.writeArrayFieldStart(outParam);

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                generator.writeStartObject();

                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    writeField(generator, resultSet, metaData, columnIndex);
                }

                generator.writeEndObject();
            }

            generator.writeEndArray();
            generator.flush();
        }
    }

    /**
     * Writes a single column value from the ResultSet to the JSON output.
     * The writing behavior depends on the SQL type of the column.
     *
     * @param generator   the {@link JsonGenerator} used to write JSON fields
     * @param resultSet   the {@link ResultSet} containing the current row of data
     * @param metaData    the {@link ResultSetMetaData} providing information about column types
     * @param columnIndex the 1-based index of the column to serialize
     * @throws SQLException if a database access error occurs when retrieving the column value
     * @throws IOException  if an I/O error occurs while writing the JSON output
     */
    private static void writeField(JsonGenerator generator, ResultSet resultSet, ResultSetMetaData metaData,
            int columnIndex) throws SQLException, IOException {
        String columnName = metaData.getColumnName(columnIndex);
        int columnType = metaData.getColumnType(columnIndex);

        switch (columnType) {
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                generator.writeNumberField(columnName, resultSet.getInt(columnName));
                break;
            case Types.NUMERIC:
                generator.writeNumberField(columnName, resultSet.getBigDecimal(columnName));
                break;
            case Types.VARCHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NVARCHAR:
            case Types.TIMESTAMP:
                generator.writeStringField(columnName, resultSet.getString(columnName));
                break;
            case Types.REAL:
            case Types.FLOAT:
                generator.writeNumberField(columnName, resultSet.getFloat(columnName));
                break;
            case Types.DECIMAL:
            case Types.DOUBLE:
                generator.writeNumberField(columnName, resultSet.getDouble(columnName));
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                generator.writeBooleanField(columnName, resultSet.getBoolean(columnName));
                break;
            case Types.CLOB:
                generator.writeStringField(columnName, resultSet.getClob(columnName).toString());
                break;
            default:
                generator.writeStringField(columnName, resultSet.getObject(columnName).toString().trim());
        }
        generator.flush();
    }

    /**
     * This method will process the resultset and Writes each field from the
     * resultset to the payload.
     *
     * @param objdata         the objdata
     * @param rs              the rs
     * @param payloadMetadata the metadata for payload
     */
    private void processResultset(ObjectData objdata, ResultSet rs, PayloadMetadata payloadMetadata) {

        CustomPayloadUtil load = null;
        try {
            while (rs.next()) {
                load = new CustomPayloadUtil(rs);
                load.setMetadata(payloadMetadata);
                _response.addPartialResult(objdata, OperationStatus.SUCCESS,
                        DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                        DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, load);
            }
            _response.finishPartialResult(objdata);
        } catch (SQLException e) {
            ResponseUtil.addExceptionFailure(_response, objdata, e);
        } finally {
            IOUtil.closeQuietly(load);
        }
    }

    /**
     * Execute the Non Batch Stored Procedure
     *
     * @param query           - Stored Procedure Query
     * @param readTimeout     - socket time out
     * @param maxFieldSize    - maximum field size
     * @param fetchSize       - fetch size
     * @param payloadMetadata - Any metadata that you want to add to the payload.
     */
    protected void doNonBatch(StringBuilder query, int readTimeout, Long maxFieldSize, Long fetchSize,
            PayloadMetadata payloadMetadata) throws SQLException {

        CallableStatement callableStatement = null;

        // We need to set autocommit false for PostgreSQL when working with refcursors
        if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(
                _databaseMetaData.getDatabaseProductName()) && _dataType.containsValue(Types.REF_CURSOR)){
            _sqlConnection.setAutoCommit(false);
        }
        for (ObjectData objectData : _trackedData) {
            try (InputStream inputStream = objectData.getData()) {
                StringBuilder initialQuery = new StringBuilder(query);
                // Here executing prepareCall function for query based on IN and OUT parameters.
                handleOracleQuestionMarks(inputStream, initialQuery);

                callableStatement = _sqlConnection.prepareCall(initialQuery.toString());
                callableStatement.setQueryTimeout(QueryBuilderUtil.convertReadTimeoutToSeconds(readTimeout));

                //Here we are verifying whether the process has passed the objectData size,
                // and if not, we don't need to use the prepareStatements method.
                if (!_inParams.isEmpty() && (objectData.getDataSize() > 0)) {
                    prepareStatements(callableStatement, inputStream);
                }
                if (!_params.isEmpty()) {
                    registerStoredProcOutputParamsTo(callableStatement);
                }
                callProcedure(callableStatement, objectData, maxFieldSize, fetchSize, payloadMetadata);
            } catch (IOException | IllegalArgumentException | DateTimeParseException e) {
                CustomResponseUtil.writeErrorResponse(e, objectData, _response);
            } catch (SQLException e) {
                CustomResponseUtil.writeSqlErrorResponse(e, objectData, _response);
            } catch (ConnectorException e) {
                ResponseUtil.addExceptionFailure(_response, objectData, e);
            } finally {
                gracefullyCloseCallableStatement(callableStatement, objectData);
            }
        }
    }

    /**
     * This method will commit the transaction for stored procedure.
     */
    private void commitStoredProcedure() {
        try {
            //In POSTGRESQL refcursors are tied to the transaction and the transaction ends immediately after the
            //procedure call, which closes the cursor before you can fetch its data, that's why we have set autoCommit
            //as false and then we are manually commiting the connection.
            if (!DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(_databaseMetaData.getDatabaseProductName())
            || (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(_databaseMetaData.getDatabaseProductName())&&
                    _dataType.containsValue(Types.REF_CURSOR))) {
                _sqlConnection.commit();
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * Registers the output parameters for the stored procedure or function with the given CallableStatement.
     *
     * @param callableStatement The CallableStatement object representing the stored procedure or function.
     * @throws SQLException
     */
    private void registerStoredProcOutputParamsTo(CallableStatement callableStatement) throws SQLException {
        for (int i = 0; i < _params.size(); i++) {
            String outParamName = _params.get(i);
            Integer outParamDatatype = _dataType.get(outParamName);

            if (_outParams.contains(outParamName)) {
                if (DatabaseConnectorConstants.ORACLE.equals(_databaseMetaData.getDatabaseProductName())) {
                    if (outParamDatatype == Types.OTHER) {
                        callableStatement.registerOutParameter(outParamName, OracleType.JSON);
                    }  else if(outParamDatatype == Types.REF_CURSOR) {
                        callableStatement.registerOutParameter(outParamName, OracleTypes.REF_CURSOR);
                    }  else {
                        callableStatement.registerOutParameter(outParamName, outParamDatatype);
                    }
                } else if (DatabaseConnectorConstants.POSTGRESQL.equals(_databaseMetaData.getDatabaseProductName()) &&
                        outParamDatatype == Types.REF_CURSOR) {
                    callableStatement.registerOutParameter(_params.indexOf(outParamName) + 1,
                            Types.OTHER);
                } else {
                    callableStatement.registerOutParameter(_params.indexOf(outParamName) + 1,
                            outParamDatatype);
                }
            }
        }
    }

    /**
     * This method will call the procedure and process the resultset based on the
     * OUT param.
     *
     * @param callableStatement the callableStatement
     * @param jsonGenerator     the Json Generator
     * @param outParamIndex     the outParam index
     * @throws SQLException the SQL exception
     * @throws IOException  the IO exception
     */
    private void writeResponseResultSetForOracle(CallableStatement callableStatement, JsonGenerator jsonGenerator,
            int outParamIndex) throws SQLException, IOException {
        if (callableStatement.getObject(_outParams.get(outParamIndex)) != null) {
            if (callableStatement.getObject(_outParams.get(outParamIndex)) instanceof Blob) {
                Blob blob = callableStatement.getBlob(_outParams.get(outParamIndex));
                byte[] byteArray = blob.getBytes(1, (int) blob.length());
                String data = new String(byteArray, StandardCharsets.UTF_8);
                jsonGenerator.writeStringField(_outParams.get(outParamIndex), data);
            } else {
                jsonGenerator.writeStringField(_outParams.get(outParamIndex),
                        String.valueOf(callableStatement.getObject(_outParams.get(outParamIndex))).trim());
            }
        } else {
            jsonGenerator.writeStringField(_outParams.get(outParamIndex), "");
        }
        jsonGenerator.flush();
    }

    /**
     * This method will set the prepared statement data for necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param databaseName      the database name
     * @param inParamsIndex     the inParam index
     * @param jsonNode          the jsonNode
     * @param inputColumnName   the input column name
     * @param inputColumnIndex  the input column index
     * @throws SQLException the SQL exception
     * @throws IOException  Signals that an I/O exception has occurred.
     */
    private void setPreparedStatementsForQueryBasedOnInputData(CallableStatement callableStatement, String databaseName,
            int inParamsIndex, JsonNode jsonNode, String inputColumnName, int inputColumnIndex)
            throws SQLException, IOException {
        if ((jsonNode == null) || jsonNode.isNull()) {
            // if parameter passing with null then need to set value datatype null
            setNullValueForParameter(callableStatement, databaseName, inParamsIndex, inputColumnName, inputColumnIndex);
        } else {
            int dataTypeValue = _dataType.get(_inParams.get(inParamsIndex - 1));
            setPreparedStatementForQuery(callableStatement, databaseName, jsonNode, inputColumnName, inputColumnIndex,
                    dataTypeValue);
        }
    }

    /**
     * This method will set the null for prepared statement data of the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param databaseName      the database name
     * @param inParamsIndex     the inParam index
     * @param inputColumnName   the input column name
     * @param inputColumnIndex  the input column index
     * @throws SQLException the SQL exception
     */
    private static void setNullValueForParameter(CallableStatement callableStatement, String databaseName,
            int inParamsIndex, String inputColumnName, int inputColumnIndex) throws SQLException {
        if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
            callableStatement.setNull(inParamsIndex, Types.NULL);
        } else if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setNull(inputColumnName, Types.VARCHAR);
        } else {
            callableStatement.setNull(inputColumnIndex, Types.VARCHAR);
        }
    }

    /**
     * This method will set dataTypeValue for the query with necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param databaseName      the database name
     * @param node              the node
     * @param inputColumnName   the input column Name
     * @param inputColumnIndex  the input column index
     * @param dataTypeValue     the data type value
     * @throws SQLException the SQL exception
     * @throws IOException  Signals that an I/O exception has occurred.
     */
    private static void setPreparedStatementForQuery(CallableStatement callableStatement, String databaseName,
            JsonNode node, String inputColumnName, int inputColumnIndex, int dataTypeValue)
            throws SQLException, IOException {
        switch (dataTypeValue) {
            case Types.OTHER:
                setPrepareStatementsForDataBase(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            case Types.VARCHAR:
            case Types.CLOB:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                setPrepareStatementsForVarchar(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                setPrepareStatementsForInteger(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            case Types.DATE:
                setPrepareStatementsForDate(callableStatement, inputColumnIndex, node, inputColumnName, databaseName);
                break;
            case Types.TIME:
                setPrepareStatementsForTime(callableStatement, inputColumnIndex, node, inputColumnName, databaseName);
                break;
            case Types.NVARCHAR:
                setPrepareStatementsForNVarchar(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                setPrepareStatementsForBoolean(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            case Types.BIGINT:
                setPrepareStatementsForBigInt(callableStatement, inputColumnIndex, node, inputColumnName, databaseName);
                break;
            case Types.DOUBLE:
            case Types.FLOAT:
                setPrepareStatementsForDouble(callableStatement, inputColumnIndex, node, inputColumnName, databaseName);
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setPrepareStatementsForDecimal(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            case Types.REAL:
                setPrepareStatementsForReal(callableStatement, inputColumnIndex, node, inputColumnName, databaseName);
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                setPrepareStatementsForBlob(callableStatement, inputColumnIndex, node, inputColumnName, databaseName);
                break;
            case Types.TIMESTAMP:
                setPrepareStatementsForTimestamp(callableStatement, inputColumnIndex, node, inputColumnName,
                        databaseName);
                break;
            default:
                break;
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForDataBase(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            OracleJsonFactory factory = new OracleJsonFactory();
            OracleJsonObject object = factory.createObject();
            JSONObject jsonObject = new JSONObject(node.toString());
            Iterator<String> keys = jsonObject.keys();
            while (keys.hasNext()) {
                String jsonKeys = keys.next();
                object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
            }
            callableStatement.setObject(inputColumnName, object, OracleType.JSON);
        } else if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
            PGobject jsonObject = new PGobject();
            jsonObject.setType("json");
            jsonObject.setValue(QueryBuilderUtil.unescapeEscapedStringFrom(node));
            callableStatement.setObject(inputColumnIndex, jsonObject);
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForVarchar(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setString(inputColumnName, QueryBuilderUtil.unescapeEscapedStringFrom(node));
        } else {
            callableStatement.setString(inputColumnIndex, QueryBuilderUtil.unescapeEscapedStringFrom(node));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForInteger(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setInt(inputColumnName,
                    Integer.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setInt(inputColumnIndex,
                    Integer.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForDate(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setString(inputColumnName,
                    node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, ""));
        } else if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
            callableStatement.setObject(inputColumnIndex,
                    LocalDate.parse(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            try {
                callableStatement.setDate(inputColumnIndex,
                        Date.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR + e);
            }
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForTime(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setTime(inputColumnName,
                    Time.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
            callableStatement.setObject(inputColumnIndex,
                    LocalTime.parse(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setTime(inputColumnIndex,
                    Time.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForNVarchar(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setString(inputColumnName, QueryBuilderUtil.unescapeEscapedStringFrom(node));
        } else {
            callableStatement.setString(inputColumnIndex, QueryBuilderUtil.unescapeEscapedStringFrom(node));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForBoolean(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setBoolean(inputColumnName,
                    Boolean.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setBoolean(inputColumnIndex,
                    Boolean.valueOf(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForBigInt(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setLong(inputColumnName,
                    Long.parseLong(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setLong(inputColumnIndex,
                    Long.parseLong(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForDouble(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setDouble(inputColumnName,
                    Double.parseDouble(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setDouble(inputColumnIndex,
                    Double.parseDouble(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForDecimal(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setBigDecimal(inputColumnName,
                    new BigDecimal(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setBigDecimal(inputColumnIndex,
                    new BigDecimal(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForReal(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setFloat(inputColumnName,
                    Float.parseFloat(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        } else {
            callableStatement.setFloat(inputColumnIndex,
                    Float.parseFloat(node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "")));
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     * @throws IOException  the IO exception
     */
    private static void setPrepareStatementsForBlob(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException, IOException {

        String blobData = node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
        try (InputStream stream =
                new ByteArrayInputStream(blobData.getBytes())) {
            if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
                callableStatement.setBinaryStream(inputColumnIndex, stream);
            } else if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
                callableStatement.setBlob(inputColumnName, stream);
            } else {
                callableStatement.setBlob(inputColumnIndex, stream);
            }
        }
    }

    /**
     * This method will provide the necessary parameters required for the Callable
     * statement based on the incoming requests.
     *
     * @param callableStatement the callableStatement
     * @param inputColumnIndex  the inputColumnIndex
     * @param node              the json node
     * @param inputColumnName   the inputColumnName
     * @param databaseName      the databaseName
     * @throws SQLException the SQL exception
     */
    private static void setPrepareStatementsForTimestamp(CallableStatement callableStatement, int inputColumnIndex,
            JsonNode node, String inputColumnName, String databaseName) throws SQLException {
        String timeStamp = node.toString().replace(DatabaseConnectorConstants.DOUBLE_QUOTE, "");
        if (DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime t = LocalDateTime.parse(timeStamp, formatter);
            callableStatement.setObject(inputColumnIndex, t);
        } else if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(databaseName)) {
            callableStatement.setTimestamp(inputColumnName, Timestamp.valueOf(timeStamp));
        } else {
            callableStatement.setTimestamp(inputColumnIndex, Timestamp.valueOf(timeStamp));
        }
    }

    /**
     * Get the current tracked data.
     *
     * @return the tracked data of type {@link UpdateRequest}
     */
    public UpdateRequest getTrackedData() {
        return _trackedData;
    }

    /**
     * Get the database metadata.
     * @return the database metadata of type {@link DatabaseMetaData}
     */
    public DatabaseMetaData getDatabaseMetaData() {
        return _databaseMetaData;
    }

    /**
     * Get the list of parameters.
     * @return the list of parameters of type {@link List<String>}
     */
    public List<String> getParams() {
        return _params;
    }

    /**
     * Get the procedure name with package.
     * @return the procedure name with package of type {@link String}
     */
    public String getProcedureNameWithPackage() {
        return _procedureNameWithPackage;
    }
}
