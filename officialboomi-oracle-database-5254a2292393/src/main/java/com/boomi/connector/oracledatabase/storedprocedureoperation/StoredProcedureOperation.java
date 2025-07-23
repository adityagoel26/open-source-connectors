// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.storedprocedureoperation;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
import java.sql.Connection;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;

/**
 * @author swastik.vn
 *
 */
public class StoredProcedureOperation extends SizeLimitedUpdateOperation {

	public StoredProcedureOperation(OracleDatabaseConnection conn) {
		super(conn);
	}

	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		OracleDatabaseConnection conn = getConnection();
		String procedureName = getContext().getObjectTypeId();
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties()
					.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			con.setAutoCommit(false);
			Long batchCount = getContext().getOperationProperties().getLongProperty(BATCH_COUNT);
			Long maxFieldSize = getContext().getOperationProperties().getLongProperty("maxFieldSize");
			Long fetchSize = getContext().getOperationProperties().getLongProperty(FETCH_SIZE);
			String procedure = null;
			if (procedureName!= null && procedureName.lastIndexOf(".") > 0) {
				procedure = procedureName.substring(procedureName.lastIndexOf(".") + 1, procedureName.length());
			} else {
				procedure = procedureName;
			}
			StoredProcedureExecute execute = new StoredProcedureExecute(con, procedure, procedureName, request,
					response, getContext());
			int readTimeout = getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : DEFAULT_VALUE;
			execute.executeStatements(batchCount, maxFieldSize, readTimeout, fetchSize);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}

	}

	/**
	 * Gets the Connection instance
	 */
	@Override
	public OracleDatabaseConnection getConnection() {
		return (OracleDatabaseConnection) super.getConnection();
	}
}