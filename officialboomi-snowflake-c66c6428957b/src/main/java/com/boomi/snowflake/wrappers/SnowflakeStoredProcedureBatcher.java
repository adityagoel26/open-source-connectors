// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.wrappers;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.snowflake.util.ConnectionProperties.ConnectionGetter;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

public class SnowflakeStoredProcedureBatcher extends SnowflakeWrapper {

	private static final Logger LOG = LogUtil.getLogger(SnowflakeStoredProcedureBatcher.class);
	private Long _batchSize;
	private Long _callsLeft;
	private String _storedProcedureName;
	private String[] _arguments;
	private String _currentCommand;
	private static final String SQL_COMMAND_CALL = "CALL ";
	private static final String SQL_COMMAND_NULL = "NULL ";
	private static final int THRESHHOLD = 500000;

	/**
	 * Constructs a new instance of SnowflakeStoredProcedureBatcher.
	 * @param getter The {@link ConnectionGetter} instance used to get the connection.
	 * @param connectionTimeFormat The {@link ConnectionTimeFormat} instance for handling time formatting.
	 * @param logger The {@link Logger} instance for logging purposes.
	 * @param tableName The name of the stored procedure, formatted as a string with schema, name, and parameters.
	 * @param batchSize The size of the batch to process stored procedure calls.
	 * @throws ConnectorException If the provided tableName is not correctly formatted as a stored procedure.
	 * This constructor initializes the batcher by splitting the `tableName` to extract the stored procedure
	 * name and its arguments.
	 * It verifies that the `tableName` contains the expected number of components (schema, stored procedure name,
	 * parameters, etc.).
	 * The parameters are extracted and stored in the `_arguments` array.
	 * The `_batchSize` and `_callsLeft` are set to the provided `batchSize`, and the current command is initialized
	 * as an empty string.
	 */
	public SnowflakeStoredProcedureBatcher(ConnectionGetter getter, ConnectionTimeFormat connectionTimeFormat,
			Logger logger, String tableName, Long batchSize) {
		super(getter, connectionTimeFormat, logger, tableName);
		LOG.entering(this.getClass().getCanonicalName(), "SnowflakeStoredProcedureBatcher()");
		_batchSize = _callsLeft = batchSize;
		String[] storedProcedureDataArray = tableName.split("\\.");
		if (storedProcedureDataArray.length != 5) {
			throw new ConnectorException("The imported Request Profile is not a stored procedure");
		}
		_storedProcedureName = storedProcedureDataArray[2];
		String parameters = (storedProcedureDataArray[3].replace("(", "").replace(")", ""));
		if(parameters.trim().length() == 0) {
			_arguments = new String[0];
		}else {
			_arguments = parameters.split("\\,");
		}
		_currentCommand = "";
	}

	/**
	 * constructs SQL command for the stored procedure call and append it to the rest of the calls
	 * @param callObj JSON object that has the data of the SQL call
	 */
	private void addToCommandString(SortedMap<String, String> callObj) {
		LOG.entering(this.getClass().getCanonicalName(), "addToCommandString()");
		_currentCommand += SQL_COMMAND_CALL + _storedProcedureName + "(";
		for (int i = 0; i < _arguments.length; i++) {
			if (i != 0) {
				_currentCommand += " , ";
			}
			String paramName = _arguments[i].trim().split(" ")[0];
			if (callObj.containsKey(paramName)) {
				_currentCommand += "'" + callObj.get(paramName) + "'";
				callObj.remove(paramName);
			} else {
				_currentCommand += SQL_COMMAND_NULL;
			}
		}
		_currentCommand += ");\n";
		if (callObj.size() != 0){
			throw new ConnectorException("Recevied JSON object doesn't match Stored Procedure' arguments");
		}
	}

	/**
	 * converts keys that contains name and dataType of each argument to name only
	 * @param callObj object that has the data that is passed to the stored procedure
	 * @return
	 */
	private SortedMap<String, String> refineObject(SortedMap<String, String> callObj) {
		SortedMap<String, String> result = new TreeMap<String, String>();
		while(callObj.isEmpty() == false) {
			String key = callObj.firstKey();
			String nKey = key.split(" ")[0];
			result.put(nKey, callObj.get(key));
			callObj.remove(key);
		}
		return result;
	}

	/**
	 * adds a call then if no calls should be added it issues commit and retrieve Data
	 * @param jsonCall             object that has the data of the SP parameters' values
	 * @param requestDataArray holds an array of input data from the previous shape
	 * @param response
	 * @param dynamicPropertyMap A map containing dynamic properties such as database and schema, used for overriding
	 *                            connection properties during execution.
	 */
	public void addCall(SortedMap<String, String> jsonCall, List<ObjectData> requestDataArray,
			OperationResponse response, DynamicPropertyMap dynamicPropertyMap) {
		LOG.entering(this.getClass().getCanonicalName(), "addCall()");
		if (_currentCommand.length() >= THRESHHOLD) {
			commitAndRetrieveData(requestDataArray, response, dynamicPropertyMap);
		}

		_callsLeft--;
		addToCommandString(refineObject(jsonCall));
		if (_callsLeft != 0) {
			return;
		}
		commitAndRetrieveData(requestDataArray, response, dynamicPropertyMap);
	}
	/**
	 * executes generated multi statements, then retrieve data row by row
	 * @param requestDataArray holds an array of input data from the previous shape
	 * @param response
	 * @param dynamicPropertyMap A map containing dynamic properties such as database and schema,
	 * 							 used for overriding connection properties during execution.
	 */
	public void commitAndRetrieveData(List<ObjectData> requestDataArray, OperationResponse response,
			DynamicPropertyMap dynamicPropertyMap) {
		executeMultiStatement(_currentCommand, _batchSize - _callsLeft, dynamicPropertyMap);
		InputStream row = null;
		try {
			while(null != (row = getNextRowFromMultiStatement(dynamicPropertyMap))) {
				requestDataArray.get(_currentStatement).getLogger().info("started getting results");
				ResponseUtil.addSuccess(response, requestDataArray.get(_currentStatement), "0",
						ResponseUtil.toPayload(row));
				IOUtil.closeQuietly(row);
			}
			requestDataArray.clear();
			_currentCommand = "";
			_callsLeft = _batchSize;
		}catch (SQLException e) {
			throw new ConnectorException("Unable to fetch data from snowflake", e);
		}finally {
			IOUtil.closeQuietly(row);
		}
	}
}
