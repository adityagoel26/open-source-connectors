// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.wrappers;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.LogUtil;

/**
 * The Class SnowflakeTableStream.
 *
 * @author Vanangudi,S
 */
public class SnowflakeTableStream extends SnowflakeWrapper {
	
	/** The Constant LOG. */
	private static final Logger LOG = LogUtil.getLogger(SnowflakeTableStream.class);
	/** The Batch Size. */
	private final long _batchSize;
	/** The Filter Object. */
	private final SortedMap<String, String> _filterObj;
	/** The Selected Column Names. */
	private final String _selectedColumnsStr;
	/** The Order Items Name. */
	private final String _orderItemsStr;
	/** The Result Set Object. */
	private ResultSet _resultSet;
	/** The Empty Flag option. */
	private Boolean _emptyFlag;
	/** The Prepared Statement object. */
	private PreparedStatement _preparedStatement;
	/** The Document Batching Option. */
	private final Boolean _documentBatching;
	/** The list of filter by expressions. */
	private final List<Map<String, String>> _expressionList;
	
	/**
	 * Instantiates a new Snowflake Table Stream
	 * 
	 * @param properties of operations and connection
	 * @param selectedColumns to be retrieved
	 * @param filterObj used to filter data
	 * @param orderItems to sort data
	 * @param expressionList to get filter data
	 * @param dynamicPropertyMap a DynamicPropertyMap containing dynamic properties for the query
	 */
	public SnowflakeTableStream(ConnectionProperties properties,
			List<String> selectedColumns,
			SortedMap<String, String> filterObj,
			List<String> orderItems,
			List<Map<String, String>> expressionList, DynamicPropertyMap dynamicPropertyMap) {
		super(properties.getConnectionGetter(), properties.getConnectionTimeFormat(), properties.getLogger(),
				properties.getTableName());
		LOG.entering(SnowflakeTableStream.class.getCanonicalName(), "SnowflakeTableStream()");
		_batchSize = properties.getBatchSize();
		_documentBatching = properties.getDocumentBatching();
		_filterObj = filterObj;
		_expressionList = expressionList;
		_selectedColumnsStr = selectedColumns.isEmpty() ? "*"
				: (DOUBLE_QUOTES + String.join(DOUBLE_QUOTES + COMMA_DELIMITER + DOUBLE_QUOTES, selectedColumns)
				+ DOUBLE_QUOTES);
		_orderItemsStr = orderItems.isEmpty() ? "" : (SQL_COMMAND_ORDER_BY + String.join(COMMA_DELIMITER, orderItems));
		_emptyFlag = true;
		_preparedStatement = null;
		executeQuery(dynamicPropertyMap);
	}

	/**
	 * Execute the SQL Statement for fetching the result
	 *
	 * @param dynamicPropertyMap a DynamicPropertyMap containing dynamic properties for the query
	 */
	private void executeQuery(DynamicPropertyMap dynamicPropertyMap) {
		StringJoiner sqlCommand = new StringJoiner("");
		sqlCommand.add(SQL_COMMAND_SELECT)
				.add(_selectedColumnsStr)
				.add(SQL_COMMAND_FROM)
				.add(_tableName)
				.add((null == _expressionList ? sqlConstructWhereClause(_filterObj)
						: sqlConstructQueryWhereClause(_expressionList)))
				.add(_orderItemsStr);
		_processLogger.fine(() -> String.format("SQL: %s", sqlCommand));
		_preparedStatement = createPreparedStatement(sqlCommand.toString(),dynamicPropertyMap);
		if(null != _expressionList){
			fillStatementValuesWithDataTypeForQuery(_preparedStatement, _expressionList);
		} else {
			fillObjectDefination(filterObjectDefinition, _filterObj);
			SortedMap<String, String> metadata = null;
			if (null != _tableName) {
				metadata = SnowflakeOperationUtil.getTableMetadata(_tableName, _getter.getConnection(_processLogger),
						dynamicPropertyMap.getProperty(SnowflakeOverrideConstants.DATABASE),
						dynamicPropertyMap.getProperty(SnowflakeOverrideConstants.SCHEMA));
			}
			fillStatementValuesWithDataType(_preparedStatement, new TreeMap<>(), _filterObj, metadata);
		}
		_resultSet = executePreparedStatement(_preparedStatement);
	}
	
	/**
	 * Checks if there is result set available to process
	 * @return false if there are no data left true otherwise
	 */
	public boolean next() {
		LOG.entering(SnowflakeTableStream.class.getCanonicalName(), "next()");
		if(!_emptyFlag) {
			return false;
		}

		try {
			if (_resultSet == null || !_resultSet.next()) {
				_emptyFlag = false;
				return _emptyFlag;
			}
			return true;
		}catch(SQLException e) {
			throw new ConnectorException("Unable to fetch data from database", e);
		}
	}

	/**
	 * Get the Rows in Batch Wise
	 * 
	 * @return stream containing JSON string represents one row from the database
	 */
	public InputStream getBatchedRow() {
		LOG.entering(SnowflakeTableStream.class.getCanonicalName(), "getBatch()");
		return _documentBatching ? resultSetToStreamBatchWise(_resultSet, _batchSize) : resultSetToStreams(_resultSet);
	}

	/**
	 * close reader
	 */
	public void closeReader() {
		LOG.entering(SnowflakeTableStream.class.getCanonicalName(), "closeReader()");
		try {
			closePreparedStatement(_preparedStatement);
			if(_resultSet != null) {
				_resultSet.close();
			}
		}catch(SQLException e) {
			throw new ConnectorException("Unable to close ResultSet", e);
		}finally {
			close();
		}
	}

}
