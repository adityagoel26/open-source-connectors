// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.operations;

import org.junit.Before;
import org.junit.Test;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.util.StreamUtil;
import com.boomi.connector.api.PropertyMap;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SnowflakeCommandsTest {
    private Logger _logger = mock(Logger.class);
	private SnowflakeConnection _snfConnection = mock(SnowflakeConnection.class);
	private UpdateRequest _request = mock(UpdateRequest.class);
	private OperationResponse _response = mock(OperationResponse.class);
	private OperationContext _context = mock(OperationContext.class);
	private PropertyMap _operationProperties = mock(PropertyMap.class);
	private SnowflakeCommandOperation _operation = mock(SnowflakeCommandOperation.class);
	private ObjectData _objectData = mock(ObjectData.class);
	private Connection _connection = mock(Connection.class);
	private PreparedStatement _preparedStatement = mock(PreparedStatement.class);
	private ResultSet _resultSet = mock(ResultSet.class);
	private ResultSetMetaData _resultSetMetaData = mock(ResultSetMetaData.class);
	private ConnectorException _exception;
	
	private static final String CUSTOM_OPEARTION_TYPE = "copyIntoTable";
	private static final long DEFAULT_LONG_VALUE = 4;
	private static final long SIZE = 200L;
	private static final String ERROR_MESSAGE = "Error message";
	private static final int COL_COUNT = 5;
	private static final String COL_NAME = "col name";
	
	
	@Before
	public void setup() throws IOException {
		_exception = new ConnectorException(ERROR_MESSAGE);
		when(_snfConnection.getAWSSecret()).thenReturn("");
		when(_snfConnection.getAWSAccessKey()).thenReturn("");
		when(_snfConnection.createJdbcConnection()).thenReturn(_connection);
		when(_snfConnection.getContext()).thenReturn(_context);
		
		when(_operationProperties.getProperty(any(String.class), any(String.class))).thenReturn("");
		when(_operationProperties.getLongProperty(any(String.class), any(long.class))).thenReturn(DEFAULT_LONG_VALUE);
		when(_operationProperties.getOrDefault(any(String.class), any(String.class))).thenReturn("");
		
		when(_context.getOperationProperties()).thenReturn(_operationProperties);
		when(_context.getCustomOperationType()).thenReturn(CUSTOM_OPEARTION_TYPE);
		
		when(_response.getLogger()).thenReturn(_logger);
		
        when(_objectData.getLogger()).thenReturn(_logger);
        when(_objectData.getData()).thenReturn(StreamUtil.EMPTY_STREAM);
        when(_objectData.getDataSize()).thenReturn(SIZE);
		
		 when(_request.iterator()).thenReturn(Collections.singletonList(_objectData).iterator());
		
		_operation  = new SnowflakeCommandOperation(_snfConnection);
		
		when(_operation.getContext()).thenReturn(_context);
	}
	
	@Test(expected = ConnectorException.class)
	public void shouldAddApplicationErrorWhenInstantiationOfConnectionFails(){
		when(_snfConnection.createJdbcConnection()).thenThrow(_exception);
		executeSnowflakeCommandOperation();
		verifyAddApplicationErrorResultFromException(_exception);
	}

    @Test(expected = ConnectorException.class)
	public void shouldAddApplicationErrorWhenCreationOfPreparedStatementFails() throws SQLException {
		when(_snfConnection.createJdbcConnection()).thenReturn(_connection);
		when(_connection.prepareStatement(anyString())).thenThrow(_exception);
		executeSnowflakeCommandOperation();
		verifyAddApplicationErrorResultFromException(_exception);
		verifyConnectionIsClosed();
	}

    @Test(expected = ConnectorException.class)
	public void shouldAddApplicationErrorWhenExecutionOfCommandFails() throws SQLException {
		when(_snfConnection.createJdbcConnection()).thenReturn(_connection);
		when(_connection.prepareStatement(anyString())).thenReturn(_preparedStatement);
		when(_preparedStatement.executeQuery()).thenThrow(_exception);
		executeSnowflakeCommandOperation();
		verifyAddApplicationErrorResultFromException(_exception);
		verifyPreparedStatementIsClosed();
		verifyConnectionIsClosed();
	}

    @Test(expected = ConnectorException.class)
	public void shouldAddApplicationErrorWhenResultSetFails() throws SQLException {
		when(_snfConnection.createJdbcConnection()).thenReturn(_connection);
		when(_connection.prepareStatement(anyString())).thenReturn(_preparedStatement);
		when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
		when(_resultSet.next()).thenReturn(true);
		when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
		when(_resultSetMetaData.getColumnCount()).thenReturn(COL_COUNT);
		when(_resultSetMetaData.getColumnName(any(Integer.class))).thenReturn(COL_NAME);
		when(_resultSet.getObject(any(Integer.class))).thenReturn(new Object());
		when(_resultSetMetaData.getColumnType(any(Integer.class))).thenReturn(java.sql.Types.VARCHAR);
		when(_resultSet.getString(any(Integer.class))).thenThrow(_exception);
		executeSnowflakeCommandOperation();
		verifyAddApplicationErrorResultFromException(_exception);
		verifyResultSetIsClosed();
		verifyPreparedStatementIsClosed();
		verifyConnectionIsClosed();
	}
	
	private void executeSnowflakeCommandOperation() {
		_operation.executeUpdate(_request, _response);
	}

    private void verifyLogger() {
        verify(_objectData).getLogger();
        verify(_logger).log(eq(Level.WARNING), any(String.class));
    }
	private void verifyAddApplicationErrorResultFromException(ConnectorException e) {
	     verifyLogger();
	     verify(_response).addResult(eq(_objectData),
	    		 eq(OperationStatus.APPLICATION_ERROR),
	    		 eq(e.getStatusCode()), 
	    		 any(String.class),
	    		 any(Payload.class));
	 }
	
	private void verifyConnectionIsClosed() throws SQLException {
		verify(_connection, times(1)).close();
	}
	
	private void verifyPreparedStatementIsClosed() throws SQLException {
		verify(_preparedStatement, times(1)).close();
	}
	private void verifyResultSetIsClosed() throws SQLException {
		verify(_resultSet, times(1)).close();
	}
}
