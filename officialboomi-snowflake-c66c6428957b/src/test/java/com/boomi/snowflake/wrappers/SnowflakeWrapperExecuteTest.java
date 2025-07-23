// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.wrappers;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import net.snowflake.client.jdbc.SnowflakeStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
public class SnowflakeWrapperExecuteTest {
    @Mock
    private ConnectionProperties.ConnectionGetter _getter ;
    @Mock
    private Statement statement;
    @Spy
    private SnowflakeStatement snowflakeStatement;
    @Mock
    ConnectionTimeFormat connectionTimeFormat;
    @Mock
    private Connection mockConnection;
    @Mock
    private SnowflakeWrapper _snowflakeWrapper;
    @Mock
    private  SnowflakeConnection _connection ;

    @Before
    public void setup() throws SQLException {
        MockitoAnnotations.initMocks(this);

        Mockito.when(_connection.getOperationContext())
                .thenReturn(new SnowflakeContextIT(OperationType.EXECUTE, "EXECUTE"));
        Mockito.when(_connection.createJdbcConnection()).thenReturn(mockConnection);
        Mockito.when(_connection.createJdbcConnection().createStatement()).thenReturn(statement);
        Mockito.when(mockConnection.getCatalog()).thenReturn("TEST_CATALOG");
        Mockito.when(mockConnection.getSchema()).thenReturn("PUBLIC");

        ConnectionProperties properties = new ConnectionProperties(_connection, new MutablePropertyMap(),
                "TEST_TABLE", Logger.getAnonymousLogger());
        _snowflakeWrapper = new SnowflakeWrapper(properties.getConnectionGetter(),
                connectionTimeFormat,
                properties.getLogger(),
                properties.getTableName());
    }
    @Test
    public void testExecuteMultiStatement() throws SQLException {
        String sqlCommands = "CALL \"FIND_EMPLOYEE_BY_ID\"('6');";
        Long numberOfStatements = 2L;
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, "TEST_SF_DB");
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, "DEV");

        Mockito.when(_getter.getStatement()).thenReturn(statement);
        Mockito.when(statement.unwrap(SnowflakeStatement.class)).thenReturn(snowflakeStatement);
        Mockito.when(statement.getConnection()).thenReturn(mockConnection);
        _snowflakeWrapper.executeMultiStatement(sqlCommands, numberOfStatements, dynamicOpProps);

        Mockito.verify(snowflakeStatement).setParameter("MULTI_STATEMENT_COUNT", numberOfStatements);
        Mockito.verify(statement).execute(sqlCommands);
    }

    @Test
    public void testExecuteMultiStatementWithDynamicPropertyMap() throws SQLException {
        String sqlCommands = "INSERT INTO TEST_TABLE VALUES (1, 'test');";
        Long numberOfStatements = 1L;
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, "TEST_SF_DB");
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, "DEF");

        Mockito.when(_getter.getStatement()).thenReturn(statement);
        Mockito.when(statement.unwrap(SnowflakeStatement.class)).thenReturn(snowflakeStatement);
        Mockito.when(statement.getConnection()).thenReturn(mockConnection);
        _snowflakeWrapper.executeMultiStatement(sqlCommands, numberOfStatements, dynamicOpProps);
        Mockito.verify(snowflakeStatement).setParameter("MULTI_STATEMENT_COUNT", numberOfStatements);
        Mockito.verify(statement).execute(sqlCommands);
    }
}
