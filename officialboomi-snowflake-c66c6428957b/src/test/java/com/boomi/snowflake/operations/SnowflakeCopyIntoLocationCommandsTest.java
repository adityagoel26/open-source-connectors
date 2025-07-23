// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.util.StreamUtil;
import com.boomi.connector.api.PropertyMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * SnowflakeCopyIntoLocationCommandsTest for copy into location.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({ SnowflakeCommandOperation.class})
public class SnowflakeCopyIntoLocationCommandsTest {

    private Logger _logger = Mockito.mock(Logger.class);
    private SnowflakeConnection _snfConnection = Mockito.mock(SnowflakeConnection.class);
    private UpdateRequest _request = Mockito.mock(UpdateRequest.class);
    private OperationResponse _response = Mockito.mock(OperationResponse.class);
    private OperationContext _context = Mockito.mock(OperationContext.class);
    private PropertyMap _operationProperties = Mockito.mock(PropertyMap.class);
    private SnowflakeCommandOperation _operation = Mockito.mock(SnowflakeCommandOperation.class);
    private ObjectData _objectData = Mockito.mock(ObjectData.class);
    private Connection _connection = Mockito.mock(Connection.class);
    private PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private ConnectorException _exception;

    private static final String CUSTOM_OP_TYPE = "copyIntoLocation";
    private static final long DEFAULT_LONG_VALUE = 4;
    private static final long SIZE = 200L;
    private static final String ERROR_MESSAGE = "Error message";
    private static final int COL_COUNT = 5;
    private static final String COL_NAME = "col name";

    /**
     * AWS_BUCKET_NAME.
     */
    private static final String AWS_BUCKET_NAME = "awsBucketName";
    /**
     * AWS_REGION.
     */
    private static final String AWS_REGION = "awsRegion";
    /**
     * DB Constant
     */
    private static final String DB = "db";
    /**
     * SCHEMA Constant
     */
    private static final String SCHEMA = "schema";

    /**
     * dbDynamicOperationProperty
     */
    private final String dbDynamicOperationProperty;
    /**
     * schemaDynamicOperationProperty
     */
    private final String schemaDynamicOperationProperty;

    /**
     * Constructor for DynamicOperationProperty db and schema
     */
    public SnowflakeCopyIntoLocationCommandsTest(String dbDynamicOperationProperty,
            String schemaDynamicOperationProperty) {
        this.dbDynamicOperationProperty = dbDynamicOperationProperty;
        this.schemaDynamicOperationProperty = schemaDynamicOperationProperty;
    }

    /**
     * Parameterized for Copy into location operation
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Test executeSizeLimitedUpdate with DynamicOperationProperty DB and SCHEMA - valid value.
                { "DBDynamicOperationPropertyTest1", "SchemaDynamicOperationPropertyTest1" },
                // Test executeSizeLimitedUpdate with DynamicOperationProperty DB and SCHEMA - DB valid and SCHEMA empty.
                { "DBDynamicOperationPropertyTest1", "" },
                // Test executeSizeLimitedUpdate with DynamicOperationProperty DB and SCHEMA - DB empty and SCHEMA valid.
                { "", "SchemaDynamicOperationPropertyTest1" },
                // Test executeSizeLimitedUpdate with DynamicOperationProperty DB and SCHEMA - DB empty and SCHEMA empty.
                { "", "" },
                // Test executeSizeLimitedUpdate with DynamicOperationProperty DB and SCHEMA - DB and SCHEMA whitespaces.
                { " ", " " },
        });
    }

    /**
     * set up for Copy into location operation
     */
    @Before
    public void setup() throws IOException {
        _exception = new ConnectorException(ERROR_MESSAGE);
        Mockito.when(_snfConnection.getAWSSecret()).thenReturn("");
        Mockito.when(_snfConnection.getAWSAccessKey()).thenReturn("");
        Mockito.when(_snfConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_snfConnection.getContext()).thenReturn(_context);
        Mockito.when(_snfConnection.getOperationContext()).thenReturn(_context);

        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("enablePooling",false);
        Mockito.when(_context.getConnectionProperties()).thenReturn(propertyMap);

        Mockito.when(_operationProperties.getProperty(ArgumentMatchers.any(String.class), ArgumentMatchers.any(String.class))).thenReturn("");
        Mockito.when(_operationProperties.getLongProperty(ArgumentMatchers.any(String.class), ArgumentMatchers.any(long.class))).thenReturn(DEFAULT_LONG_VALUE);
        Mockito.when(_operationProperties.getOrDefault(ArgumentMatchers.any(String.class), ArgumentMatchers.any(String.class))).thenReturn("");

        Mockito.when(_context.getOperationProperties()).thenReturn(_operationProperties);
        Mockito.when(_context.getCustomOperationType()).thenReturn(CUSTOM_OP_TYPE);

        Mockito.when(_response.getLogger()).thenReturn(_logger);

        Mockito.when(_objectData.getLogger()).thenReturn(_logger);
        Mockito.when(_objectData.getData()).thenReturn(StreamUtil.EMPTY_STREAM);
        Mockito.when(_objectData.getDataSize()).thenReturn(SIZE);

        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(AWS_BUCKET_NAME,"S3_TEST_BUCKET");
        dynamicOpProps.addProperty(AWS_REGION, "us-east-1");
        dynamicOpProps.addProperty(DB, dbDynamicOperationProperty);
        dynamicOpProps.addProperty(SCHEMA, schemaDynamicOperationProperty);

        Mockito.when(_objectData.getDynamicOperationProperties()).thenReturn(dynamicOpProps);

        Mockito.when(_request.iterator()).thenReturn(Collections.singletonList(_objectData).iterator());

        _operation  = new SnowflakeCommandOperation(_snfConnection);
    }

    /**
     * override db and schema for dynamic operation properties for Copy into location operation
     */
    @Test
    public void overridingDynamicOperationPropertiesDBandSchema() throws SQLException {
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(COL_COUNT);
        Mockito.when(_resultSetMetaData.getColumnName(ArgumentMatchers.any(Integer.class))).thenReturn(COL_NAME);
        Mockito.when(_resultSet.getObject(ArgumentMatchers.any(Integer.class))).thenReturn(new Object());
        Mockito.when(_resultSetMetaData.getColumnType(ArgumentMatchers.any(Integer.class))).thenReturn(java.sql.Types.VARCHAR);
        Mockito.when(_resultSet.getString(ArgumentMatchers.any(Integer.class))).thenThrow(_exception);

        _operation.executeUpdate(_request, _response);

        //verify db and schema with connection
        Mockito.verify(_connection).setCatalog(dbDynamicOperationProperty.toUpperCase());
        Mockito.verify(_connection).setSchema(schemaDynamicOperationProperty.toUpperCase());
    }

}

