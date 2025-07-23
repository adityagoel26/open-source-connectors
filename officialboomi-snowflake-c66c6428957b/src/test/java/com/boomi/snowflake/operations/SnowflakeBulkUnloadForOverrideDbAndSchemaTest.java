// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.operations;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.stages.AmazonWebServicesHandler;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.util.StreamUtil;

import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSStaticCredentialsProvider;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.powermock.reflect.Whitebox;

import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The Class SnowflakeBulkUnloadForOverrideDbAndSchemaTest
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({ SnowflakeBulkUnloadOperation.class, AmazonWebServicesHandler.class, AmazonS3ClientBuilder.class})
public class SnowflakeBulkUnloadForOverrideDbAndSchemaTest extends BaseTestOperation {

    /**
     * The Constant OPERATION_NAME.
     */
    private static final String OPERATION_NAME = "bulkUnload";
    /**
     * The Constant DOC_COUNT.
     */
    private static final int DOC_COUNT = 1;
    /**
     * The Input Stream List.
     */
    private List<InputStream> inputs;
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
    public SnowflakeBulkUnloadForOverrideDbAndSchemaTest(String dbDynamicOperationProperty,
            String schemaDynamicOperationProperty) {
        this.dbDynamicOperationProperty = dbDynamicOperationProperty;
        this.schemaDynamicOperationProperty = schemaDynamicOperationProperty;
    }

    /**
     * Set up testContext
     */
    @Before
    public void setup() {
        testContext = new SnowflakeContextIT(OperationType.EXECUTE, OPERATION_NAME);
    }

    /**
     * Sets the Input Value
     *
     * @param value the input stream
     */
    private void setInputs(InputStream value) {
        inputs = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            inputs.add(value);
        }
    }

    /**
     * Parameterized for Bulk Unload operation
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
     * Test for overriding DB and schema in Dynamic Operation Properties
     */
    @Test
    public void overridingDyanamicOperationPropertiesDBandSchema() throws Exception {
        SnowflakeConnection conn = new SnowflakeConnection(testContext);
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkUnloadOperation snowflakeBulkUnloadOperation = new SnowflakeBulkUnloadOperation(conn);

        // Adding Dynamic operation properties
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(AWS_BUCKET_NAME, "S3_TEST_BUCKET");
        dynamicOpProps.addProperty(AWS_REGION, "us-east-1");
        dynamicOpProps.addProperty(DB, dbDynamicOperationProperty);
        dynamicOpProps.addProperty(SCHEMA, schemaDynamicOperationProperty);

        SimpleTrackedData inputDoc = new SimpleTrackedData(1, inputs.get(0), null, null,
                dynamicOpProps);
        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        //setting connection field state
        Connection connectionMock = PowerMockito.mock(Connection.class);
        Whitebox.setInternalState(conn, "connection", connectionMock);

        AmazonS3ClientBuilder mockBuilder = PowerMockito.mock(AmazonS3ClientBuilder.class);
        AmazonS3 mockS3Client = PowerMockito.mock(AmazonS3.class);

        // Mock the static method standard() to return amazon mock builder
        PowerMockito.mockStatic(AmazonS3ClientBuilder.class);
        Mockito.when(AmazonS3ClientBuilder.standard()).thenReturn(mockBuilder);

        // Mock the chaining methods to return the mocked builder and client
        Mockito.when(mockBuilder.withRegion(ArgumentMatchers.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.withCredentials(ArgumentMatchers.any(AWSStaticCredentialsProvider.class))).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(mockS3Client);

        //Calling actual executeSizeLimitedUpdate for bulk unload
        snowflakeBulkUnloadOperation.executeSizeLimitedUpdate(request, response);

        //verifying connection is being called with catalog and schema in get connection
        Mockito.verify(connectionMock).setCatalog(dbDynamicOperationProperty.toUpperCase());
        Mockito.verify(connectionMock).setSchema(schemaDynamicOperationProperty.toUpperCase());
    }
}