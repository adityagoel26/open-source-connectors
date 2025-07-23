package com.boomi.snowflake;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.snowflake.operations.SnowflakeBulkLoadOperation;
import com.boomi.snowflake.operations.SnowflakeCommandOperation;
import com.boomi.snowflake.operations.SnowflakeCreateOperation;
import com.boomi.snowflake.operations.SnowflakeDeleteOperation;
import com.boomi.snowflake.operations.SnowflakeExecuteOperation;
import com.boomi.snowflake.operations.SnowflakeGetOperation;
import com.boomi.snowflake.operations.SnowflakeQueryOperation;
import com.boomi.snowflake.operations.SnowflakeSnowSQLOperation;
import com.boomi.snowflake.operations.SnowflakeUpdateOperation;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link SnowflakeConnector} class.
 * <p>
 * This test class covers the creation of different operations and browsers by the
 * {@link SnowflakeConnector} class, including the standard operations (Get, Create, Update, Delete, Query)
 * as well as specialized operations like Execute for different custom operation types (snowSQL, bulkLoad, EXECUTE).
 * </p>
 * The tests validate that the correct operation instances are created based on the operation type
 * and that the {@link SnowflakeConnector} behaves as expected under various conditions.
 */
public class SnowflakeConnectorTest {

    private SnowflakeConnector snowflakeConnector;
    private BrowseContext mockBrowseContext;
    private OperationContext mockOperationContext;

    /**
     * Set up the test environment by initializing the {@link SnowflakeConnector} and mock dependencies.
     */
    @Before
    public void setUp() {
        snowflakeConnector = new SnowflakeConnector();
        mockBrowseContext = mock(BrowseContext.class);
        mockOperationContext = mock(OperationContext.class);
    }

    /**
     * Tests the creation of a {@link SnowflakeBrowser} instance using the {@link SnowflakeConnector}.
     */
    @Test
    public void testCreateBrowser() {
        Browser browser = snowflakeConnector.createBrowser(mockBrowseContext);
        assertTrue("Expected instance of SnowflakeBrowser", browser instanceof SnowflakeBrowser);
    }

    /**
     * Tests the creation of a {@link SnowflakeGetOperation} instance using the {@link SnowflakeConnector}.
     */
    @Test
    public void testCreateGetOperation() {
        Operation operation = snowflakeConnector.createGetOperation(mockOperationContext);
        assertTrue("Expected SnowflakeGetOperation instance", operation instanceof SnowflakeGetOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeCreateOperation} instance using the {@link SnowflakeConnector}.
     */
    @Test
    public void testCreateCreateOperation() {
        Operation operation = snowflakeConnector.createCreateOperation(mockOperationContext);
        assertTrue("Expected SnowflakeCreateOperation instance", operation instanceof SnowflakeCreateOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeUpdateOperation} instance using the {@link SnowflakeConnector}.
     */
    @Test
    public void testCreateUpdateOperation() {
        Operation operation = snowflakeConnector.createUpdateOperation(mockOperationContext);
        assertTrue("Expected SnowflakeUpdateOperation instance", operation instanceof SnowflakeUpdateOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeDeleteOperation} instance using the {@link SnowflakeConnector}.
     */
    @Test
    public void testCreateDeleteOperation() {
        Operation operation = snowflakeConnector.createDeleteOperation(mockOperationContext);
        assertTrue("Expected SnowflakeDeleteOperation instance", operation instanceof SnowflakeDeleteOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeQueryOperation} instance using the {@link SnowflakeConnector}.
     */
    @Test
    public void testCreateQueryOperation() {
        Operation operation = snowflakeConnector.createQueryOperation(mockOperationContext);
        assertTrue("Expected SnowflakeQueryOperation instance", operation instanceof SnowflakeQueryOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeSnowSQLOperation} instance when the operation type is "snowSQL".
     */
    @Test
    public void testCreateExecuteOperationSnowSQL() {
        when(mockOperationContext.getCustomOperationType()).thenReturn("snowSQL");
        Operation operation = snowflakeConnector.createExecuteOperation(mockOperationContext);
        assertTrue("Expected SnowflakeSnowSQLOperation instance", operation instanceof SnowflakeSnowSQLOperation);
    }


    /**
     * Tests the creation of a {@link SnowflakeBulkLoadOperation} instance when the operation type is "bulkLoad".
     */
    @Test
    public void testCreateExecuteOperationBulkLoad() {
        when(mockOperationContext.getCustomOperationType()).thenReturn("bulkLoad");
        Operation operation = snowflakeConnector.createExecuteOperation(mockOperationContext);
        assertTrue("Expected SnowflakeBulkLoadOperation instance", operation instanceof SnowflakeBulkLoadOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeExecuteOperation} instance when the operation type is "EXECUTE".
     */
    @Test
    public void testCreateExecuteOperationExecute() {
        when(mockOperationContext.getCustomOperationType()).thenReturn("EXECUTE");
        Operation operation = snowflakeConnector.createExecuteOperation(mockOperationContext);
        assertTrue("Expected SnowflakeExecuteOperation instance", operation instanceof SnowflakeExecuteOperation);
    }

    /**
     * Tests the creation of a {@link SnowflakeCommandOperation} instance when the operation type is unknown.
     */
    @Test
    public void testCreateExecuteOperationDefault() {
        when(mockOperationContext.getCustomOperationType()).thenReturn("unknown");
        Operation operation = snowflakeConnector.createExecuteOperation(mockOperationContext);
        assertTrue("Expected SnowflakeCommandOperation instance", operation instanceof SnowflakeCommandOperation);
    }
}
