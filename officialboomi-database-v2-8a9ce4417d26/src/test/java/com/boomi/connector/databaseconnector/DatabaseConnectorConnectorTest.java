package com.boomi.connector.databaseconnector;

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;
import com.boomi.connector.databaseconnector.operations.transaction.CommitOperation;
import com.boomi.connector.databaseconnector.operations.transaction.RollbackOperation;
import com.boomi.connector.databaseconnector.operations.transaction.StartTransactionOperation;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link DatabaseConnectorConnector}
 */
public class DatabaseConnectorConnectorTest {

    private final  OperationContext _operationContext = Mockito.mock(OperationContext.class);
    private final PropertyMap      _propertyMap      = Mockito.mock(PropertyMap.class);

    /**
     * Test {@link DatabaseConnectorConnector#createExecuteOperation(OperationContext)} start trans.
     */
    @Test
    public void testExecuteStartTrans() {

        DatabaseConnectorConnector databaseConnectorConnector = new DatabaseConnectorConnector();
        Mockito.when(_operationContext.getCustomOperationType()).thenReturn(OperationTypeConstants.START_TRANSACTION);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getConnectionProperties()).thenReturn(_propertyMap);

        Assert.assertTrue(databaseConnectorConnector.createExecuteOperation(_operationContext) instanceof StartTransactionOperation);
    }

    /**
     * Test {@link DatabaseConnectorConnector#createExecuteOperation(OperationContext)} commit.
     */
    @Test
    public void testExecuteCommit() {

        DatabaseConnectorConnector databaseConnectorConnector = new DatabaseConnectorConnector();
        Mockito.when(_operationContext.getCustomOperationType()).thenReturn(OperationTypeConstants.COMMIT_TRANSACTION);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getConnectionProperties()).thenReturn(_propertyMap);

        Assert.assertTrue(databaseConnectorConnector.createExecuteOperation(_operationContext) instanceof CommitOperation);
    }

    /**
     * Test {@link DatabaseConnectorConnector#createExecuteOperation(OperationContext)} rollback.
     */
    @Test
    public void testExecuteRollback() {

        DatabaseConnectorConnector databaseConnectorConnector = new DatabaseConnectorConnector();
        Mockito.when(_operationContext.getCustomOperationType()).thenReturn(OperationTypeConstants.ROLLBACK_TRANSACTION);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getConnectionProperties()).thenReturn(_propertyMap);

        Assert.assertTrue(databaseConnectorConnector.createExecuteOperation(_operationContext) instanceof RollbackOperation);
    }
}
