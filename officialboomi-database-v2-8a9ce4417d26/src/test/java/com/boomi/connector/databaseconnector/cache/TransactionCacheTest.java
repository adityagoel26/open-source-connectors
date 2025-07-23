package com.boomi.connector.databaseconnector.cache;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.ConnectorCacheFactory;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class to test {@link TransactionCache}
 */
public class TransactionCacheTest {

    public static final String EXECUTION_ID = "executionId";
    private final OperationContext _operationContext = Mockito.mock(OperationContext.class);

    /**
     * Test {@link TransactionCache#isValid()}
     */
    @Test
    public void testIsValid() {
        TransactionCacheKey transactionCacheKey = new TransactionCacheKey(EXECUTION_ID, null);
        TransactionCache transactionCache = new TransactionCache(transactionCacheKey, null);
        Assert.assertTrue(transactionCache.isValid());
    }

    /**
     * Test {@link TransactionCache#getConnectionFactory(Connection)}
     */
    @Test
    public void testGetConnectionFactory() {
        ConnectorCacheFactory<TransactionCacheKey, TransactionCache, ConnectorContext> factory =
                TransactionCache.getConnectionFactory(null);
        Assert.assertNotNull("Factory is null!", factory);

        Mockito.when(_operationContext.getConnectionProperties()).thenReturn(null);
        TransactionCache cache = factory.createCache(new TransactionCacheKey(EXECUTION_ID, null), _operationContext);
        Assert.assertNotNull("Cache is null!", cache);
    }

    /**
     * Test {@link TransactionCache#getConnection()} and {@link TransactionCache#setConnection(Connection)}
     */
    @Test
    public void testGetSetConnection() throws SQLException {
        TransactionCacheKey transactionCacheKey = new TransactionCacheKey(EXECUTION_ID, null);
        TransactionCache transactionCache = new TransactionCache(transactionCacheKey, null);
        transactionCache.setConnection(null);

        Assert.assertEquals(null, transactionCache.getConnection());
    }
}
