package com.boomi.connector.databaseconnector.cache;

import com.boomi.connector.testutil.MutablePropertyMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link TransactionCacheKey}
 */
public class TransactionCacheKeyTest {

    private TransactionCacheKey _transactionCacheKey1;
    private TransactionCacheKey _transactionCacheKey2;

    @Before
    public void setUp() {
        MutablePropertyMap propertyMap1 = new MutablePropertyMap();
        MutablePropertyMap propertyMapNested1 = new MutablePropertyMap();
        propertyMapNested1.put("nestedKey", "nestedValue");
        propertyMap1.put("key1", "value1");
        propertyMap1.put("key2", propertyMapNested1);
        _transactionCacheKey1 = new TransactionCacheKey("topLevelExecutionId", propertyMap1);

        MutablePropertyMap propertyMap2 = new MutablePropertyMap();
        MutablePropertyMap propertyMapNested2 = new MutablePropertyMap();
        propertyMapNested2.put("nestedKey", "nestedValue");
        propertyMap2.put("key1", "value1");
        propertyMap2.put("key2", propertyMapNested2);

        _transactionCacheKey2 = new TransactionCacheKey("topLevelExecutionId", propertyMap2);
    }

    /**
     *Test {@link TransactionCacheKey#equals(Object)} method.
     */
    @Test
    public void testEquals() {
        Assert.assertTrue(_transactionCacheKey2.equals(_transactionCacheKey1));
    }

    /**
     *Test {@link TransactionCacheKey#equals(Object)} method.
     */
    @Test
    public void testEqualsNot() {
        MutablePropertyMap propertyMap1 = new MutablePropertyMap();
        MutablePropertyMap propertyMapNested1 = new MutablePropertyMap();
        propertyMapNested1.put("nestedKey", "nestedValue");
        propertyMap1.put("key1", "differentValue");
        propertyMap1.put("key2", propertyMapNested1);
        _transactionCacheKey1 = new TransactionCacheKey("topLevelExecutionId", propertyMap1);

        Assert.assertFalse(_transactionCacheKey2.equals(_transactionCacheKey1));
    }

    /**
     *Test {@link TransactionCacheKey#equals(Object)} method.
     */
    @Test
    public void testEqualsSameObj() {
        Assert.assertTrue(_transactionCacheKey1.equals(_transactionCacheKey1));
    }

    /**
     *Test {@link TransactionCacheKey#equals(Object)} method.
     */
    @Test
    public void testEqualsNull() {
        Assert.assertFalse(_transactionCacheKey2.equals(null));
    }

    /**
     *Test {@link TransactionCacheKey#equals(Object)} method.
     */
    @Test
    public void testHashCode() {
        Assert.assertTrue(_transactionCacheKey2.equals(_transactionCacheKey1));
        Assert.assertTrue(_transactionCacheKey2.hashCode() == _transactionCacheKey1.hashCode());
    }

    /**
     *Test {@link TransactionCacheKey#equals(Object)} method.
     */
    @Test
    public void testHashCodeNotEqual() {
        MutablePropertyMap propertyMap1 = new MutablePropertyMap();
        MutablePropertyMap propertyMapNested1 = new MutablePropertyMap();
        propertyMapNested1.put("nestedKey", "nestedValue");
        propertyMap1.put("key1", "differentValue");
        propertyMap1.put("key2", propertyMapNested1);
        _transactionCacheKey1 = new TransactionCacheKey("topLevelExecutionId", propertyMap1);

        Assert.assertFalse(_transactionCacheKey2.equals(_transactionCacheKey1));
        Assert.assertFalse(_transactionCacheKey2.hashCode() == _transactionCacheKey1.hashCode());
    }
}
