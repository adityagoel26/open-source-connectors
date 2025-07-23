// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.testutil;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.databaseconnector.DatabaseConnectorConnector;
import com.boomi.connector.databaseconnector.cache.TransactionCache;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;

/**
 * Test context for DBv2 connector
 */
public class DatabaseConnectorTestContext extends ConnectorTestContext {

    public static final String OBJECT_TYPE_ID = "EVENT";
    public static final String SCHEMA_NAME_REF = "Schema Name";
    public static final long BATCH_COUNT_LONG = 10L;
    public static final String SCHEMA_REF = "Schema";
    public static final String TYPE_NAME_REF = "Type Name";
    public static final String DATA_TYPE_REF = "datatype";
    public static final String TEST_QUERY = "SELECT * FROM Test.EVENT WHERE USER_ID IN ($USER_ID)";

    public static DatabaseConnectorTestContext getDatabaseContext(){
        DatabaseConnectorTestContext context = new DatabaseConnectorTestContext();
        context.setOperationType(OperationType.EXECUTE);

        context.addOperationProperty(DatabaseConnectorConstants.COMMIT_OPTION, DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        context.addOperationProperty(DatabaseConnectorConstants.TYPE_NAME, TYPE_NAME_REF);
        context.addOperationProperty(DatabaseConnectorConstants.DATA_TYPE, DATA_TYPE_REF);
        context.setObjectTypeId(OBJECT_TYPE_ID);
        context.addOperationProperty(DatabaseConnectorConstants.QUERY, TEST_QUERY );
        context.addOperationProperty(DatabaseConnectorConstants.SCHEMA_NAME, SCHEMA_NAME_REF);
        context.addOperationProperty(DatabaseConnectorConstants.BATCH_COUNT, BATCH_COUNT_LONG);
        context.addOperationProperty(DatabaseConnectorConstants.DELETE_TYPE, OperationTypeConstants.DYNAMIC_DELETE);
        context.addOperationProperty("schema", SCHEMA_REF);
        context.addConnectionProperty(DatabaseConnectorConstants.QUERY, TEST_QUERY );
        context.addConnectionProperty(DatabaseConnectorConstants.COMMIT_OPTION, "");
        context.addConnectionProperty(DatabaseConnectorConstants.GET_TYPE, "get");
        context.addConnectionProperty(DatabaseConnectorConstants.TYPE, "type");
        context.setObjectTypeId("TEST");
        return context;
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return DatabaseConnectorConnector.class;
    }

    /**
     * This method will return the DBV2TestContext object using passed argument
     *
     * @param transactionCache transaction cache
     * @return DBV2TestContext object
     */
    public static DatabaseConnectorTestContext getDatabaseContext(TransactionCache transactionCache) {
        DatabaseConnectorTestContext context = getDatabaseContext();
        context.addToCache(transactionCache.getKey(), transactionCache);
        return context;
    }

    /**
     * This method will return the DBV2TestContext object using passed argument
     *
     * @param transactionCache transaction cache
     * @param operationType operation Type
     * @return DBV2TestContext object
     */
    public static DatabaseConnectorTestContext getDatabaseContext(TransactionCache transactionCache, String operationType) {
        DatabaseConnectorTestContext context = getDatabaseContext();
        context.setOperationCustomType(operationType);
        context.addToCache(transactionCache.getKey(), transactionCache);
        return context;
    }
}
