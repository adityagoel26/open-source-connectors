// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;
import com.boomi.snowflake.operations.SnowflakeUpdateOperation;
import com.boomi.snowflake.operations.SnowflakeQueryOperation;
import com.boomi.snowflake.operations.SnowflakeSnowSQLOperation;
import com.boomi.snowflake.operations.SnowflakeGetOperation;
import com.boomi.snowflake.operations.SnowflakeBulkLoadOperation;
import com.boomi.snowflake.operations.SnowflakeBulkUnloadOperation;
import com.boomi.snowflake.operations.SnowflakeCommandOperation;
import com.boomi.snowflake.operations.SnowflakeCreateOperation;
import com.boomi.snowflake.operations.SnowflakeDeleteOperation;
import com.boomi.snowflake.operations.SnowflakeExecuteOperation;

public class SnowflakeConnector extends BaseConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new SnowflakeBrowser(createConnection(context));
    }    

    @Override
    protected Operation createGetOperation(OperationContext context) {
        return new SnowflakeGetOperation(createConnection(context));
    }

    @Override
    protected Operation createCreateOperation(OperationContext context) {
        return new SnowflakeCreateOperation(createConnection(context));
    }

    @Override
    protected Operation createUpdateOperation(OperationContext context) {
        return new SnowflakeUpdateOperation(createConnection(context));
    }

    @Override
    protected Operation createDeleteOperation(OperationContext context) {
        return new SnowflakeDeleteOperation(createConnection(context));
    }
    
    @Override
    protected Operation createQueryOperation(OperationContext context) {
        return new SnowflakeQueryOperation(createConnection(context));
    }
    
    @Override
    protected Operation createExecuteOperation(OperationContext context) {
    	SnowflakeConnection connection = createConnection(context);
    	switch(context.getCustomOperationType()) {
    		case "snowSQL":    return new SnowflakeSnowSQLOperation(connection);
    		case "bulkLoad":   return new SnowflakeBulkLoadOperation(connection);
    		case "bulkUnload": return new SnowflakeBulkUnloadOperation(connection);
    		case "EXECUTE":    return new SnowflakeExecuteOperation(connection);
    		default:           return new SnowflakeCommandOperation(connection);
    	}

    }
  
    private SnowflakeConnection createConnection(BrowseContext context) {
        return new SnowflakeConnection(context);
    }
}