//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.cosmosdb.util.CosmosDbConstants;
import com.boomi.connector.util.BaseConnector;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBConnector extends BaseConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new CosmosDBBrowser(createConnection(context));
    }    

    @Override
    protected Operation createUpdateOperation(OperationContext context) {
        return new CosmosDBUpdateOperation(createConnection(context));
    }
    
    @Override
    protected Operation createCreateOperation(OperationContext context) {
        return new CosmosDBCreateOperation(createConnection(context));
    }
    
    @Override
    protected Operation createQueryOperation(OperationContext context) {
    	return new CosmosDBQueryOperation(createConnection(context));
    }   
    
    @Override
    protected Operation createUpsertOperation(OperationContext context) {
        return new CosmosDBUpsertOperation(createConnection(context));
    }

    @Override
    protected Operation createExecuteOperation(OperationContext context) {
    	String operationType = context.getCustomOperationType();
    	switch (operationType) {
		case CosmosDbConstants.HTTP_GET:
			return new CosmosDBExecuteOperation(createConnection(context));
		case CosmosDbConstants.HTTP_DELETE:
			return new CosmosDBDeleteExecuteOperation(createConnection(context));
		default:
			return null;
		}
        
    }

    private CosmosDBConnection createConnection(BrowseContext context) {
        return new CosmosDBConnection(context);
    }
}