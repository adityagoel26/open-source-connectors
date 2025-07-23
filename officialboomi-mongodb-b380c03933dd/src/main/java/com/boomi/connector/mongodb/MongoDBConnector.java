// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;

/**
 * The Class MongoDBConnector to instantiate the Connector components
 *
 */
public class MongoDBConnector extends BaseConnector {

    /* (non-Javadoc)
     * @see com.boomi.connector.api.Connector#createBrowser(com.boomi.connector.api.BrowseContext)
     */
    @Override
    public Browser createBrowser(BrowseContext context) {
        return new MongoDBConnectorBrowser(createConnection(context));
    }    

    /* (non-Javadoc)
     * @see com.boomi.connector.util.BaseConnector#createGetOperation(com.boomi.connector.api.OperationContext)
     */
    @Override
    protected Operation createGetOperation(OperationContext context) {
        return new MongoDBConnectorGetOperation(createConnection(context));
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.util.BaseConnector#createQueryOperation(com.boomi.connector.api.OperationContext)
     */
    @Override
    protected Operation createQueryOperation(OperationContext context) {
        return new MongoDBConnectorQueryOperation(createConnection(context));
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.util.BaseConnector#createCreateOperation(com.boomi.connector.api.OperationContext)
     */
    @Override
    protected Operation createCreateOperation(OperationContext context) {
        return new MongoDBConnectorCreateOperation(createConnection(context));
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.util.BaseConnector#createUpdateOperation(com.boomi.connector.api.OperationContext)
     */
    @Override
    protected Operation createUpdateOperation(OperationContext context) {
        return new MongoDBConnectorUpdateOperation(createConnection(context));
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.util.BaseConnector#createUpsertOperation(com.boomi.connector.api.OperationContext)
     */
    @Override
    protected Operation createUpsertOperation(OperationContext context) {
        return new MongoDBConnectorUpsertOperation(createConnection(context));
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.util.BaseConnector#createDeleteOperation(com.boomi.connector.api.OperationContext)
     */
    @Override
    protected Operation createDeleteOperation(OperationContext context) {
        return new MongoDBConnectorDeleteOperation(createConnection(context));
    }

    /**
     * Creates the connection.
     *
     * @param context the context
     * @return the mongo DB connector connection
     */
    private MongoDBConnectorConnection createConnection(BrowseContext context) {
        return new MongoDBConnectorConnection(context);
    }
}