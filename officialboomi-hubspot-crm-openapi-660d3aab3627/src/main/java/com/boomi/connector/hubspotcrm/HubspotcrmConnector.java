// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmBrowser;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmCreateOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmDeleteOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmRetrieveOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmSearchOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmUpdateOperation;
import com.boomi.connector.openapi.OpenAPIConnector;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.Operation;

public class HubspotcrmConnector extends OpenAPIConnector {

    /**
     * Creates a new instance of the HubspotcrmBrowser class.
     *
     * @param context The BrowseContext object containing the necessary context information.
     * @return A new instance of the HubspotcrmBrowser class.
     */
    @Override
    public Browser createBrowser(BrowseContext context) {
        return new HubspotcrmBrowser(new HubspotcrmConnection<>(context));
    }

    /**
     * Creates and returns an instance of HubspotcrmCreateOperation with a HubspotOperationConnection.
     *
     * @param operationContext The OperationContext for the operation.
     * @return An instance of HubspotcrmCreateOperation.
     */
    @Override
    public Operation createExecuteOperation(final OperationContext operationContext) {
        return new HubspotcrmCreateOperation(new HubspotcrmOperationConnection(operationContext));
    }

    /**
     * Creates and returns an instance of HubspotcrmDeleteOperation with a HubspotOperationConnection.
     *
     * @param operationContext The OperationContext for the operation.
     * @return An instance of HubspotcrmDeleteOperation.
     */
    @Override
    public Operation createDeleteOperation(final OperationContext operationContext) {
        return new HubspotcrmDeleteOperation(new HubspotcrmOperationConnection(operationContext));
    }

    /**
     * Creates and returns an instance of HubspotcrmUpdateOperation with a HubspotOperationConnection.
     *
     * @param operationContext The OperationContext for the operation.
     * @return An instance of HubspotcrmUpdateOperation.
     */
    @Override
    public Operation createUpdateOperation(final OperationContext operationContext) {
        return new HubspotcrmUpdateOperation(new HubspotcrmOperationConnection(operationContext));
    }

    /**
     * Creates and returns an instance of HubspotcrmRetrieveOperation with a HubspotOperationConnection.
     *
     * @param operationContext The OperationContext for the operation.
     * @return An instance of HubspotcrmRetrieveOperation.
     */
    @Override
    public Operation createGetOperation(final OperationContext operationContext) {
        return new HubspotcrmRetrieveOperation(new HubspotcrmOperationConnection(operationContext));
    }

    /**
     * Creates and returns an instance of HubspotcrmSearchOperation with a HubspotOperationConnection.
     *
     * @param operationContext The OperationContext for the operation.
     * @return An instance of HubspotcrmSearchOperation.
     */
    @Override
    public Operation createQueryOperation(final OperationContext operationContext) {
        return new HubspotcrmSearchOperation(new HubspotcrmOperationConnection(operationContext));
    }
}
