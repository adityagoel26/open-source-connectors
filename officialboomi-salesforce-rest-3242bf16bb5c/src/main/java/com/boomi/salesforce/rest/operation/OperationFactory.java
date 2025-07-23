// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.constant.OperationAPI;
import com.boomi.salesforce.rest.operation.create.CreateOperationFactory;
import com.boomi.salesforce.rest.operation.delete.DeleteOperationFactory;
import com.boomi.salesforce.rest.operation.update.UpdateOperationFactory;
import com.boomi.salesforce.rest.operation.upsert.UpsertOperationFactory;

/**
 * Base class for a family of factories that can be used to construct instances of {@link Operation}
 */
public abstract class OperationFactory {

    /**
     * Construct an instance of {@link Operation}
     *
     * @param context the operation context
     * @return the operation
     */
    public static Operation createOperation(OperationContext context) {

        OperationFactory factory;
        switch (context.getOperationType()) {
            case CREATE:
                factory = new CreateOperationFactory();
                break;
            case UPDATE:
                factory = new UpdateOperationFactory();
                break;
            case UPSERT:
                factory = new UpsertOperationFactory();
                break;
            case DELETE:
                factory = new DeleteOperationFactory();
                break;
            default:
                throw new UnsupportedOperationException("unsupported operation " + context.getOperationType());
        }
        return factory.getOperation(context);
    }

    /**
     * Construct an instance of {@link Operation}
     *
     * @param context the operation context
     * @return the operation
     */
    protected abstract Operation getOperation(OperationContext context);

    /**
     * Check if the operation is configured to use Salesforce BULK API
     *
     * @param context the operation context
     * @return {@code true} if the operation is configured to use BULK API, {@code false} otherwise
     */
    protected static boolean isBulkOperation(OperationContext context) {
        PropertyMap properties = context.getOperationProperties();
        return OperationAPI.from(properties.getProperty(OperationAPI.FIELD_ID)) == OperationAPI.BULK_V2;
    }

    /**
     * Check if the operation is configured to use Salesforce Composite API
     *
     * @param context the operation context
     * @return {@code true} if the operation is configured to use Composite API, {@code false} otherwise
     */
    protected static boolean isCompositeOperation(OperationContext context) {
        PropertyMap properties = context.getOperationProperties();

        OperationAPI operationAPI = OperationAPI.from(properties.getProperty(OperationAPI.FIELD_ID));
        if (operationAPI == OperationAPI.COMPOSITE) {
            return true;
        }

        if (operationAPI == OperationAPI.REST) {
            // for backwards compatibility, REST Operations with batchCount > 1L are considered Composite Operations
            Long batchCount = properties.getLongProperty(Constants.COMPOSITE_BATCH_COUNT_DESCRIPTOR, 1L);
            return batchCount > 1L;
        }

        return false;

    }
}
