// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationType;
import com.boomi.salesforce.rest.operation.SFRestCSVBulkV2Operation;
import com.boomi.salesforce.rest.operation.SFRestCustomSOQLOperation;
import com.boomi.salesforce.rest.operation.SFRestQueryOperation;
import com.boomi.salesforce.rest.operation.create.SFBulkCreateOperation;
import com.boomi.salesforce.rest.operation.create.SFRestCompositeCreateOperation;
import com.boomi.salesforce.rest.operation.create.SFRestCreateOperation;
import com.boomi.salesforce.rest.operation.delete.SFBulkDeleteOperation;
import com.boomi.salesforce.rest.operation.delete.SFRestCompositeDeleteOperation;
import com.boomi.salesforce.rest.operation.delete.SFRestDeleteOperation;
import com.boomi.salesforce.rest.operation.update.SFBulkUpdateOperation;
import com.boomi.salesforce.rest.operation.update.SFRestCompositeUpdateOperation;
import com.boomi.salesforce.rest.operation.update.SFRestUpdateOperation;
import com.boomi.salesforce.rest.operation.upsert.SFBulkUpsertOperation;
import com.boomi.salesforce.rest.operation.upsert.SFRestCompositeUpsertOperation;
import com.boomi.salesforce.rest.operation.upsert.SFRestUpsertOperation;
import com.boomi.salesforce.rest.testutil.SFRestContextIT;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SFRestConnectorTest {

    @Test
    void createBrowser() {
        SFRestConnector connector = buildSFRestConnector();
        Browser browser = connector.createBrowser(new SFRestContextIT());

        Assertions.assertNotNull(browser);
        Assertions.assertInstanceOf(SFRestBrowser.class, browser);
    }

    @Test
    void createQueryOperation() {
        OperationContext context = mock(OperationContext.class);
        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createQueryOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestQueryOperation.class, operation);
    }

    @Test
    void createRestCreateOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.CREATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createCreateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCreateOperation.class, operation);
    }

    @Test
    void createCompositeCreateOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.CREATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("COMPOSITEAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createCreateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeCreateOperation.class, operation);
    }

    @Test
    void createCompositeCreateOperationBackwardsCompatible() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.CREATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");
        // for backwards compatibility, if a value > 1 is set for batchCount, the operation is a COMPOSITE despite
        // being configured as REST API
        when(context.getOperationProperties().getLongProperty(eq("batchCount"), anyLong())).thenReturn(10L);

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createCreateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeCreateOperation.class, operation);
    }

    @Test
    void createBulkCreateOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.CREATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("BulkAPI2.0");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createCreateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFBulkCreateOperation.class, operation);
    }

    @Test
    void createCreateTreeOperation() {
        OperationContext context = mock(OperationContext.class);
        when(context.getOperationType()).thenReturn(OperationType.CREATE);
        Mockito.when(context.getCustomOperationType()).thenReturn("CREATE_TREE");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createCreateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCreateOperation.class, operation);
    }

    @Test
    void createRestUpdateOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPDATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpdateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestUpdateOperation.class, operation);
    }

    @Test
    void createBulkUpdateOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPDATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("BulkAPI2.0");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpdateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFBulkUpdateOperation.class, operation);
    }

    @Test
    void createCompositeUpdateOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPDATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("COMPOSITEAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpdateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeUpdateOperation.class, operation);
    }

    @Test
    void createCompositeUpdateOperationBackwardsCompatible() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPDATE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");
        // for backwards compatibility, if a value > 1 is set for batchCount, the operation is a COMPOSITE despite
        // being configured as REST API
        when(context.getOperationProperties().getLongProperty(eq("batchCount"), anyLong())).thenReturn(10L);

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpdateOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeUpdateOperation.class, operation);
    }

    @Test
    void createRestDeleteOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.DELETE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createDeleteOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestDeleteOperation.class, operation);
    }

    @Test
    void createBulkDeleteOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.DELETE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("BulkAPI2.0");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createDeleteOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFBulkDeleteOperation.class, operation);
    }

    @Test
    void createCompositeDeleteOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.DELETE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("COMPOSITEAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createDeleteOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeDeleteOperation.class, operation);
    }

    @Test
    void createCompositeDeleteOperationBackwardsCompatible() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.DELETE);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");
        // for backwards compatibility, if a value > 1 is set for batchCount, the operation is a COMPOSITE despite
        // being configured as REST API
        when(context.getOperationProperties().getLongProperty(eq("batchCount"), anyLong())).thenReturn(10L);

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createDeleteOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeDeleteOperation.class, operation);
    }

    @Test
    void createRestUpsertOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPSERT);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpsertOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestUpsertOperation.class, operation);
    }

    @Test
    void createBulkUpsertOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPSERT);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("BulkAPI2.0");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpsertOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFBulkUpsertOperation.class, operation);
    }

    @Test
    void createCompositeUpsertOperation() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPSERT);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("COMPOSITEAPI");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpsertOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeUpsertOperation.class, operation);
    }

    @Test
    void createCompositeUpsertOperationBackwardsCompatible() {
        OperationContext context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.UPSERT);
        when(context.getOperationProperties().getProperty("OperationAPI")).thenReturn("RESTAPI");
        // for backwards compatibility, if a value > 1 is set for batchCount, the operation is a COMPOSITE despite
        // being configured as REST API
        when(context.getOperationProperties().getLongProperty(eq("batchCount"), anyLong())).thenReturn(10L);

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createUpsertOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCompositeUpsertOperation.class, operation);
    }

    @Test
    void createExecuteOperation() {
        OperationContext context = mock(OperationContext.class);
        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createExecuteOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCustomSOQLOperation.class, operation);
    }

    @Test
    void createBulkExecuteOperation() {
        OperationContext context = mock(OperationContext.class);
        Mockito.when(context.getCustomOperationType()).thenReturn("csvBulkApiV2");

        SFRestConnector connector = buildSFRestConnector();

        Operation operation = connector.createExecuteOperation(context);

        Assertions.assertNotNull(operation);
        Assertions.assertInstanceOf(SFRestCSVBulkV2Operation.class, operation);
    }

    SFRestConnector buildSFRestConnector() {
        return new SFRestConnector() {
            @Override
            SFRestConnection createConnection(BrowseContext context) {
                return new SFRestConnection(context) {
                    @Override
                    void initConnection() {
                        // avoid connecting to actual service
                    }
                };
            }
        };
    }
}
