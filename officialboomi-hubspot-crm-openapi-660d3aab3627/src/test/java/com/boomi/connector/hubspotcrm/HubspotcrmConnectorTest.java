// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmBrowser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

import com.boomi.connector.api.Operation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmCreateOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmDeleteOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmRetrieveOperation;
import com.boomi.connector.hubspotcrm.operation.HubspotcrmSearchOperation;

/**
 * This class is used for testing the HubspotcrmConnector.
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({
        HubspotcrmConnectorTest.class })
class HubspotcrmConnectorTest {

    private HubspotcrmConnector hubspotcrmConnector;
    private BrowseContext mockBrowseContext;
    private OperationContext mockOperationContext;

    /**
     * Sets up the necessary objects for testing.
     * Initializes the HubspotcrmConnector instance and mocks the BrowseContext and OperationContext objects.
     * Sets the OperationType for the mockBrowseContext to GET and the CustomOperationType to an empty string.
     */

    @BeforeEach
    public void setUp() {
        hubspotcrmConnector = new HubspotcrmConnector();
        mockBrowseContext = Mockito.mock(BrowseContext.class);
        mockOperationContext = Mockito.mock(OperationContext.class);

        when(mockBrowseContext.getOperationType()).thenReturn(OperationType.GET);
        when(mockBrowseContext.getCustomOperationType()).thenReturn("");
    }

    /**
     * Tests the createBrowser method of the HubspotcrmConnector class.
     * Verifies that the returned Browser instance is of type HubspotcrmBrowser.
     */

    @Test
    void testCreateBrowser() {
        Browser browser = hubspotcrmConnector.createBrowser(mockBrowseContext);
        Assertions.assertInstanceOf(HubspotcrmBrowser.class, browser);
    }

    /**
     * Tests the createExecuteOperation method of the HubspotcrmConnector class.
     * Verifies that the returned Operation instance is of type HubspotCreateOperation.
     */
    @Test
    void testCreateExecuteOperation() {
        Operation operation = hubspotcrmConnector.createExecuteOperation(mockOperationContext);
        Assertions.assertInstanceOf(HubspotcrmCreateOperation.class, operation);
    }

    /**
     * Tests the createDeleteOperation method of the HubspotcrmConnector class.
     * Verifies that the returned Operation instance is of type HubspotcrmDeleteOperation.
     */
    @Test
    void testCreateDeleteOperation() {
        Operation operation = hubspotcrmConnector.createDeleteOperation(mockOperationContext);
        Assertions.assertInstanceOf(HubspotcrmDeleteOperation.class, operation);
    }

    /**
     * Tests the createGetOperation method of the HubspotcrmConnector class.
     * Verifies that the returned Operation instance is of type HubspotcrmRetrieveOperation.
     */
    @Test
    void testCreateGetOperation() {
        Operation operation = hubspotcrmConnector.createGetOperation(mockOperationContext);
        Assertions.assertInstanceOf(HubspotcrmRetrieveOperation.class, operation);
    }

    /**
     * Tests the createGetOperation method of the HubspotcrmConnector class.
     * Verifies that the returned Operation instance is of type HubspotcrmSearchOperation.
     */
    @Test
    void testCreateQueryOperation() {
        Operation operation = hubspotcrmConnector.createQueryOperation(mockOperationContext);
        Assertions.assertInstanceOf(HubspotcrmSearchOperation.class, operation);
    }

}

