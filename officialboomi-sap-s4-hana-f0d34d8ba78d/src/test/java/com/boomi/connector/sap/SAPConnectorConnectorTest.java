// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutil.SimpleOperationContext;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorConnectorTest {
	
	@Test
	public void testCreateBrowser() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.CREATE, null, null);
		SAPConnectorConnector sapConnector = new SAPConnectorConnector();
		assertTrue(sapConnector.createBrowser(context) instanceof SAPConnectorBrowser);
	}

	@Test
	public void testCreateQueryOperationOperationContext() {
		SimpleOperationContext context = new SimpleOperationContext(null, null, OperationType.QUERY, null, null, null, null);
		SAPConnectorConnector sapConnector = new SAPConnectorConnector();
		assertTrue(sapConnector.createQueryOperation(context)instanceof SAPConnectorQueryOperation);
	}

	@Test
	public void testCreateExecuteOperationOperationContext() {
		SimpleOperationContext context = new SimpleOperationContext(null, null, OperationType.EXECUTE, null, null, null, null);
		SAPConnectorConnector sapConnector = new SAPConnectorConnector();
		assertTrue(sapConnector.createExecuteOperation(context)instanceof SAPConnectorExecuteOperation);
	}

}
