//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static org.junit.Assert.*;

import org.junit.Test;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.liveoptics.LiveOpticsConnectorBrowser;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnector;
import com.boomi.connector.liveoptics.LiveOpticsConnectorGetOperation;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutil.SimpleOperationContext;

public class LiveOpticsConnectorConnectorIT {

	@Test
	public void testCreateBrowser() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		LiveOpticsConnectorConnector connector= new LiveOpticsConnectorConnector();
		assertTrue(connector.createBrowser(context) instanceof LiveOpticsConnectorBrowser);
	}
	@Test
	public void testGetOperationOperationContext() {
		SimpleOperationContext opContext= new SimpleOperationContext(null, null, OperationType.GET, null, null, null, null);
		LiveOpticsConnectorConnector connector= new LiveOpticsConnectorConnector();
		assertTrue(connector.createGetOperation(opContext) instanceof LiveOpticsConnectorGetOperation);
	}

}
