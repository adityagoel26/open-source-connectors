//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import com.boomi.connector.liveoptics.LiveOpticsConnectorBrowser;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnector;
import com.boomi.connector.liveoptics.LiveOpticsConnectorGetOperation;
/**
 * @author aditi ardhapure
 *
 *		{tags} 
 */
public class LiveOpticsConnectorConnectorTest {

	private final LiveOpticsTestContext context = new LiveOpticsTestContext();
	private final LiveOpticsConnectorConnector connector = new LiveOpticsConnectorConnector();

	@Test
	public void testCreateBrowser() {
		assertTrue(connector.createBrowser(context) instanceof LiveOpticsConnectorBrowser);
	}

	@Test
	public void testCreateGetOperation() {
		assertTrue(connector.createGetOperation(context) instanceof LiveOpticsConnectorGetOperation);
	}

}
