/*
*  Copyright 2019 Accenture. All Rights Reserved.
*  The trademarks used in these materials are the properties of their respective owners.
*  This work is protected by copyright law and contains valuable trade secrets and
*  confidential information.
*/
package com.boomi.connector.eventbridge;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * It overrides the methods from BaseConnector.
 * 
 * @author swastik.vn
 *
 */
public class AWSEventBridgeConnectorTest {
	
	
	 AWSEventBridgeConnector connector=new AWSEventBridgeConnector(); 
	 AWSEventBridgeTestContext context=new AWSEventBridgeTestContext();
	
	@Test
	public void testCreateBrowser() {
		assertTrue(connector.createBrowser(context) instanceof AWSEventBridgeBrowser);
		
	}
	
	@Test
	public void testCreateCreateOperation() {
		assertTrue(connector.createCreateOperation(context) instanceof AWSEventBridgeCreateOperation);
	}
	
}