// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoConnectionTest {

	SAPJcoConnectorBrowserTestContext context = new SAPJcoConnectorBrowserTestContext();
	SAPJcoConnection connection = new SAPJcoConnection(context);
	SAPJcoBrowser browser = new SAPJcoBrowser(connection);

	@Test
	public void test() {
		assertTrue(connection instanceof SAPJcoConnection);
		browser.testConnection();
	}

}
