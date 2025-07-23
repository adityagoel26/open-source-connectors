// Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism;

import org.junit.Test;
import com.boomi.connector.workdayprism.operations.GetOperation;
import com.boomi.connector.workdayprism.utils.PrismITContext;
import org.junit.Assert;

/**
 * @author saurav.b.sengupta
 *
 */
public class PrismConnectorTestIT {
	
	PrismITContext context=new PrismITContext();
	PrismConnector connector=new PrismConnector();
	
	
	/**
	 * Test method to validate browser instance creation functionality
	 */

	@Test
	public void shouldCreateBrowser() {
		Assert.assertTrue(connector.createBrowser(context) instanceof PrismBrowser);
	}
	
	/**
	 * Test method to validate browser instance creation functionality
	 */
	@Test
	public void shouldCreateGetOperation() {
		Assert.assertTrue(connector.createGetOperation(context) instanceof GetOperation);
	}
	
    /**
     * Test method to validate execute operation and expecting an expection
     */
    @Test (expected = UnsupportedOperationException.class)
    public void shouldThrowExceptionInExecutingOperation() {
    	connector.createExecuteOperation(context);
    }	
	

}
