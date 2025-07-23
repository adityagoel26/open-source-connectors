// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorBrowserTest {
	SAPConnectorBrowserTestContext context = new SAPConnectorBrowserTestContext(); 
	SAPConnectorConnection con = new SAPConnectorConnection(context);
	SAPConnectorBrowser browser = new SAPConnectorBrowser(con);

	@Test
	public void testGetObjectDefinitions() {
		String objectTypeId = "/A_AddressEmailAddress";
		List<ObjectDefinitionRole> roles = Arrays.asList(ObjectDefinitionRole.INPUT,ObjectDefinitionRole.OUTPUT);
		ObjectDefinitions objDefinitions = browser.getObjectDefinitions(objectTypeId,roles);
		assertNotNull(objDefinitions);
	}

	@Test
	public void testGetObjectTypes() {
		ObjectTypes objType=browser.getObjectTypes();
		assertNotNull(objType);
	}

}
