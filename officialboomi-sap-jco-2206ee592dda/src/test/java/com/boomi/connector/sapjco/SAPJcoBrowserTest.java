// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoBrowserTest {

	SAPJcoConnectorBrowserTestContext context = new SAPJcoConnectorBrowserTestContext();
	SAPJcoConnection connection = new SAPJcoConnection(context);
	SAPJcoBrowser browser = new SAPJcoBrowser(connection);

	@Test
	public void testGetObjectDefinitions() {
		List<ObjectDefinitionRole> roles = Arrays.asList(ObjectDefinitionRole.INPUT, ObjectDefinitionRole.OUTPUT);
		String objectTypeId = "BAPI_EMPLOYEE_GETDATA";
		ObjectDefinitions objectDefinitions = browser.getObjectDefinitions(objectTypeId, roles);
		assertNotNull(objectDefinitions);
		assertTrue(!objectDefinitions.getDefinitions().isEmpty());
	}

	@Test
	public void testGetObjectTypes() {
		assertNotNull(browser.getObjectTypes());
	}

}
