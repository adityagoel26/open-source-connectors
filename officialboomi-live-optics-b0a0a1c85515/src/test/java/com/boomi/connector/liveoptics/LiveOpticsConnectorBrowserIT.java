//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;

import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.liveoptics.LiveOpticsConnectorBrowser;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnection;
import com.boomi.connector.testutil.SimpleBrowseContext;

public class LiveOpticsConnectorBrowserIT {

	@Test
	public void testGetConnection() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		LiveOpticsConnectorConnection conn = new LiveOpticsConnectorConnection(context);
		LiveOpticsConnectorBrowser browser = new LiveOpticsConnectorBrowser(conn);
		assertEquals(conn, browser.getConnection()); 
	}
	@Test
	public void testgetObjectDefinitions_GET() { 
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		LiveOpticsConnectorConnection conn = new LiveOpticsConnectorConnection(context);
		LiveOpticsConnectorBrowser browser = new LiveOpticsConnectorBrowser(conn);
		ObjectDefinitions objectDefinitions = browser.getObjectDefinitions("Project", Arrays.asList(INPUT, OUTPUT));
		assertEquals("JSON", objectDefinitions.getDefinitions().get(0).getInputType().toString());
		assertEquals("JSON", objectDefinitions.getDefinitions().get(0).getOutputType().toString());
	}
	@Test
	public void testgetObjectTypes_GET() {
		SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.GET, null, null);
		LiveOpticsConnectorConnection conn = new LiveOpticsConnectorConnection(context);
		LiveOpticsConnectorBrowser browser = new LiveOpticsConnectorBrowser(conn);
		ObjectTypes objectTypes = browser.getObjectTypes();
		assertEquals("Project", objectTypes.getTypes().get(0).getId().toString());
	}

}
