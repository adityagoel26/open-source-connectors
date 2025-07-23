// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome.test;

import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.ENABLESSL;
import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.IPADDRESS;
import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.PASSWORD;
import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.USERNAME;

import java.util.HashMap;
import java.util.Map;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.dellome.DellOMEBrowser;
import com.boomi.connector.dellome.DellOMEConnector;
import com.boomi.connector.testutil.ConnectorTester;

public class DellOMEConnectorTest {

	public static void main(String args[]) throws Exception {

		Map<String, Object> connectionProps = new HashMap<>();

		connectionProps.put(USERNAME, "admin");
		connectionProps.put(PASSWORD, "password");
		connectionProps.put(IPADDRESS, "192.168.43.3");
		connectionProps.put(ENABLESSL, false);

		testConnection(connectionProps);
		testQueryOperation(connectionProps);
	}

	public static void testConnection(Map<String, Object> connectionProps) throws Exception {
		DellOMEConnector connector = new DellOMEConnector();
		ConnectorTester tester = new ConnectorTester(connector);
		tester.setOperationContext(OperationType.QUERY, null, null, "SomeType", null);
		tester.setBrowseContext(OperationType.QUERY, connectionProps, null);
		BrowseContext bc = tester.getBrowseContext();
		DellOMEBrowser ome = (DellOMEBrowser) connector.createBrowser(bc);
		ome.testConnection();

	}

	public static void testQueryOperation(Map<String, Object> connectionProps) throws Exception {
		DellOMEConnector connector = new DellOMEConnector();
		ConnectorTester tester = new ConnectorTester(connector);
		tester.setOperationContext(OperationType.QUERY, connectionProps, null, "SomeType", null);
		QueryFilter filter = null;
		tester.executeQueryOperation(filter);

	}
}
