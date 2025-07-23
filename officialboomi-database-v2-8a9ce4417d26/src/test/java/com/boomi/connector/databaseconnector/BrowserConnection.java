// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector;

import com.boomi.connector.api.Connector;
import com.boomi.connector.testutil.ConnectorTestContext;

public class BrowserConnection extends ConnectorTestContext {

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return DatabaseConnectorConnector.class;
	}
}