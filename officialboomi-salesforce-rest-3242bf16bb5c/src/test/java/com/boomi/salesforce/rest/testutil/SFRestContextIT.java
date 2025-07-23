// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.testutil;

import com.boomi.connector.api.Connector;
import com.boomi.connector.testutil.ConnectorTestContext;
import com.boomi.salesforce.rest.SFRestConnector;

public class SFRestContextIT extends ConnectorTestContext {

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return SFRestConnector.class;
    }
}
