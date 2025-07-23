//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import com.boomi.connector.api.Connector;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnector;
import com.boomi.connector.testutil.ConnectorTestContext;
/**
 * @author aditi ardhapure
 *
 *		{tags} 
 */
public class LiveOpticsTestContext extends ConnectorTestContext {
 
	public LiveOpticsTestContext() {
		addConnectionProperty("loginSecret", LiveOpticsTestConstants.LOGIN_SECRET);
		addConnectionProperty("sharedSecret", LiveOpticsTestConstants.SHARED_SECRET);
		addConnectionProperty("includeEntities", LiveOpticsTestConstants.INCLUDE_ENTITIES);
		addConnectionProperty("url", LiveOpticsTestConstants.DEFAULT_URL);
		addConnectionProperty("loginID", LiveOpticsTestConstants.LOGIN_ID_1);
	}

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return LiveOpticsConnectorConnector.class;
	}

}