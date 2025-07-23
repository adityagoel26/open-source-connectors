//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.Connector;
import com.boomi.connector.testutil.ConnectorTestContext;
import com.boomi.connector.workdayprism.PrismConnector;

/**
 * @author saurav.b.sengupta
 *
 */
public class PrismITContext extends ConnectorTestContext {

	public PrismITContext() {
		
		 addConnectionProperty(TestConstants.PROP_API_ENDPOINT, TestConstants.API_ENDPOINT);
	      addConnectionProperty(TestConstants.PROP_CLIENT_ID, TestConstants.CLIENT_ID);
	        addConnectionProperty(TestConstants.PROP_CLIENT_SECRET, TestConstants.CLIENT_SECRET);
	        addConnectionProperty(TestConstants.PROP_REFRESH_TOKEN, TestConstants.REFRESH_TOKEN);
	      //Requester.setSSlContextForTest();	
	}
	
		
	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return PrismConnector.class;
	}

}
