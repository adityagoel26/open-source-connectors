//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;
import com.boomi.connector.workdayprism.PrismConnector;
import com.boomi.connector.workdayprism.requests.RequestContextHelper;
import com.boomi.connector.workdayprism.utils.TestConstants;

/**
 * @author saurav.b.sengupta
 *
 */
public class PrismITContextForCreateTable extends ConnectorTestContext {

	public PrismITContextForCreateTable() {
		
		 addConnectionProperty(TestConstants.PROP_API_ENDPOINT, TestConstants.API_ENDPOINT);
	      addConnectionProperty(TestConstants.PROP_CLIENT_ID, TestConstants.CLIENT_ID);
	        addConnectionProperty(TestConstants.PROP_CLIENT_SECRET, TestConstants.CLIENT_SECRET);
	        addConnectionProperty(TestConstants.PROP_REFRESH_TOKEN, TestConstants.REFRESH_TOKEN);
	        setOperationType(OperationType.CREATE);
	        setObjectTypeId("dataset");
	        //RequestContextHelper.setSSLContextForTest();
	}
	
		
	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return PrismConnector.class;
	}

}
