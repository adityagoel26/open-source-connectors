// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorQueryTestContext extends ConnectorTestContext {

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return SAPConnectorConnector.class;
	}
	
	public SAPConnectorQueryTestContext() {
		addConnectionProperty("host", "https://sandbox.api.sap.com/");
		addConnectionProperty("apikey", "gW5Ph6q0KsfnfAfVSWGuUGdGkC3pu4qe");
		addConnectionProperty("businessHubUserName", "kishore.pulluru@accenture.com");
		addConnectionProperty("businessHubPassword", "Kishore@123");
		
		addOperationProperty("url", "s4hanacloud/sap/opu/odata/sap/API_BUSINESS_PARTNER");
		addOperationProperty("top", "10");
		setOperationType(OperationType.QUERY);
		setObjectTypeId("/A_AddressEmailAddress");
	}

}
