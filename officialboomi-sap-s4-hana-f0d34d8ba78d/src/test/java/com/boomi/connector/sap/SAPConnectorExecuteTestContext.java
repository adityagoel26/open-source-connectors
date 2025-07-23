// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorExecuteTestContext extends ConnectorTestContext {

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return SAPConnectorConnector.class;
	}
	
	public SAPConnectorExecuteTestContext() {
		addConnectionProperty("host", TestConstasts.HOST);
		addConnectionProperty("apikey", "gW5Ph6q0KsfnfAfVSWGuUGdGkC3pu4qe");
		addConnectionProperty("businessHubUserName", "kishore.pulluru@accenture.com");
		addConnectionProperty("businessHubPassword", "Kishore@123");
		addConnectionProperty("sapUser", TestConstasts.SAPUSER);
		addConnectionProperty("sapPassword", TestConstasts.SAPPASSWORD);
		addOperationProperty("url", "/sap/opu/odata/sap/API_RECIPE");
		addOperationProperty("select", "RecipeUUID,Language,RecipeDescription,to_Recipe");
		addOperationProperty("expand", "to_Recipe");
		setOperationType(OperationType.EXECUTE);
		setObjectTypeId("/A_RecipeText(RecipeUUID={RecipeUUID},Language='{Language}')");
	}

}
