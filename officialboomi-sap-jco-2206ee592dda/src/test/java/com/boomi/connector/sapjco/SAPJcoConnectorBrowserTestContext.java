// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import java.util.HashMap;
import java.util.Map;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.testutil.ConnectorTestContext;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoConnectorBrowserTestContext extends ConnectorTestContext {

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return SAPJcoConnector.class;
	}

	public SAPJcoConnectorBrowserTestContext() {
		Map<String, Object> connectionProperties = new HashMap<>();
		Map<String, Object> operationProperties = new HashMap<>();
		connectionProperties.put("connectionType", "AHOST");
		connectionProperties.put("server", "");
		connectionProperties.put("userName", "");
		connectionProperties.put("password", "");
		connectionProperties.put("client", "800");
		connectionProperties.put("languageCode", "EN");
		connectionProperties.put("systemNumber", "05");
		connectionProperties.put("enableTrace", false);
		connectionProperties.put("enableLowLatencyLogging", false);

		operationProperties.put(SAPJcoConstants.FUNCTION_NAME, "customer*");
		operationProperties.put(SAPJcoConstants.FUNCTION_TYPE, "BUSINESS_OBJECT");

		operationProperties.put(SAPJcoConstants.COMMIT_TXN, true);
		getConnectionProperties().putAll(connectionProperties);
		getOperationProperties().putAll(operationProperties);
		setObjectTypeId("BAPI_EMPLOYEE_GETDATA");
		setOperationType(OperationType.EXECUTE);
		setOperationCustomType("EXECUTE");
	}

}
