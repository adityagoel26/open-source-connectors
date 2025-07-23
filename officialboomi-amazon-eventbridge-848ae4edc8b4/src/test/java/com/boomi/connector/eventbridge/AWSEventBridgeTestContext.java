package com.boomi.connector.eventbridge;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;

public class AWSEventBridgeTestContext extends ConnectorTestContext {
	public AWSEventBridgeTestContext() {
		addConnectionProperty("accessKey", AWSEventBridgeTestConstant.ACCESSKEY);
		addConnectionProperty("awsSecretKey", AWSEventBridgeTestConstant.SECRETEKEY);
		addConnectionProperty("customAwsRegion", AWSEventBridgeTestConstant.CUSTOMREGION);
		addConnectionProperty("awsRegion", AWSEventBridgeTestConstant.REGION);
		setOperationType(OperationType.CREATE);
		setObjectTypeId("Events");
		addConnectionProperty("awsAccountID", AWSEventBridgeTestConstant.ACCOUNTID);

	}

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return AWSEventBridgeConnector.class;
	}

	class AWSEventBridgeTestConstant {
		private AWSEventBridgeTestConstant() {	
		}
		public static final String REGION = "us-east-2";
		public static final String CUSTOMREGION = "us-east-2";

	}

}