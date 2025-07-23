package com.boomi.connector.cosmosdb;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;

public class CosmosDBBrowseContext extends ConnectorTestContext {
	
	public CosmosDBBrowseContext()  {
		addConnectionProperty("hostUrl", "https://boomi-cosmos-db.documents.azure.com:443/");
		addConnectionProperty("masterKey", "TGzECprswfVz0XgQzRhV2ye3XHZJG3gRWRKTno1wKNBWjATP3SL6JSa5QrQwMB1hKcrso2xOUuAUIAxC6Pus2w==");
		addConnectionProperty("databaseName", "boomi_db");
		setOperationType(OperationType.CREATE);
	}

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return CosmosDBConnector.class;
	}
	
	

}
