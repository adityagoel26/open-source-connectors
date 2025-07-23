// Copyright (c) 2022 Boomi, LP.
package boomi.connector.oracledatabase;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.oracledatabase.OracleDatabaseConnector;
import com.boomi.connector.testutil.ConnectorTestContext;

public class OracleDatabaseConnectionContext extends ConnectorTestContext {

	public OracleDatabaseConnectionContext() {
		
		
		addConnectionProperty("className", "oracle.jdbc.driver.OracleDriver");
		addConnectionProperty("username", "super");
		addConnectionProperty("password", "");
		addConnectionProperty("url", "jdbc:oracle:tmhin:@172.31.0.20:1521:XE");
		setOperationCustomType("CREATE");
		
	}

	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return OracleDatabaseConnector.class;
	}
}
