// Copyright (c) 2022 Boomi, LP.
package boomi.connector.oracledatabase;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.oracledatabase.OracleDatabaseBrowser;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.OracleDatabaseConnector;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OracleDatabaseBrowseTest {
	
	private OracleDatabaseConnectionContext context = mock(OracleDatabaseConnectionContext.class);
	private OracleDatabaseConnection connection =  mock(OracleDatabaseConnection.class);
	private OracleDatabaseBrowser browser = mock(OracleDatabaseBrowser.class);
	private ObjectTypes types = mock(ObjectTypes.class);
	@SuppressWarnings("unchecked")
	private List <ObjectType> type = mock(ArrayList.class);
	private PropertyMap operationPropertiesMap = mock(PropertyMap.class);
	OracleDatabaseConnector oracleDatabaseConnector = new OracleDatabaseConnector();
	Connection con = mock(Connection.class);
	DatabaseMetaData metadata = mock(DatabaseMetaData.class);
	ResultSet result = mock(ResultSet.class);
	static String objectTypeId ="PERSON";
	static List<ObjectDefinitionRole> roles = Arrays.asList(ObjectDefinitionRole.INPUT, ObjectDefinitionRole.OUTPUT);
	
	@Before
	public void setup() throws IOException, SQLException {
		
		when(connection.getContext()).thenReturn(context);
		when(connection.getOracleConnection()).thenReturn(con);
		when(con.getMetaData()).thenReturn(metadata);
		when(con.getCatalog()).thenReturn(null);
		when(con.getSchema()).thenReturn(null);
		when(metadata.getColumns(null, null, objectTypeId, null)).thenReturn(result);
		when(type.size()).thenReturn(2);
		when(browser.getObjectTypes()).thenReturn(types);
		when(operationPropertiesMap.get(OracleDatabaseConstants.GET_TYPE)).thenReturn("get");
		when(operationPropertiesMap.get(OracleDatabaseConstants.TYPE)).thenReturn("type");
		when(operationPropertiesMap.get(OracleDatabaseConstants.DELETE_TYPE)).thenReturn("delete");
		when(operationPropertiesMap.get(OracleDatabaseConstants.INSERTION_TYPE)).thenReturn("insert");
		when(operationPropertiesMap.getBooleanProperty("enableQuery")).thenReturn(false);
		when(operationPropertiesMap.getBooleanProperty("INClause")).thenReturn(false);
		when(operationPropertiesMap.getBooleanProperty("refCursor")).thenReturn(false);
		when(context.getOperationProperties()).thenReturn(operationPropertiesMap);
		when(result.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
		when(result.getString("DATA_TYPE")).thenReturn("12");
		when(result.getString("COLUMN_NAME")).thenReturn("ID");
		when(result.getString("TYPE_NAME")).thenReturn("VARCHAR");

	}
	
	@Test()
	public void testGetObjectDefinitionsForCREATE(){
		int actualGetDefinitionSize = 2;
		int actualGetOperationFieldsSize = 1;
		connection.loadProperties();
		when(connection.getContext()).thenReturn(context);
		when(context.getCustomOperationType()).thenReturn(OracleDatabaseConstants.CREATE);
		when(context.getOperationType()).thenReturn(OperationType.CREATE);
		browser=new OracleDatabaseBrowser(connection);
		ObjectDefinitions objectDefinitions = browser.getObjectDefinitions(objectTypeId,roles);
		assertNotNull(objectDefinitions);
		assertEquals(actualGetDefinitionSize, objectDefinitions.getDefinitions().size());
		assertEquals(actualGetOperationFieldsSize, objectDefinitions.getOperationFields().size());
	}

}
