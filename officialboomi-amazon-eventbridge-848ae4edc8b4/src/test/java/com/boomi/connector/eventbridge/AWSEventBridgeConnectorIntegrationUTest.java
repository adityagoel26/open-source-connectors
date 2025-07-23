package com.boomi.connector.eventbridge;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import com.boomi.connector.api.Browser;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;

public class AWSEventBridgeConnectorIntegrationUTest {

	@InjectMocks
	private AWSEventBridgeConnector connector;
	private AWSEventBridgeTestContext context;
	private AWSEventBridgeConnection con;
	private OperationResponse response;


	public AWSEventBridgeConnectorIntegrationUTest() {
		context = new AWSEventBridgeTestContext();
		con = new AWSEventBridgeConnection(context);

	}

	@Before
	public void init() {
		MockitoAnnotations.initMocks(this);
		response = mock(OperationResponse.class);
		Logger loggertest = mock(Logger.class);
		when(response.getLogger()).thenReturn(loggertest);

	}

	@Test
	public void testcreateBrowser() {
		Browser browser = connector.createBrowser(context);
		ObjectTypes objType = browser.getObjectTypes();
		assertNotNull(objType);
		ObjectDefinitions _objDefs = browser.getObjectDefinitions("Events",
				Arrays.asList(ObjectDefinitionRole.INPUT, ObjectDefinitionRole.OUTPUT));
		assertNotNull(_objDefs);
		Operation operation = connector.createCreateOperation(context);
		assertNotNull(operation);
	}

	@Test
	public void testexecuteCreateOperation() throws IOException {
		AWSEventBridgeCreateOperation createOp = new AWSEventBridgeCreateOperation(con);
		File initialFile = new File("src/test/resources/test.txt");
		InputStream inStream = new FileInputStream(initialFile);
		SimpleTrackedData trackData = new SimpleTrackedData(1, inStream);
		List<ObjectData> inputs = new ArrayList<>();
		inputs.add(trackData);
		createOp.executeCreateOperation(inputs, response);
		assertNotNull(inStream);
		inStream.close();

	}
	
	 


}
