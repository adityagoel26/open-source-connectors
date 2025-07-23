package com.boomi.connector.cosmosdb;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.exception.CosmosDBRetryException;
import com.boomi.connector.testutil.SimpleTrackedData;

public class CosmosDBCreateOperationTest {
	
	private CosmosDBBrowseContext context = new CosmosDBBrowseContext();
	private CosmosDBConnection conn = new CosmosDBConnection(context);
	private CosmosDBCreateOperation ops = new CosmosDBCreateOperation(conn);
	private UpdateRequest request = mock(UpdateRequest.class);
	private OperationResponse response = mock(OperationResponse.class);
	private Logger logger = mock(Logger.class);
	private ObjectData objectData = mock(ObjectData.class);
//	private final UpdateRequest updateRequest = mock(UpdateRequest.class);
	
	
	@Before
	public void init() {
		when(response.getLogger()).thenReturn(logger);
	}
	
	@Test(expected =NullPointerException.class)
	public void testexecuteAUpdate() {
		ops.executeSizeLimitedUpdate(request, response);
	}
	
	@Test
	public void testexecuteCreateOperation() throws IOException, CosmosDBConnectorException, CosmosDBRetryException {
		File initialFile = new File("src/test/resource/TestMessage.txt");
	    InputStream inStream = new FileInputStream(initialFile);
		SimpleTrackedData trackData = new SimpleTrackedData(1, inStream);
		List<ObjectData> inputs = new ArrayList<>();
		inputs.add(trackData);
//		conn.doCreate(objectData, "school_coll");
		for(ObjectData objData : inputs) {
			conn.doCreate(objData, "school_coll");
		}
		
//		ops.executeSizeLimitedUpdate(, response);
		assertNotNull(inStream);
		assertNotNull(objectData);
		close(inStream);
	
	}

	private void close(InputStream inStream) {
		try {
			inStream.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE,"Closing Resources failed");
		}
		
	}

}
