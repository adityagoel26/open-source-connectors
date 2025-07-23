/*
*  Copyright 2019 Accenture. All Rights Reserved.
*  The trademarks used in these materials are the properties of their respective owners.
*  This work is protected by copyright law and contains valuable trade secrets and
*  confidential information.
*/
package com.boomi.connector.eventbridge;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;

/**
 * Executes the Create Operation from AWS Event Bridge Connector.
 * 
 * @author a.kumar.samantaray
 *
 */
public class AWSEventBridgeCreateOperationTest {

	private Logger logger = mock(Logger.class);
	private final UpdateRequest request = mock(UpdateRequest.class);
	private final ObjectData objectData = mock(ObjectData.class);
	private final OperationResponse response = mock(OperationResponse.class);
	private final AWSEventBridgeTestContext context = new AWSEventBridgeTestContext();
	private final AWSEventBridgeConnection connection = new AWSEventBridgeConnection(context);
	private final AWSEventBridgeCreateOperation operation = new AWSEventBridgeCreateOperation(connection);
	private List<ObjectData> trackedData = null;

	
	@Before
	public void init() throws IOException {
		trackedData = new ArrayList<>();
		trackedData.add(objectData);
		String str = readJsonFromFile();
		InputStream inputStream = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
		 Map<String, String> parameters = new HashMap<>();
		parameters.put("abc", "123");
		parameters.put("def", "456");
		when(response.getLogger()).thenReturn(logger);
		when(objectData.getData()).thenReturn(inputStream);
	}

	@Test(expected = NullPointerException.class)
	public void testExecuteUpdate() {
		operation.executeSizeLimitedUpdate(request, response);
	}


	@Test @Ignore
	public void testExecuteCreateOperation() {
		operation.executeCreateOperation(trackedData, response);
		assertNotNull(trackedData);
	}

	public String readJsonFromFile() {
		String text = null;
		try {
			text = new String(Files.readAllBytes(Paths.get("src/test/resources/test.txt")));

		} catch (IOException e) {
			logger.info("Error occured in Test class.");
		}
		return text;
	}

}
