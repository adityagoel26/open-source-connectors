//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import org.junit.Assert;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.fasterxml.jackson.databind.JsonNode;

public class CreateOperationBucketTest {


    private Logger logger = Mockito.mock(Logger.class);
	private OperationResponse operationResponse = Mockito.mock(OperationResponse.class);
	private ObjectData objectData = Mockito.mock(ObjectData.class, Mockito.RETURNS_DEEP_STUBS);
	private UpdateRequest updateRequest = Mockito.mock(UpdateRequest.class);
	private OperationContext opContext = Mockito.mock(OperationContext.class);
	private PrismOperationConnection connection = Mockito.mock(PrismOperationConnection.class, Mockito.RETURNS_DEEP_STUBS);
	private PrismResponse prismResponse = Mockito.mock(PrismResponse.class);

	
	@Before
	public void init() throws IOException {
		String inputJsonString=readJsonFromFile();
		InputStream inputStream = new ByteArrayInputStream(inputJsonString.getBytes(StandardCharsets.UTF_8));
		Mockito.when(operationResponse.getLogger()).thenReturn(logger);
		Mockito.when(objectData.getData()).thenReturn(inputStream);
		Mockito.when(opContext.getObjectTypeId()).thenReturn("bucket");
	}
	
	@Test
	public void testCreateOperationConnectionCall() {
		Assert.assertNotNull(new CreateOperation(connection).getConnection());
	}
	
	public String readJsonFromFile() {
		String text = null;
		try {
			text = new String(Files.readAllBytes(Paths.get("src/test/resources/"+"create_bucket.json")));

		} catch (IOException e) {
			logger.info("Error occured in Test class.");
		}
		return text;
	}
	
	
	@Test
	public void testExecuteUpdate() throws IOException {
		Assert.assertNotNull(updateRequest);
		Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
		Mockito.when(connection.createBucket(ArgumentMatchers.any(JsonNode.class))).thenReturn(prismResponse);
		new CreateOperation(connection).execute(updateRequest, operationResponse);
	}
	
	@Test
	public void testExceptionThrown() throws IOException {
		Assert.assertNotNull(updateRequest);
		Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());

		Exception exception = new ConnectorException("");
		Mockito.doThrow(exception).when(connection).createBucket(ArgumentMatchers.any(JsonNode.class));

		new CreateOperation(connection).execute(updateRequest, operationResponse);

	}
	
	@Test
    public void testFailWhenNullPointerExceptionIsThrown() throws IOException {
		Assert.assertNotNull(updateRequest);
		Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
        Exception exception = new NullPointerException();
		Mockito.doThrow(exception).when(connection).createBucket(ArgumentMatchers.any(JsonNode.class));
        new CreateOperation(connection).execute(updateRequest, operationResponse);

    }
	

}
