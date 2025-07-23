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
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;

public class CreateOperationTableTest {

	private Logger logger = Mockito.mock(Logger.class);
	private OperationResponse operationResponse = Mockito.mock(OperationResponse.class);
	private ObjectData objectData = Mockito.mock(ObjectData.class, Mockito.RETURNS_DEEP_STUBS);
	private UpdateRequest updateRequest = Mockito.mock(UpdateRequest.class);
	private OperationContext opContext = Mockito.mock(OperationContext.class);
	private PrismOperationConnection connection = Mockito.mock(PrismOperationConnection.class, Mockito.RETURNS_DEEP_STUBS);
	private PrismResponse prismResponse = Mockito.mock(PrismResponse.class);

	@Before
	public void init() throws IOException {
		String inputJsonString = readJsonFromFile();
		InputStream inputStream = new ByteArrayInputStream(inputJsonString.getBytes(StandardCharsets.UTF_8));
		Mockito.when(connection.getOperationContext()).thenReturn(opContext);
		Mockito.when(opContext.getObjectTypeId()).thenReturn("dataset");
		Mockito.when(operationResponse.getLogger()).thenReturn(logger);
		Mockito.when(objectData.getData()).thenReturn(inputStream);
	}

	@Test
	public void testCreateOperationConnectionCall() {
		Assert.assertNotNull(new CreateOperation(connection).getConnection());
	}

	public String readJsonFromFile() {
		String text = null;
		try {
			text = new String(Files.readAllBytes(Paths.get("src/test/resources/" + "create_table.json")));

		} catch (IOException e) {
			logger.info("Error occured in Test class.");
		}
		return text;
	}

	@Test
	public void testAdditionOfApplicationErrorWhenInvalidInput() throws IOException {
		Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
		Mockito.when(connection.createTable(objectData)).thenReturn(prismResponse);
		new CreateOperation(connection).execute(updateRequest, operationResponse);
		Mockito.verify(prismResponse).addResult(objectData, operationResponse);
		Mockito.verify(connection).createTable(objectData);
	}

	@Test
	public void testAdditionOfApplicationErrorWhenAConnectorExceptionIsThrown() throws IOException {
		Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());

		Exception exception = new ConnectorException("");
		Mockito.doThrow(exception).when(connection).createTable(objectData);

		new CreateOperation(connection).execute(updateRequest, operationResponse);

		Mockito.verify(operationResponse).addResult(ArgumentMatchers.eq(objectData), ArgumentMatchers.eq(OperationStatus.APPLICATION_ERROR), ArgumentMatchers.eq(""),
				ArgumentMatchers.eq("Unknown failure"), (Payload) ArgumentMatchers.isNull(Payload.class));
	}
	
	@Test
    public void testFailWhenNullPointerExceptionIsThrown() throws IOException {
		Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());

        Exception exception = new NullPointerException();
        Mockito.doThrow(exception).when(connection).createTable(objectData);

        new CreateOperation(connection).execute(updateRequest, operationResponse);

		Mockito.verify(operationResponse).addErrorResult(ArgumentMatchers.eq(objectData), ArgumentMatchers.eq(OperationStatus.FAILURE), ArgumentMatchers.eq(""),
                ArgumentMatchers.eq("java.lang.NullPointerException"), ArgumentMatchers.eq(exception));
    }
	
	

}
