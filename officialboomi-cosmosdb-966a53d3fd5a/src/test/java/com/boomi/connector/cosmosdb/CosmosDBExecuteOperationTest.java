package com.boomi.connector.cosmosdb;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.bean.GetOperationRequest;
import com.boomi.connector.cosmosdb.util.DocumentUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DocumentUtil.class, HttpClientBuilder.class, ResponseUtil.class})
@PowerMockIgnore("javax.net.ssl.*")
public class CosmosDBExecuteOperationTest {
	
	private Logger logger = mock(Logger.class);
	private static final ObjectData data = mock(ObjectData.class);
	private final CosmosDBConnection connection = mock(CosmosDBConnection.class);
	private final UpdateRequest updateRequest = mock(UpdateRequest.class);
	private final CosmosDBExecuteOperation operation = new CosmosDBExecuteOperation(connection);
	private final OperationResponse response = mock(OperationResponse.class);
	private final CloseableHttpClient clientConnection = mock(CloseableHttpClient.class);	
	private final HttpRequestBase httpRequest = mock(HttpRequestBase.class);
	private final CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
	private final GetOperationRequest getOperationRequest = mock(GetOperationRequest.class);
	private final HttpClientBuilder clientBuilder = mock(HttpClientBuilder.class);
	private final StatusLine statusLine = mock(StatusLine.class);
	private final HttpEntity entity = mock(HttpEntity.class);
	private final InputStream inputStream = mock(InputStream.class);
	OperationContext context = mock(OperationContext.class);
	
	@Before
	public void init() throws ClientProtocolException, IOException, InvalidKeyException, NoSuchAlgorithmException, URISyntaxException {
		when(response.getLogger()).thenReturn(logger);
		when(clientConnection.execute(httpRequest)).thenReturn(httpResponse);
		when(connection.buildUriRequest(Mockito.anyString(), Mockito.anyString())).thenReturn(httpRequest);
		PowerMockito.mockStatic(HttpClientBuilder.class);
		when(HttpClientBuilder.create()).thenReturn(clientBuilder);
		when(operation.getContext()).thenReturn(context);
		when(operation.getContext().getObjectTypeId()).thenReturn("");
		when(clientBuilder.build()).thenReturn(clientConnection);
		when(httpResponse.getStatusLine()).thenReturn(statusLine);
		when(httpResponse.getEntity()).thenReturn(entity);
		when(entity.getContent()).thenReturn(inputStream);
	}
	
	@Test
	public void testCosmosConnectorGetOperation() throws IOException {
		List<ObjectData> list = new ArrayList<ObjectData>();
		list.add(data);
		when(updateRequest.iterator()).thenReturn(list.iterator());
		when(getOperationRequest.getPartitionKey()).thenReturn("1");
		when(operation.getContext()).thenReturn(context);
		when(operation.getContext().getObjectTypeId()).thenReturn(Mockito.anyString());
		PowerMockito.mockStatic(DocumentUtil.class);
		when(DocumentUtil.getRequestData(data)).thenReturn(getOperationRequest);
		when(clientConnection.execute(httpRequest)).thenReturn(httpResponse);
		when(statusLine.getStatusCode()).thenReturn(200);
		operation.executeUpdate(updateRequest, response);
		assertNotNull(response);
	}
	
	@Test
	public void testCosmosConnectorGetOperationError() throws IOException {
		List<ObjectData> list = new ArrayList<ObjectData>();
		list.add(data);
		when(updateRequest.iterator()).thenReturn(list.iterator());
		when(getOperationRequest.getPartitionKey()).thenReturn("1");
		when(operation.getContext()).thenReturn(context);
		when(operation.getContext().getObjectTypeId()).thenReturn(Mockito.anyString());
		PowerMockito.mockStatic(DocumentUtil.class);
		when(DocumentUtil.getRequestData(data)).thenReturn(getOperationRequest);
		when(clientConnection.execute(httpRequest)).thenReturn(httpResponse);
		when(statusLine.getStatusCode()).thenReturn(400);
		operation.executeUpdate(updateRequest, response);
		assertNotNull(response);
	}
	@Test
	public void testCosmosConnectorGetOperationNullResponse() throws IOException {
		List<ObjectData> list = new ArrayList<ObjectData>();
		list.add(data);
		when(updateRequest.iterator()).thenReturn(list.iterator());
		when(getOperationRequest.getId()).thenReturn("1");
		PowerMockito.mockStatic(DocumentUtil.class);
		when(DocumentUtil.getRequestData(data)).thenReturn(getOperationRequest);
		when(clientConnection.execute(httpRequest)).thenReturn(httpResponse);
		when(entity.getContent()).thenReturn(null);
		when(statusLine.getStatusCode()).thenReturn(200);
		operation.executeUpdate(updateRequest, response);
		assertNotNull(response);
	}
	
	@Test
	public void testCosmosConnectorGetOperationNullException() throws IOException {
		operation.executeUpdate(updateRequest, response);
		assertNotNull(response);
	}
	
	@Test
	public void testdoGetException() throws IOException {
		List<ObjectData> list = new ArrayList<ObjectData>();
		list.add(data);
		when(updateRequest.iterator()).thenReturn(list.iterator());
		PowerMockito.mockStatic(ResponseUtil.class);
		operation.executeUpdate(updateRequest, response);
		assertNotNull(response);
	}
	
	@Test
	public void testdoGetError() throws IOException {
		List<ObjectData> list = new ArrayList<ObjectData>();
		list.add(data);
		when(updateRequest.iterator()).thenReturn(list.iterator());
		when(getOperationRequest.getId()).thenReturn("1");
		PowerMockito.mockStatic(DocumentUtil.class);
		when(DocumentUtil.getRequestData(data)).thenReturn(getOperationRequest);
		when(clientConnection.execute(httpRequest)).thenReturn(null);
		PowerMockito.mockStatic(ResponseUtil.class);
		operation.executeUpdate(updateRequest, response);
		assertNotNull(response);
	}

}
