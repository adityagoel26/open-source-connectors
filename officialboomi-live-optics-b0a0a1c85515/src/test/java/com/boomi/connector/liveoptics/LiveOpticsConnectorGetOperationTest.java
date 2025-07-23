//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.liveoptics.ArgumentException;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnection;
import com.boomi.connector.liveoptics.LiveOpticsConnectorGetOperation;
import com.boomi.connector.liveoptics.utils.LiveOpticsResponse;

/**
 * @author a.kumar.samantaray
 *
 * 
 */
public class LiveOpticsConnectorGetOperationTest { 

	private Logger logger = mock(Logger.class);
	private static final ObjectIdData data = mock(ObjectIdData.class);
	private final LiveOpticsConnectorConnection connection = mock(LiveOpticsConnectorConnection.class);
	private final LiveOpticsConnectorGetOperation operation = new LiveOpticsConnectorGetOperation(connection);
	private final GetRequest request = mock(GetRequest.class);
	private final OperationResponse response = mock(OperationResponse.class);
	private final LiveOpticsResponse lLoginResp = mock(LiveOpticsResponse.class);
	private final CloseableHttpClient clientConnection = mock(CloseableHttpClient.class);	
	private final HttpGet httpRequest = mock(HttpGet.class);
	private final CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
	private final TrackedData trackedData = mock(TrackedData.class);
 
	@Before
	public void init()
			throws ClientProtocolException, IOException,ArgumentException, InvalidKeyException, NoSuchAlgorithmException, JSONException {
		String nodeString = "1|20191128115253pIVvJAmwaFEd/ochOT+oVQ==9fj9tKKHnbEfgqcRV5Db7aRDHyaUnb1eiFjQpvZMqJoG9b28tEGf3RAFzB04s9VWddcD9RjDDiWkkkqtKksk0fa/kEQGZ6OWnIAKpSu03ufvS7B20TB0LRaWB/dcOlvPLfqfg9S90qid2FMRUS3ptcF2LJD4IB49A8ogtEkFOn/0OVWM7mWjeR0OtzsJKF4E+L9NpeDnzDiUVwuMqweNYw==|WKnbub1Ixcf46RxPBCNFln/XlooIcWSKC7Am455DdUsQiMseoLGVi0FoKFQwPxjl63serdCkvEf2eZEtEZ7d6g==";
		when(response.getLogger()).thenReturn(logger);
		when(connection.doLogin()).thenReturn(lLoginResp);
		when(lLoginResp.getSessionToken()).thenReturn(nodeString);
		when(lLoginResp.getResponseCode()).thenReturn(1);
		when(connection.getLoginSecret()).thenReturn("");
		when(connection.getSharedSecret()).thenReturn("+idBbAUqzNVV02z300zD5PLOfuABOuTWypBv4s2xEqwf");
		when(connection.getIncludeEntities()).thenReturn("true");
		when(clientConnection.execute(httpRequest)).thenReturn(httpResponse);
		when(clientConnection.execute((HttpUriRequest) Mockito.any())).thenReturn(httpResponse);
		when(connection.doClose(Mockito.anyString())).thenReturn(lLoginResp);
		when(trackedData.getLogger()).thenReturn(logger);
		when(clientConnection.execute((HttpGet) any())).thenReturn(httpResponse);
	}

	@Test(expected = NullPointerException.class)
	public void testLiveOpticsConnectorGetOperation() {
		when(request.getObjectId()).thenReturn(data);
		when(lLoginResp.getStatus()).thenReturn(OperationStatus.SUCCESS);
		operation.executeGet(request, response);
	}

	@Test
	public void testLiveOpticsConnectorGetOperationError() {
		assertNotNull(request);
		assertNotNull(response);
		when(request.getObjectId()).thenReturn(data);
		when(lLoginResp.getStatus()).thenReturn(OperationStatus.FAILURE);
		operation.executeGet(request, response);
	}

	@Test
	public void testLiveOpticsConnectorGetOperationCatch() {
		when(lLoginResp.getStatus()).thenReturn(OperationStatus.SUCCESS);
		ObjectIdData getObjectId = mock(ObjectIdData.class);
		getObjectId = null;
		when(request.getObjectId()).thenReturn(getObjectId);
		operation.executeGet(request, response);
	}
}
