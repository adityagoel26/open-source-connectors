//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.xml.sax.SAXException;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.liveoptics.utils.JsonSchemaBuilder;
import com.boomi.connector.liveoptics.utils.LiveOpticsResponse;
/**
 * @author aditi ardhapure
 *
 *		{tags} 
 */

public class LiveOpticsConnectorConnectionTest {

	private LiveOpticsTestContext context = new LiveOpticsTestContext();
	private LiveOpticsConnectorConnection connection = new LiveOpticsConnectorConnection(context);
	HttpURLConnection conn = mock(HttpURLConnection.class);
	JSONObject jsonObject = mock(JSONObject.class);
	InputStream responseStream = mock(InputStream.class);
	private final LiveOpticsResponse lLoginResp = mock(LiveOpticsResponse.class);
	private static final int EXPECTED_LENGTH = 345;
	
	@Test
	public void testDoClose() throws ArgumentException, IOException, JSONException {
		//when(request.getObjectId()).thenReturn(data);
		assertNotNull(LiveOpticsTestConstants.SESSIONTOKEN);
		connection.doClose(LiveOpticsTestConstants.SESSIONTOKEN);
	}

	@Test
	public void testGetMetadata() throws IOException,SAXException{	 
		PowerMockito.mockStatic(JsonSchemaBuilder.class);
		String value = connection.getMetadata();
		assertNotNull(value); 
	}

	@Test(expected=Exception.class)
	public void testDoLogin() throws IOException,ArgumentException, InvalidKeyException, NoSuchAlgorithmException, JSONException {
		when(conn.getInputStream()).thenReturn(responseStream);
		connection.doLogin();
	}

	@Test
	public void testGetLiveOpticsResponse() throws Exception {
		when(lLoginResp.getStatus()).thenReturn(OperationStatus.SUCCESS);
		assertNotNull(conn);
		assertNotNull(lLoginResp);
		connection.getLiveOpticsResponse(conn, lLoginResp);
	}

	@Test
	public void testGetLiveOpticsResponseElse() throws Exception {
		when(lLoginResp.getStatus()).thenReturn(OperationStatus.FAILURE);
		assertNotNull(conn);
		assertNotNull(lLoginResp);
		connection.getLiveOpticsResponse(conn, lLoginResp);
	}

	@Test
	public void testGetLiveOpticsResponseNull() throws Exception {
		assertNotNull(lLoginResp);
		connection.getLiveOpticsResponse(null, lLoginResp);
	}
	
	@Test
	public void testGetters() {
		assertNotNull(connection.getBaseUrl());
		assertNotNull(connection.getSharedSecret());
		assertNotNull(connection.getLoginSecret());
		assertNull(connection.getIncludeEntities());
	}
	
	@Test
	public void testParse() throws Exception {
		String[] expectedResult = new String[3];
		String[] actualResult = LiveOpticsTestConstants.MAYBESESSION.split("\\|");
		assertEquals(expectedResult.length, actualResult.length);
		connection.parse(LiveOpticsTestConstants.MAYBESESSION, LiveOpticsTestConstants.SHARED_SECRET,Base64.decodeBase64(LiveOpticsTestConstants.LOGIN_SECRET));
	}

	@Test(expected=Exception.class)
	public void testParseMayBeSessionLengthEqualToExpected() throws Exception {
		assertTrue(LiveOpticsTestConstants.MAYBESESSION_LENGTHCHECK.length() == EXPECTED_LENGTH);
		connection.parse(LiveOpticsTestConstants.MAYBESESSION_LENGTHCHECK, LiveOpticsTestConstants.SHARED_SECRET,
				Base64.decodeBase64(LiveOpticsTestConstants.LOGIN_SECRET));
	}

	@Test(expected=ArgumentException.class)
	public void testParseMayBeSessionLengthNotEqualToExpected() throws Exception {
		assertTrue(LiveOpticsTestConstants.MAYBESESSION_LESSERLENGTH.length() != EXPECTED_LENGTH);
		connection.parse(LiveOpticsTestConstants.MAYBESESSION_LESSERLENGTH, LiveOpticsTestConstants.SHARED_SECRET,
				Base64.decodeBase64(LiveOpticsTestConstants.LOGIN_SECRET));
	}
	
	@Test(expected=ArgumentException.class)
	public void testParseCheckVersionLength() throws Exception {
		String[] lParts = LiveOpticsTestConstants.MAYBESESSION_VERSIONLENGTH.split("\\|");
		String lVersion = lParts[1];
		assertNotNull(lVersion);
		connection.parse(LiveOpticsTestConstants.MAYBESESSION_VERSIONLENGTH, LiveOpticsTestConstants.SHARED_SECRET,
				Base64.decodeBase64(LiveOpticsTestConstants.LOGIN_SECRET));
	}
	
	@Test(expected=ArgumentException.class)
	public void testParselSessionBlobLength() throws Exception {
		String[] lParts = LiveOpticsTestConstants.MAYBESESSION_BLOBCHECK.split("\\|");
		String lSessionBlob = lParts[1];
		assertNotNull(lSessionBlob);
		//assertTrue(lSessionBlob.length() != 254);
		connection.parse(LiveOpticsTestConstants.MAYBESESSION_BLOBCHECK, LiveOpticsTestConstants.SHARED_SECRET,
				Base64.decodeBase64(LiveOpticsTestConstants.LOGIN_SECRET));

	}
	
}
