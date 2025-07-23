//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.workdayprism.utils.TestConstants;
import com.boomi.connector.api.ConnectorException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author saurav.b.sengupta
 *
 */
public class CredentialsTest {
	
	public Credentials createValidCredentials() {
		
		return new Credentials(TestConstants.API_ENDPOINT, TestConstants.CLIENT_ID, TestConstants.CLIENT_SECRET, TestConstants.REFRESH_TOKEN);
	}
	
	@Test
	public void testParseEndpoint() {
		final String expectedBaseEndpoint = "https://wd2-impl-services1.workday.com";
        final String expectedTenant = "dellboomi_pt1";
        
        Credentials actualCredentials = createValidCredentials();
 
        Assert.assertEquals(expectedBaseEndpoint, actualCredentials.getBaseEndpoint());
        Assert.assertEquals(expectedTenant, actualCredentials.getTenant());
		
	}
	
	@Test (expected = ConnectorException.class)
	public void testMalformedEndpoint() {
		 String endpoint = "invalid.endpoint.com.malformed";
		 new Credentials(endpoint, TestConstants.CLIENT_ID, TestConstants.CLIENT_SECRET, TestConstants.REFRESH_TOKEN);
	}
	
	@Test (expected = ConnectorException.class)
	public void testWrongEndpoint() {
		 String endpoint = "https://invalid.endpoint.com/very-wrong";
		 new Credentials(endpoint, TestConstants.CLIENT_ID, TestConstants.CLIENT_SECRET, TestConstants.REFRESH_TOKEN);
	}
	
	@Test
	public void testEqualsAndHashCode() {
		
		Credentials c1=createValidCredentials();
		Credentials c2=createValidCredentials();
		Credentials c3=new Credentials("http://wd2-impl-services2.workday.com/ccx/opa/v1/dellboomi_pt2", TestConstants.CLIENT_ID, TestConstants.CLIENT_SECRET, TestConstants.REFRESH_TOKEN);
		Credentials c4=new Credentials(TestConstants.API_ENDPOINT, "clientId", TestConstants.CLIENT_SECRET, TestConstants.REFRESH_TOKEN);
		Credentials c5=new Credentials(TestConstants.API_ENDPOINT, TestConstants.CLIENT_ID, "clientSecret", TestConstants.REFRESH_TOKEN);
		Credentials c6=new Credentials(TestConstants.API_ENDPOINT, TestConstants.CLIENT_ID, TestConstants.CLIENT_SECRET, "refreshToken");
		
		Assert.assertTrue(c1.equals(c1));
		Assert.assertTrue(c1.equals(c2));
		Assert.assertFalse(c1.equals(c3));
		Assert.assertFalse(c1.equals(c4));
		Assert.assertFalse(c1.equals(c5));
		Assert.assertFalse(c1.equals(c6));
		
		Assert.assertEquals(c1.hashCode(), c2.hashCode());
		Assert.assertNotEquals(c1.hashCode(), c3.hashCode());
		Assert.assertNotEquals(c1.hashCode(), c4.hashCode());
		Assert.assertNotEquals(c1.hashCode(), c5.hashCode());
		Assert.assertNotEquals(c1.hashCode(), c6.hashCode());
	}
	
	@Test
    public void getBasicAuthTest() {
		String expectedBasicAuth="TVdOa1lqVXdNakV0TkdSa1l5MDBObUUzTFRoa1pHTXRPRFUyTkdReVlqQX"
				+ "dNVFJqOjMzcHVmbHFldzM1ODlua3VnMTg2Y3FsemVtc3VucGpoeWg3OGdhajJwZmRmMjhxYjZrZ"
				+ "W80ZXlqcnhqdXhmMHQ0bGIydXp6a251NnExYW4zMDgwbTNlbDVqeWo1Y2V5ZXNmeQ==";
		Credentials actual=createValidCredentials();
		
		Assert.assertEquals(expectedBasicAuth, actual.getBasicAuth());
		
	}
	
	@Test
    public void getBasePathTest() {
		String expectedBasePath="https://wd2-impl-services1.workday.com/ccx/api/prismAnalytics/v2/dellboomi_pt1";
		String path="ccx/api/prismAnalytics/v2/";
		Credentials actual=createValidCredentials();
		String basePath=actual.getBasePath(path);
		Assert.assertEquals(expectedBasePath, actual.getBasePath(path));	
	}


}
