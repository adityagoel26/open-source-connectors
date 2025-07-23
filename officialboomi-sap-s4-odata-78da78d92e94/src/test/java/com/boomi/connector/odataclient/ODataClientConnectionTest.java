// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.odataclient;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class ODataClientConnectionTest {
    @Mock
    private BrowseContext browseContext;
    @Mock
    private PropertyMap propertyMap;
    @Mock
    private CloseableHttpResponse httpResponse;
    @Mock
    private StatusLine statusLine;

    private ODataClientConnection connection;

    private static final String EXCEPTION_WAS_NOT_THROWN = "Expected exception was not thrown";
    private static final String ERROR_MESSAGE_STRING =
            "Problem connecting to endpoint: https://myendpoint.com/sap/opu/odata/sap/"
                    + "opu/odata/sap/API_BILL_OF_MATERIAL_SRV/?sap-client=001 404 Not Found";
    private static final String ERROR_MESSAGE_INTEGER =
            "Problem connecting to endpoint: https://myendpoint.com/sap/opu/odata/sap/"
            + "opu/odata/sap/API_BILL_OF_MATERIAL_SRV/?sap-client=1 404 Not Found";
    private static final String SERVICE_URL = "https://myendpoint.com/sap/opu/odata";
    private static final Integer STATUS_CODE = 404;
    private static final String SAP_CLIENT_NUMBER = "001";
    private  static final String CONNECTION_SERVICE_URL= "/sap/opu/odata/sap/API_BILL_OF_MATERIAL_SRV/";
    private static final String USERNAME= "username";
    private static final String PASSWORD= "password";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(browseContext.getConnectionProperties()).thenReturn(propertyMap);

        // Setup default property values
        Mockito.when(propertyMap.getProperty(ODataClientConnection.ConnectionProperties.AUTHTYPE.name(),
                ODataClientConnection.AuthType.NONE.name())).thenReturn(ODataClientConnection.AuthType.BASIC.name());
        Mockito.when(propertyMap.getProperty(ODataClientConnection.ConnectionProperties.USERNAME.name()))
                .thenReturn(USERNAME);
        Mockito.when(propertyMap.getProperty(ODataClientConnection.ConnectionProperties.PASSWORD.name()))
                .thenReturn(PASSWORD);
        Mockito.when(propertyMap.getProperty(ODataClientConnection.ConnectionProperties.URL.name(), ""))
                .thenReturn(SERVICE_URL);

        connection = new ODataClientConnection(browseContext);
    }

    /**
     * Tests connection validation with SAP client number as a String.
     * Verifies that appropriate ConnectorException is thrown
     * when attempting connection with invalid client credentials.
     *
     * @throws Exception if test execution fails
     */
    @Test
    public void testConnection_ClientNumber_String() throws Exception {
        Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(STATUS_CODE);
        Mockito.when(propertyMap.getProperty(Mockito.eq(ODataClientConnection.ConnectionProperties.
                TEST_CONNECTION_SERVICE.name()), Mockito.anyString())).thenReturn(CONNECTION_SERVICE_URL);
        Mockito.when(propertyMap.getProperty(Mockito.eq(ODataClientConnection.ConnectionProperties.
                SAP_CLIENT_NUMBER.name()), Mockito.anyString())).thenReturn(SAP_CLIENT_NUMBER);

        try{
            connection.testConnection(); // Should throw exception
            Assert.fail(EXCEPTION_WAS_NOT_THROWN);
        }
        catch (ConnectorException e){
            Assert.assertEquals(ERROR_MESSAGE_STRING,e.getMessage());
        }
    }

    /**
     * Tests connection validation with SAP client number as a integer.
     * Verifies that appropriate ConnectorException is thrown
     * when attempting connection with invalid client credentials.
     *
     * @throws Exception if test execution fails
     */
    @Test
    public void testConnection_ClientNumber_Integer() throws Exception {
        Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(STATUS_CODE);
        Mockito.when(propertyMap.getProperty(Mockito.eq(ODataClientConnection.ConnectionProperties.
                TEST_CONNECTION_SERVICE.name()), Mockito.anyString())).thenReturn(CONNECTION_SERVICE_URL);
        Mockito.when(propertyMap.getProperty(Mockito.eq(ODataClientConnection.ConnectionProperties.
                SAP_CLIENT_NUMBER.name()), Mockito.anyString())).thenThrow(new RuntimeException());

        Mockito.when(propertyMap.getLongProperty(Mockito.eq(ODataClientConnection.ConnectionProperties.
                SAP_CLIENT_NUMBER.name()), Mockito.anyLong())).thenReturn(1L);

        try{
            connection.testConnection(); // Should throw exception
            Assert.fail(EXCEPTION_WAS_NOT_THROWN);
        }
        catch (ConnectorException e){
            Assert.assertEquals(ERROR_MESSAGE_INTEGER, e.getMessage());
        }
    }
}
