// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm;

import com.boomi.common.apache.http.entity.CloseableInputStreamEntity;
import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HubspotcrmResponseUtil;
import com.boomi.connector.testutil.MutablePropertyMap;

import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * The HubspotcrmConnectionTest class contains test cases for the HubSpotcrmConnection.
 */
class HubspotcrmConnectionTest {

    private PropertyMap _connectionProperties;

    private HubspotcrmConnection connection;

    List<ObjectType> objectListPost = new ArrayList<>();
    ObjectType contactCreate = new ObjectType();
    ObjectType companyCreate = new ObjectType();
    ObjectType ticketCreate = new ObjectType();
    ObjectType dealsCreate = new ObjectType();

    List<ObjectType> objectListQuery = new ArrayList<>();
    ObjectType contactSearch = new ObjectType();
    ObjectType companySearch = new ObjectType();
    ObjectType ticketsSearch = new ObjectType();
    ObjectType dealsSearch = new ObjectType();
    ObjectType goalsSearch = new ObjectType();

    List<ObjectType> objectListDelete = new ArrayList<>();
    ObjectType contactDelete = new ObjectType();
    ObjectType companyDelete = new ObjectType();
    ObjectType ticketDelete = new ObjectType();
    ObjectType dealsDelete = new ObjectType();

    List<ObjectType> objectListPatch = new ArrayList<>();
    ObjectType contactUpdate = new ObjectType();
    ObjectType companyUpdate = new ObjectType();
    ObjectType ticketUpdate = new ObjectType();
    ObjectType dealsUpdate = new ObjectType();

    List<ObjectType> objectListRetrieve = new ArrayList<>();
    ObjectType contactRetrieve = new ObjectType();
    ObjectType companyRetrieve = new ObjectType();
    ObjectType ticketsRetrieve = new ObjectType();
    ObjectType dealsRetrieve = new ObjectType();
    ObjectType goalsRetrieve = new ObjectType();

    @Mock
    private BrowseContext browseContext;

    private static final String CUSTOM_SPEC = "custom-specification-hubspot_crm.yaml";

    /**
     * Sets up the connection properties for testing the HubSpot CRM connector.
     */
    @BeforeEach
    public void setup() {
        MockitoAnnotations.initMocks(this);
        _connectionProperties = new MutablePropertyMap();
        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("Authorization", "Bearer someToken");
        customHeaders.put("Content-Type", "application/json");
        _connectionProperties.put("Server", "https://api.hubapi.com");
        _connectionProperties.put("url", "https://api.hubapi.com");
        _connectionProperties.put("Grant Type", "Authorization Code");
        _connectionProperties.put("customHeaders", customHeaders);
        _connectionProperties.put("oauthContext", mock(OAuth2Context.class));

        contactCreate.setId("POST::/crm/v3/objects/contacts");
        contactCreate.setLabel("contacts");

        companyCreate.setId("POST::/crm/v3/objects/companies");
        companyCreate.setLabel("companies");

        ticketCreate.setId("POST::/crm/v3/objects/tickets");
        ticketCreate.setLabel("tickets");

        dealsCreate.setId("POST::/crm/v3/objects/deals");
        dealsCreate.setLabel("deals");

        companyDelete.setId("DELETE::/crm/v3/objects/companies/{companyId}");
        companyDelete.setLabel("companies");

        contactDelete.setId("DELETE::/crm/v3/objects/contacts/{contactId}");
        contactDelete.setLabel("contacts");

        ticketDelete.setId("DELETE::/crm/v3/objects/tickets/{ticketId}");
        ticketDelete.setLabel("tickets");

        dealsDelete.setId("DELETE::/crm/v3/objects/deals/{dealId}");
        dealsDelete.setLabel("deals");

        contactUpdate.setId("PATCH::/crm/v3/objects/contacts/{contactId}");
        contactUpdate.setLabel("contacts");

        contactSearch.setId("POST::/crm/v3/objects/contacts/search");
        contactSearch.setLabel("contacts");

        companySearch.setId("POST::/crm/v3/objects/companies/search");
        companySearch.setLabel("companies");

        ticketsSearch.setId("POST::/crm/v3/objects/tickets/search");
        ticketsSearch.setLabel("tickets");

        dealsSearch.setId("POST::/crm/v3/objects/deals/search");
        dealsSearch.setLabel("deals");

        goalsSearch.setId("POST::/crm/v3/objects/goal_targets/search");
        goalsSearch.setLabel("goals");

        companyUpdate.setId("PATCH::/crm/v3/objects/companies/{companyId}");
        companyUpdate.setLabel("companies");

        ticketUpdate.setId("PATCH::/crm/v3/objects/tickets/{ticketId}");
        ticketUpdate.setLabel("tickets");

        dealsUpdate.setId("PATCH::/crm/v3/objects/deals/{dealId}");
        dealsUpdate.setLabel("deals");



        objectListPost.add(contactCreate);
        objectListPost.add(companyCreate);
        objectListPost.add(ticketCreate);
        objectListPost.add(dealsCreate);

        objectListQuery.add(contactSearch);
        objectListQuery.add(companySearch);
        objectListQuery.add(ticketsSearch);
        objectListQuery.add(dealsSearch);
        objectListQuery.add(goalsSearch);

        objectListDelete.add(contactDelete);
        objectListDelete.add(companyDelete);
        objectListDelete.add(ticketDelete);
        objectListDelete.add(dealsDelete);

        objectListPatch.add(contactUpdate);
        objectListPatch.add(companyUpdate);
        objectListPatch.add(ticketUpdate);
        objectListPatch.add(dealsUpdate);

        contactRetrieve.setId("GET::/crm/v3/objects/contacts/{contactId}");
        contactRetrieve.setLabel("contacts");

        companyRetrieve.setId("GET::/crm/v3/objects/companies/{companyId}");
        companyRetrieve.setLabel("companies");

        ticketsRetrieve.setId("GET::/crm/v3/objects/tickets/{ticketId}");
        ticketsRetrieve.setLabel("tickets");

        dealsRetrieve.setId("GET::/crm/v3/objects/deals/{dealId}");
        dealsRetrieve.setLabel("deals");

        goalsRetrieve.setId("GET::/crm/v3/objects/goal_targets/{goalTargetId}");
        goalsRetrieve.setLabel("goals");

        objectListRetrieve.add(contactRetrieve);
        objectListRetrieve.add(companyRetrieve);
        objectListRetrieve.add(ticketsRetrieve);
        objectListRetrieve.add(dealsRetrieve);
        objectListRetrieve.add(goalsRetrieve);

        when(browseContext.getConnectionProperties()).thenReturn(_connectionProperties);
        when(browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());

        connection = new HubspotcrmConnection(browseContext);
    }

    /**
     * Tests the header generation process for the HubspotCRM connection with an empty API key.
     * This test method simulates an API Key authenticated request to the HubspotCRM API
     * where the API key is intentionally left empty. It verifies that:
     * - The "Authorization" header contains "Bearer fakeBearerToken" when using OAuth2.
     * - The headers do not include a blank API key value.
     * Assertions:
     * - Ensures the "Authorization" header has the correct value ("Bearer fakeBearerToken").
     * - Verifies that the headers do not include a blank "Authorization" header (e.g., "Bearer ").
     *
     * @throws IOException if an I/O error occurs during the test execution.
     */
    @Test
    void testExecuteApiKey1() throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        CloseableInputStreamEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream("response".getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);

        StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK");
        when(response.getEntity()).thenReturn(entity);
        when(response.getStatusLine()).thenReturn(statusLine);
        OAuth2Token oAuthToken = mock(OAuth2Token.class);
        when(oAuthToken.getAccessToken()).thenReturn("fakeBearerToken");
        when(connection.getOAuthContext().getOAuth2Token(false)).thenReturn(oAuthToken);

        try (MockedStatic<HubspotcrmResponseUtil> responseUtil = mockStatic(HubspotcrmResponseUtil.class)) {
            responseUtil.when(() -> HubspotcrmResponseUtil.getWithResettableContentStream(any())).thenReturn(response);
            _connectionProperties.put("apiKey", "");
            _connectionProperties.put("auth", "ACCESS_TOKEN");
            List<Map.Entry<String, String>> headers = connection.getHeaders();
            // Assert that the headers contain "Authorization: Bearer fakeBearerToken"
            boolean containsAuthHeader = headers.stream()
                    .anyMatch(entry -> "Authorization".equals(entry.getKey()) &&
                            "Bearer fakeBearerToken".equals(entry.getValue()));
            Assertions.assertTrue(containsAuthHeader, "Headers should contain Bearer fakeBearerToken");

            // Assert that the headers do not contain a blank API key
            boolean containsBlankApiKey = headers.stream()
                    .anyMatch(entry -> "Authorization".equals(entry.getKey()) && "Bearer ".equals(entry.getValue()));
            Assertions.assertFalse(containsBlankApiKey, "Headers should not contain a blank API key");

        }
    }

    /**
     * Tests the execute() method of the HubspotcrmConnection class when the HTTP response is successful.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    void testExecuteApiKey() throws IOException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        CloseableInputStreamEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream("response".getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);

        StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK");
        when(response.getEntity()).thenReturn(entity);
        when(response.getStatusLine()).thenReturn(statusLine);
        try (MockedStatic<HubspotcrmResponseUtil> responseUtil = mockStatic(HubspotcrmResponseUtil.class)) {
            responseUtil.when(() -> HubspotcrmResponseUtil.getWithResettableContentStream(any())).thenReturn(response);
            _connectionProperties.put("apiKey", "api-key");
            _connectionProperties.put("auth", "ACCESS_TOKEN");
            CloseableHttpResponse response1 = ExecutionUtils.execute("GET", "/crm/v3/contacts", client, null,
                    connection.getUrl(), connection.getHeaders(), null);
            assertEquals(response, response1);
            response.close();
        }
    }

    /**
     * Tests the OAuth2 execution process for the HubspotCRM connection.
     * This test method simulates an OAuth2 authenticated request to the HubspotCRM API.
     * It mocks various components including the HTTP client, response, and OAuth context
     * to verify the correct behavior of the connection execution.
     *
     * @throws IOException if an I/O error occurs during the test execution
     */
    @Test
    void testExecuteOAuth2() throws IOException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        CloseableInputStreamEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream("response".getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);

        StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK");
        when(response.getEntity()).thenReturn(entity);
        when(response.getStatusLine()).thenReturn(statusLine);
        try (MockedStatic<HubspotcrmResponseUtil> responseUtil = mockStatic(HubspotcrmResponseUtil.class)) {
            responseUtil.when(() -> HubspotcrmResponseUtil.getWithResettableContentStream(any())).thenReturn(response);
            _connectionProperties.put("auth", "OAUTH2");
            _connectionProperties.put("oauthContext", mock(OAuth2Context.class));
            _connectionProperties.put("clientID", "clientID");
            _connectionProperties.put("clientSecretId", "clientSecretId");
            _connectionProperties.put("scopes",
                    "crm.objects.companies.read crm.objects.companies.write crm.objects.contacts.read crm.objects"
                            + ".contacts.write crm.objects.deals.read crm.objects.deals.write");

            OAuth2Token oAuthToken = mock(OAuth2Token.class);
            when(oAuthToken.getAccessToken()).thenReturn("fakeBearerToken");
            when(connection.getOAuthContext().getOAuth2Token(false)).thenReturn(oAuthToken);

            CloseableHttpResponse response1 = ExecutionUtils.execute("GET", "/crm/v3/contacts", client, null,
                    connection.getUrl(), connection.getHeaders(), null);
            assertEquals(response, response1);
            response.close();
        }
    }

    /**
     * Tests the testConnection() method of the HubspotcrmConnection class when the connection is successful.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    void testTestConnectionPositive() throws IOException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        String path = "/crm/v3/contacts";

        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        ArgumentCaptor<HttpRequestBase> requestCaptor = ArgumentCaptor.forClass(HttpRequestBase.class);
        when(client.execute(requestCaptor.capture())).thenReturn(response);
        _connectionProperties.put("apiKey", "api-key");
        _connectionProperties.put("auth", "ACCESS_TOKEN");

        try (MockedStatic<HubspotcrmResponseUtil> responseUtil = mockStatic(HubspotcrmResponseUtil.class)) {
            responseUtil.when(() -> HubspotcrmResponseUtil.getWithResettableContentStream(any())).thenReturn(response);

            connection.testConnection(client, path);
            assertEquals("https://api.hubapi.com/crm/v3/contacts", requestCaptor.getValue().getURI().toString());
            assertEquals("Bearer api-key", requestCaptor.getValue().getFirstHeader("Authorization").getValue());
        }
    }

    /**
     * Tests the testConnection() method of the HubspotcrmConnection class when the connection fails.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    void testTestConnectionNegative() throws IOException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        String path = "/crm/v3/contacts";
        when(client.execute(any(HttpRequestBase.class))).thenThrow(new IOException("Connection failed"));
        _connectionProperties.put("apiKey", "api-key");
        _connectionProperties.put("auth", "ACCESS_TOKEN");

        Exception exception = Assert.assertThrows(IOException.class, () -> {
            connection.testConnection(client, path);
        });
        assertEquals("Connection failed", exception.getMessage());
    }

    /**
     * Tests the getSpec() method of the HubspotcrmConnection class when the spec property is set.
     */
    @Test
    void testGetSpec() {
        assertEquals(CUSTOM_SPEC, connection.getSpec());
    }

    /**
     * Tests the getObjectTypes() method of the HubspotcrmConnection class.
     */
    @Test
    void testGetObjectTypesCreate() {
        when(browseContext.getCustomOperationType()).thenReturn("POST");
        List<ObjectType> result = connection.getObjectTypes();
        Assertions.assertNotNull(result);
        assertEquals(objectListPost.size(), result.size());
        for (int i = 0; i < objectListPost.size(); i++) {
            assertEquals(objectListPost.get(i).getId(), result.get(i).getId());
            assertEquals(objectListPost.get(i).getLabel(), result.get(i).getLabel());
        }
    }

    /**
     * Tests the getObjectTypes() method of the HubspotcrmConnection class.
     */
    @Test
    void testGetObjectTypesDelete() {
        when(browseContext.getCustomOperationType()).thenReturn("DELETE");
        List<ObjectType> result = connection.getObjectTypes();
        Assertions.assertNotNull(result);
        assertEquals(objectListDelete.size(), result.size());
        for (int i = 0; i < objectListDelete.size(); i++) {
            assertEquals(objectListDelete.get(i).getId(), result.get(i).getId());
            assertEquals(objectListDelete.get(i).getLabel(), result.get(i).getLabel());
        }
    }

    /**
     * Tests the getObjectTypes() method for update operation.
     */
    @Test
    void testGetObjectTypesUpdate() {
        when(browseContext.getCustomOperationType()).thenReturn("PATCH");
        List<ObjectType> result = connection.getObjectTypes();
        Assertions.assertNotNull(result);
        assertEquals(objectListPatch.size(), result.size());
        for (int i = 0; i < objectListPatch.size(); i++) {
            assertEquals(objectListPatch.get(i).getId(), result.get(i).getId());
            assertEquals(objectListPatch.get(i).getLabel(), result.get(i).getLabel());
        }
    }

    /**
     * Tests the getObjectTypes() method for search operation.
     */
    @Test
    void testGetObjectTypesSearch() {
        when(browseContext.getCustomOperationType()).thenReturn("QUERY");
        List<ObjectType> result = connection.getObjectTypes();
        Assertions.assertNotNull(result);
        assertEquals(objectListQuery.size(), result.size());
        for (int i = 0; i < objectListQuery.size(); i++) {
            assertEquals(objectListQuery.get(i).getId(), result.get(i).getId());
            assertEquals(objectListQuery.get(i).getLabel(), result.get(i).getLabel());
        }
    }

    /**
     * Tests the getObjectTypes() method for retrieve operation.
     */
    @Test
    void testGetObjectTypesRetrieve() {
        when(browseContext.getCustomOperationType()).thenReturn("GET");
        List<ObjectType> result = connection.getObjectTypes();
        Assertions.assertNotNull(result);
        assertEquals(objectListRetrieve.size(), result.size());
        for (int i = 0; i < objectListRetrieve.size(); i++) {
            assertEquals(objectListRetrieve.get(i).getId(), result.get(i).getId());
            assertEquals(objectListRetrieve.get(i).getLabel(), result.get(i).getLabel());
        }
    }
}
