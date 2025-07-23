// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.browser;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.hubspotcrm.browser.profile.ProfileFactory;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;

import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.hubspotcrm.HubspotcrmConnection;
import com.boomi.connector.hubspotcrm.HubspotcrmConnector;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HttpClientFactory;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.util.LogUtil;
import com.boomi.util.TestUtil;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * This class contains tests for the HubspotcrmBrowser functionality.
 * It sets up the necessary mocks and configurations for testing the browse operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HubspotcrmBrowserTest {

    private static HubspotcrmConnection connection;
    private static SimpleBrowseContext context;
    ProfileFactory profileFactory;

    private static final String OTS =
            "{\n" + "    \"results\": [\n" + "        {\n" + "            \"name\": \"field-name\",\n"
                    + "            \"type\": \"string\"\n" + "        }\n" + "    ]\n" + "}";
    private static PropertyMap _connectionProperties;
    private static ConnectorTester CONNECTOR_TESTER;

    private static HubspotcrmConnector CONNECTOR;
    /**
     * Sets up the necessary mocks and configurations before each test.
     */
    @BeforeEach
    public void setup() throws IOException {
        CONNECTOR = new HubspotcrmConnector();
        connection = mock(HubspotcrmConnection.class);
        context = mock(SimpleBrowseContext.class, RETURNS_DEEP_STUBS);
        CONNECTOR_TESTER = new ConnectorTester(CONNECTOR);
        profileFactory = Mockito.mock(ProfileFactory.class);

        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("Authorization", "Bearer someToken");
        customHeaders.put("Content-Type", "application/json");
        _connectionProperties = new MutablePropertyMap();
        _connectionProperties.put("authenticationType", "OAuth 2.0");
        _connectionProperties.put("Server", "https://api.hubapi.com");
        _connectionProperties.put("url", "https://api.hubapi.com");
        _connectionProperties.put("Grant Type", "Authorization Code");
        _connectionProperties.put("scopes",
                "crm.objects.companies.read crm.objects.companies.write crm.objects.contacts.read crm.objects"
                        + ".contacts.write crm.objects.deals.read crm.objects.deals.write");
        _connectionProperties.put("spec", "open.yaml");
        _connectionProperties.put("customHeaders", customHeaders);
        _connectionProperties.put("X-API-Status", "SUCCESS");

        OAuth2Context mockOAuth2Context = mock(OAuth2Context.class);
        when(mockOAuth2Context.getOAuth2Token(true)).thenReturn(mock(OAuth2Token.class));

        _connectionProperties.put("oauthContext", mockOAuth2Context);
        TestUtil.disableBoomiLog();
    }

    /**
     * Tests the positive scenario for the connection test.
     */

    @Test
    void testConnectionPositive() throws IOException {
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(context.getOperationType()).thenReturn(OperationType.GET);
        when(mockResponse.getStatusLine().getStatusCode()).thenReturn(HttpStatus.SC_OK);
        when(connection.getContext()).thenReturn(context);
        when(connection.testConnection(any(CloseableHttpClient.class), anyString())).thenReturn(mockResponse);
        new HubspotcrmBrowser(connection).testConnection();
        verify(mockResponse, times(1)).close();
    }

    /**
     * Tests the negative scenario for the connection test.
     */
    @Test
    void testConnectionNegative() throws IOException {
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        StatusLine mockStatusLine = mock(StatusLine.class);
        HttpEntity mockEntity = mock(HttpEntity.class);
        when(context.getOperationType()).thenReturn(OperationType.GET);

        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

        when(mockResponse.getEntity()).thenReturn(mockEntity);
        String errorMessage = "{\"error\": \"IOException occurred while test connection.\"}";
        when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(errorMessage.getBytes()));

        when(connection.getContext()).thenReturn(context);
        when(connection.testConnection(any(CloseableHttpClient.class), anyString())).thenReturn(mockResponse);

        Exception exception = Assert.assertThrows(Exception.class, () -> {
            new HubspotcrmBrowser(connection).testConnection();
        });
        Assertions.assertNotNull(exception.getMessage());
        verify(mockResponse, times(1)).close();
    }

    /**
     * Tests the getObjectDefinitions method for the CREATE operation Request Profile with mocked HTTP responses.
     *
     * @throws IOException if an I/O error occurs while reading the expected schema file
     */
    @Test
    void testGetObjectDefinitionsCreateInput() throws IOException {
        String objectTypeId = "POST::/crm/v3/objects/contacts";
        Collection<ObjectDefinitionRole> roles = new ArrayList<>();
        roles.add(ObjectDefinitionRole.OUTPUT);

        SimpleBrowseContext context_n = new SimpleBrowseContext(null, CONNECTOR, OperationType.CREATE, "POST",
                _connectionProperties, new MutablePropertyMap());

        HubspotcrmBrowser hubspotcrmBrowser = (HubspotcrmBrowser) CONNECTOR.createBrowser(context_n);
        ObjectDefinitions result = hubspotcrmBrowser.getObjectDefinitions(objectTypeId, roles);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getDefinitions().size());
        String expectedSchema = new String(
                Files.readAllBytes(Paths.get("src/test/resources/expectedSchema_contactResponse.json")));
        Assertions.assertEquals(expectedSchema, result.getDefinitions().get(0).getJsonSchema());
    }

    @Test
    void testGetObjectDefinitionsUpdateInput() throws IOException {
        String objectTypeId = "PATCH::/crm/v3/objects/contacts/{contactId}";
        Collection<ObjectDefinitionRole> roles = new ArrayList<>();
        roles.add(ObjectDefinitionRole.OUTPUT);

        SimpleBrowseContext context_n = new SimpleBrowseContext(null, CONNECTOR, OperationType.UPDATE, "PATCH",
                _connectionProperties, new MutablePropertyMap());

        HubspotcrmBrowser hubspotcrmBrowser = (HubspotcrmBrowser) CONNECTOR.createBrowser(context_n);
        ObjectDefinitions result = hubspotcrmBrowser.getObjectDefinitions(objectTypeId, roles);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.getDefinitions().size());
        String expectedSchema = new String(
                Files.readAllBytes(Paths.get("src/test/resources/expectedSchema_contactResponse.json")));
        Assertions.assertEquals(expectedSchema, result.getDefinitions().get(0).getJsonSchema());
    }

    /**
     * Tests the getObjectDefinitions method for the CREATE operation Response Profile.
     *
     * @throws IOException if an I/O error occurs while reading the expected schema file
     */
    @Test
    void testGetObjectDefinitionsCreateOutput() throws IOException {

        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
        HttpEntity entity = mock(HttpEntity.class);


        when(connection.getContext()).thenReturn(context);

        try (InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8));
                MockedStatic<HttpClientFactory> factory = Mockito.mockStatic(HttpClientFactory.class);
                MockedStatic<ExecutionUtils> executionUtils = Mockito.mockStatic(ExecutionUtils.class);
             MockedStatic<LogUtil> mockedLogUtil = Mockito.mockStatic(LogUtil.class)) {
            Logger mockLogger = mock(Logger.class);
            mockedLogUtil.when(() -> LogUtil.getLogger(HubspotcrmObjectMetadataRetriever.class))
                    .thenReturn(mockLogger);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(content);

            when(context.getCustomOperationType()).thenReturn("POST");
            factory.when(HttpClientFactory::createHttpClient).thenReturn(mockClient);
            executionUtils.when(
                    () -> ExecutionUtils.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
                    response);

            executionUtils.when(() -> ExecutionUtils.getRootNode(HubspotcrmOperationType.CREATE, "contact")).thenReturn(
                    "contact");

            ObjectDefinitions inputDefinitions = new HubspotcrmBrowser(connection).getObjectDefinitions("contact",
                    Collections.singleton(ObjectDefinitionRole.INPUT));

            ObjectDefinition inputDefinition = inputDefinitions.getDefinitions().get(0);

            Assertions.assertEquals("/contact", inputDefinition.getElementName());
            Assertions.assertEquals(ContentType.JSON, inputDefinition.getInputType());

            String expectedJsonSchema = new String(Files.readAllBytes(Paths.get(
                    Objects.requireNonNull(getClass().getClassLoader().getResource("expectedSchema_contact.json"))
                            .toURI())), StandardCharsets.UTF_8);

            Assertions.assertEquals(expectedJsonSchema.trim(), inputDefinition.getJsonSchema());

            verify(response, times(1)).close();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Tests the retrieval of object definitions for Archive operations
     * Verifies that the object definitions are correctly retrieved for the DELETE
     * endpoint "/crm/v3/objects/contacts/{contactId}".
     */
    @Test
    void testGetObjectDefinitionsForArchive() {
        Collection<ObjectDefinitionRole> roles = new ArrayList<>();
        roles.add(ObjectDefinitionRole.OUTPUT);
        testGetObjectDefinitions("DELETE", "DELETE::/crm/v3/objects/contacts/{contactId}", roles,
                HubspotcrmOperationType.DELETE);
    }

    /**
     * Tests the retrieval of object definitions for Retrieve operation.
     * Verifies that the object definitions are correctly retrieved for the GET
     * endpoint "/crm/v3/objects/contacts/{contactId}".
     */
    @Test
    void testGetObjectDefinitionsForRetrieve() {
        Collection<ObjectDefinitionRole> roles = new ArrayList<>();
        roles.add(ObjectDefinitionRole.OUTPUT);
        testGetObjectDefinitions("GET", "GET::/crm/v3/objects/contacts/{contactId}", roles,
                HubspotcrmOperationType.GET);
    }

    /**
     * Tests the retrieval of object definitions for Search operation.
     * Verifies that the object definitions are correctly retrieved for the Search
     * endpoint "/crm/v3/objects/contacts/search".
     */
    @Test
    void testGetObjectDefinitionsForQuery() {
        Collection<ObjectDefinitionRole> roles = new ArrayList<>();
        roles.add(ObjectDefinitionRole.OUTPUT);
        testGetObjectDefinitions("QUERY", "POST::/crm/v3/objects/contacts/search", roles,
                HubspotcrmOperationType.QUERY);
    }

    /**
     * Helper method to test the retrieval of object definitions for different operation types.
     * Creates a browse context, initializes the HubspotCRM browser, and verifies that:
     * 1. The object definitions are not null
     * 2. Exactly one definition is returned
     *
     * @param operationType The type of operation
     * @param objectPath    The full path of the endpoint including the operation type
     */
    void testGetObjectDefinitions(String operationType, String objectPath, Collection<ObjectDefinitionRole> roles,
            HubspotcrmOperationType hubspotcrmOperationType) {
        HttpEntity entity = mock(HttpEntity.class);
        OperationType opType = OperationType.valueOf(operationType);
        context = new SimpleBrowseContext(null, CONNECTOR, opType, operationType, _connectionProperties,
                new MutablePropertyMap());
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        HubspotcrmBrowser hubspotcrmBrowser = (HubspotcrmBrowser) CONNECTOR.createBrowser(context);
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class);
                InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8))) {
            Mockito.when(response.getEntity()).thenReturn(entity);
            Mockito.when(entity.getContent()).thenReturn(content);
            mockedStatic.when(
                    () -> ExecutionUtils.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
                    response);
            Mockito.when(ExecutionUtils.getRootNode(hubspotcrmOperationType, objectPath)).thenReturn("Contacts");
            Mockito.when(profileFactory.getJsonProfile("contacts", hubspotcrmOperationType, "Contacts")).thenReturn(
                    "test profile");

            ObjectDefinitions objectDefinitions = hubspotcrmBrowser.getObjectDefinitions(objectPath, roles);

            Assertions.assertNotNull(objectDefinitions);
            Assertions.assertEquals(1, objectDefinitions.getDefinitions().size());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

