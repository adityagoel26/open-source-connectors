// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutilopensource.TestUtilExtended;
import com.boomi.connector.veeva.VeevaConnection;
import com.boomi.connector.veeva.VeevaConnector;
import com.boomi.connector.veeva.util.ExecutionUtils;
import com.boomi.connector.veeva.util.HttpClientFactory;
import com.boomi.util.CollectionUtil;
import com.boomi.util.TestUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class VeevaBrowseTest {

    private static final Map<String, Object> CONN_PROPS = new HashMap<>();
    private static final Map<String, Object> OP_PROPS = new HashMap<>();
    private static final String JOIN_DEPTH = "JOIN_DEPTH";
    private static final String SERVICES_FILE_STAGING_ITEMS = "/services/file_staging/items";
    private static final int BROWSE_PROFILES_LIMIT = 1000;

    //record regression data for actual vs expected
    private static final boolean CAPTURE_EXPECTED = false;
    private static final String AUTH_RESPONSE_SUCCESS =
            "{\"responseStatus\":\"SUCCESS\",\"sessionId\":\"sessionId\",\"userId\":11111}";

    private static final String AUTH_RESPONSE_FAILURE =
            "{\"responseStatus\":\"FAILURE\",\"sessionId\":\"sessionId\",\"userId\":11111}";

    private static final String OBJECT_TYPES = "{\"responseStatus\":\"SUCCESS\",\"objects\":[{\"url\":\"/api/v21"
                                               + ".1/metadata/vobjects/distribution_task_action__v\","
                                               + "\"label\":\"Distribution Task Action\","
                                               + "\"name\":\"distribution_task_action__v\","
                                               + "\"label_plural\":\"Distribution Task Actions\","
                                               + "\"prefix\":\"V42\","
                                               + "\"order\":181,\"in_menu\":true,\"source\":\"standard\","
                                               + "\"status\":[\"active__v\"]},{\"url\":\"/api/v21"
                                               + ".1/metadata/vobjects/site_package_definition__v\",\"label\":\"Site "
                                               + "Package Definition\","
                                               + "\"name\":\"site_package_definition__v\",\"label_plural\":\"Site "
                                               + "Package Definitions\"," + "\"prefix\":\"V41\","
                                               + "\"order\":181,\"in_menu\":true,\"source\":\"standard\","
                                               + "\"status\":[\"active__v\"]},{\"url\":\"/api/v21"
                                               + ".1/metadata/vobjects/product__v\",\"label\":\"Product\","
                                               + "\"name\":\"product__v\"," + "\"label_plural\":\"Products\","
                                               + "\"prefix\":\"00P\",\"order\":47,\"in_menu\":true,"
                                               + "\"source\":\"standard\",\"status\":[\"active__v\"]}]}";

    private static final String OTS = "{\n" + "    \"responseStatus\": \"SUCCESS\",\n" + "    \"object\": {\n"
                                      + "        \"available_lifecycles\": [],\n"
                                      + "        \"label_plural\": \"Products\",\n" + "        \"prefix\": \"00P\",\n"
                                      + "        \"data_store\": \"standard\",\n" + "        \"description\": null,\n"
                                      + "        \"enable_esignatures\": false,\n"
                                      + "        \"source\": \"standard\",\n"
                                      + "        \"allow_attachments\": false,\n" + "        \"relationships\": [],\n"
                                      + "        \"urls\": {\n" + "          \"field\": \"/api/v23"
                                      + ".3/metadata/vobjects/product__v/fields/{name}\",\n"
                                      + "          \"record\": \"/api/v23.3/vobjects/product__v/{id}\",\n"
                                      + "          \"list\": \"/api/v23.3/vobjects/product__v\",\n"
                                      + "          \"metadata\": \"/api/v23.3/metadata/vobjects/product__v\"\n"
                                      + "        },\n" + "        \"role_overrides\": false,\n"
                                      + "        \"localized_data\": {\n" + "            \"label_plural\": {\n"
                                      + "              \"de\": \"Produkte\"\n" + "            },\n"
                                      + "            \"label\": {\n" + "              \"de\": \"Produkt\"\n"
                                      + "            }\n" + "        },\n" + "        \"object_class\": \"base\",\n"
                                      + "        \"fields\": [\n" + "            {\n"
                                      + "              \"help_content\": null,\n"
                                      + "              \"editable\": false,\n"
                                      + "              \"lookup_relationship_name\": null,\n"
                                      + "              \"label\": \"ID\",\n"
                                      + "              \"source\": \"standard\",\n"
                                      + "              \"type\": \"ID\",\n"
                                      + "              \"modified_date\": \"2020-05-26T10:19:27.000Z\",\n"
                                      + "              \"created_by\": 1,\n" + "              \"required\": false,\n"
                                      + "              \"no_copy\": true,\n" + "              \"localized_data\": {\n"
                                      + "                \"label\": {\n" + "                  \"de\": \"ID\"\n"
                                      + "                }\n" + "              },\n"
                                      + "              \"name\": \"id\",\n" + "              \"list_column\": false,\n"
                                      + "              \"modified_by\": 1,\n" + "              \"facetable\": false,\n"
                                      + "              \"created_date\": \"2020-05-26T10:19:27.000Z\",\n"
                                      + "              \"lookup_source_field\": null,\n"
                                      + "              \"status\": [\n" + "                \"active__v\"\n"
                                      + "              ],\n" + "              \"order\": 0\n" + "            }\n"
                                      + "          ],\n" + "          \"status\": [\n" + "            \"active__v\"\n"
                                      + "          ],\n" + "          \"default_obj_type\": \"base__v\"\n" + "  }\n"
                                      + "}";

    private static VeevaConnector CONNECTOR;
    private static ConnectorTester CONNECTOR_TESTER;

    @BeforeAll
    public static void setup() {
        CONNECTOR = new VeevaConnector();
        CONNECTOR_TESTER = new ConnectorTester(CONNECTOR);

        CONN_PROPS.put("vaultSubdomain", "https://www.veeva.com");
        CONN_PROPS.put("apiVersion", "v23.3");
        CONN_PROPS.put("username", "the username");
        CONN_PROPS.put("password", "the password");
        CONN_PROPS.put("sessionTimeout", 10L);

        TestUtil.disableBoomiLog();
    }

    private static void setBrowseContext(OperationType execute, String customOpType) {
        SimpleBrowseContext context = new SimpleBrowseContext(null, CONNECTOR, execute, customOpType, CONN_PROPS,
                OP_PROPS);
        CONNECTOR_TESTER.setBrowseContext(context);
    }

    @Test
    public void testBrowseOpenAPITypesExecute() throws IOException {
        TestUtilExtended.testBrowseTypes("testBrowseOpenAPITypesEXECUTE", CONNECTOR, OperationType.EXECUTE, "OPEN_API",
                CONN_PROPS, null, CAPTURE_EXPECTED);
    }

    //always fails XML compare because cookie JSON randomly changes order each execution
    //Need to just test length, or break up to do a lower level JSON compare
    @Test
    public void testBrowseDefinitionsMultipart() {
        setBrowseContext(OperationType.EXECUTE, "OPEN_API");
        String actual = CONNECTOR_TESTER.browseProfiles("POST::" + SERVICES_FILE_STAGING_ITEMS);
        assert (actual.length() > BROWSE_PROFILES_LIMIT);
    }

    @Test
    public void testBrowseDefinitionsDocumentRelationshipQUERY() throws IOException {
        OP_PROPS.put(JOIN_DEPTH, 0L);
        setBrowseContext(OperationType.QUERY, "");
        String objectTypeId = "relationships";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsDocumentRelationshipQUERY", CAPTURE_EXPECTED);
    }

    //always fails XML compare because cookie JSON randomly changes order each execution
    //Need to just test length, or break up to do a lower level JSON compare
    @Test
    public void testBrowseDefinitionsOpenAPIPicklistsGet() {
        setBrowseContext(OperationType.EXECUTE, "OPEN_API");
        String objectTypeId = "GET::/objects/picklists";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        assert (actual.length() > BROWSE_PROFILES_LIMIT);
    }

    //always fails XML compare because cookie JSON randomly changes order each execution
    //Need to just test length, or break up to do a lower level JSON compare
    @Test
    public void testBrowseDefinitionsOpenAPIFilesGet() {
        setBrowseContext(OperationType.QUERY, "");
        String objectTypeId = "get_items_at_path_list";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        assert (actual.length() > BROWSE_PROFILES_LIMIT);
    }

    //always fails XML compare because cookie JSON randomly changes order each execution
    //Need to just test length, or break up to do a lower level JSON compare
    @Test
    public void testBrowseDefinitionsOpenAPIFilePost() {
        setBrowseContext(OperationType.EXECUTE, "OPEN_API");
        String actual = CONNECTOR_TESTER.browseProfiles("POST::" + SERVICES_FILE_STAGING_ITEMS);
        assert (actual.length() > BROWSE_PROFILES_LIMIT);
    }

    @Test
    void executeGetVeevaFieldMetadataTest() throws IOException {
        VeevaConnection connection = mock(VeevaConnection.class);
        BrowseContext context = mock(BrowseContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        CloseableHttpClient client = mock(CloseableHttpClient.class);

        try (InputStream content = new ByteArrayInputStream(OBJECT_TYPES.getBytes(StandardCharsets.UTF_8));
                MockedStatic<HttpClientFactory> factory = Mockito.mockStatic(HttpClientFactory.class);
                MockedStatic<ExecutionUtils> executionUtils = Mockito.mockStatic(ExecutionUtils.class)) {
            when(response.getEntity().getContent()).thenReturn(content);
            when(context.getCustomOperationType()).thenReturn("QUERY");
            when(connection.getContext()).thenReturn(context);
            factory.when(() -> HttpClientFactory.createHttpClient(context)).thenReturn(client);
            when(ExecutionUtils.execute("GET", "/metadata/vobjects", client, null, connection)).thenReturn(response);
            ObjectTypes types = new VeevaBrowser(connection).getObjectTypes();
            ObjectType type = types.getTypes().get(0);

            Assertions.assertEquals("documents", type.getId());
            Mockito.verify(response, times(1)).close();
        }
    }

    @Test
    void getObjectDefinitionsPostTest() throws IOException {
        VeevaConnection connection = mock(VeevaConnection.class);
        BrowseContext context = mock(BrowseContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        PropertyMap operationProperties = new MutablePropertyMap();

        try (InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8));
                MockedStatic<HttpClientFactory> factory = Mockito.mockStatic(HttpClientFactory.class);
                MockedStatic<ExecutionUtils> executionUtils = Mockito.mockStatic(ExecutionUtils.class)) {
            when(response.getEntity().getContent()).thenReturn(content);
            when(context.getOperationProperties()).thenReturn(operationProperties);
            when(context.getCustomOperationType()).thenReturn("POST");
            when(connection.getContext()).thenReturn(context);
            factory.when(() -> HttpClientFactory.createHttpClient(context)).thenReturn(client);
            executionUtils.when(
                            () -> ExecutionUtils.execute("GET", "/metadata/vobjects/product__v", client, null,
                                    connection))
                    .thenReturn(response);
            ObjectDefinitions definitions = new VeevaBrowser(connection).getObjectDefinitions("product__v",
                    OperationType.CREATE.getSupportedDefinitionRoles());
            ObjectDefinition definition = definitions.getDefinitions().get(0);

            Assertions.assertEquals("/product__v", definition.getElementName());
            Mockito.verify(response, times(1)).close();
        }
    }

    @Test
    void executeGetVeevaFieldMetadataFailureTest() throws IOException {
        VeevaConnection connection = mock(VeevaConnection.class);
        BrowseContext context = mock(BrowseContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpClient client = mock(CloseableHttpClient.class);

        try (MockedStatic<HttpClientFactory> factory = Mockito.mockStatic(HttpClientFactory.class);
                MockedStatic<ExecutionUtils> executionUtils = Mockito.mockStatic(ExecutionUtils.class)) {
            when(context.getCustomOperationType()).thenReturn("QUERY");
            when(connection.getContext()).thenReturn(context);
            factory.when(() -> HttpClientFactory.createHttpClient(context)).thenReturn(client);
            executionUtils.when(() -> ExecutionUtils.execute("GET", "/metadata/vobjects", client, null, connection))
                    .thenThrow(IOException.class);

            Throwable t = Assertions.assertThrows(ConnectorException.class,
                    () -> new VeevaBrowser(connection).getObjectTypes());
            Assertions.assertEquals("[Unable to browse ] Unknown failure: java.io.IOException", t.getMessage());
            Mockito.verify(client, times(1)).close();
        }
    }

    @Test
    void connectionTest() throws IOException {
        VeevaConnection connection = mock(VeevaConnection.class);
        BrowseContext context = mock(BrowseContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

        try (InputStream content = new ByteArrayInputStream(AUTH_RESPONSE_SUCCESS.getBytes(StandardCharsets.UTF_8))) {
            when(context.getCustomOperationType()).thenReturn(null);
            when(context.getOperationType()).thenReturn(OperationType.GET);
            when(response.getEntity().getContent()).thenReturn(content);
            when(connection.getContext()).thenReturn(context);
            when(connection.testConnection(ArgumentMatchers.any(CloseableHttpClient.class),
                    ArgumentMatchers.anyString())).thenReturn(response);
            new VeevaBrowser(connection).testConnection();

            Mockito.verify(response, times(1)).close();
        }
    }

    @Test
    void connectionFailureTest() throws IOException {
        VeevaConnection connection = mock(VeevaConnection.class);
        BrowseContext context = mock(BrowseContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);

        try (InputStream content = new ByteArrayInputStream(AUTH_RESPONSE_FAILURE.getBytes(StandardCharsets.UTF_8))) {
            when(context.getCustomOperationType()).thenReturn(null);
            when(response.getEntity().getContent()).thenReturn(content);
            when(context.getOperationType()).thenReturn(OperationType.GET);
            when(connection.getContext()).thenReturn(context);
            when(connection.testConnection(ArgumentMatchers.any(CloseableHttpClient.class),
                    ArgumentMatchers.anyString())).thenReturn(response);

            Throwable t = Assertions.assertThrows(ConnectorException.class,
                    () -> new VeevaBrowser(connection).testConnection());
            Assertions.assertEquals("{\"sessionId\":\"sessionId\",\"responseStatus\":\"FAILURE\",\"userId\":11111}",
                    t.getMessage());
            Mockito.verify(response, times(1)).close();
        }
    }

    @Test
    void connectionExceptionTest() throws IOException {
        VeevaConnection connection = mock(VeevaConnection.class);
        BrowseContext context = mock(BrowseContext.class, RETURNS_DEEP_STUBS);

        when(context.getCustomOperationType()).thenReturn("QUERY");
        when(connection.getContext()).thenReturn(context);
        when(connection.testConnection(ArgumentMatchers.any(CloseableHttpClient.class),
                ArgumentMatchers.anyString())).thenThrow(new IOException("IO exception"));

        Throwable t = Assertions.assertThrows(ConnectorException.class,
                () -> new VeevaBrowser(connection).testConnection());
        Assertions.assertEquals("IO exception", t.getMessage());
    }

    @Test
    void addVObjectObjectTypesTest() throws IOException {
        ObjectTypes objectTypes = new ObjectTypes();
        try (InputStream vObjectTypes = new ByteArrayInputStream(OBJECT_TYPES.getBytes(StandardCharsets.UTF_8))) {
            JSONObject jsonVObjectTypes = new JSONObject(new JSONTokener(vObjectTypes));
            VeevaBrowser.addVObjectObjectTypes(objectTypes, jsonVObjectTypes);
        }

        List<ObjectType> objectTypeList = objectTypes.getTypes();
        assertEquals(3, objectTypeList.size());

        ObjectType distributionTaskAction = objectTypes.getTypes().get(0);
        assertEquals("distribution_task_action__v", distributionTaskAction.getId());
        assertEquals("Distribution Task Action", distributionTaskAction.getLabel());
    }

    @Test
    void sortObjectTypesTest() {
        List<String> expectedObjectTypeIds = CollectionUtil.asList("documents", "relationships",
                "get_items_at_path_list", "distribution_task_action__v", "product__v", "site_package_definition__v");

        ObjectTypes objectTypes = new ObjectTypes();
        VeevaBrowser.addDocumentObjectType(objectTypes);
        VeevaBrowser.addDocumentRelationshipType(objectTypes);
        VeevaBrowser.addGetItemsAtPathType(objectTypes);
        VeevaBrowser.addVObjectObjectTypes(objectTypes, new JSONObject(OBJECT_TYPES));
        List<String> objectTypeIds = objectTypes.getTypes().stream().map(objectType -> objectType.getId()).collect(
                Collectors.toList());

        assertEquals(expectedObjectTypeIds, objectTypeIds);
    }

    @Test
    void addDocumentObjectTypeTest() {
        ObjectTypes objectTypes = new ObjectTypes();
        VeevaBrowser.addDocumentObjectType(objectTypes);
        ObjectType document = objectTypes.getTypes().get(0);
        assertEquals("documents", document.getId());
        assertEquals("Document", document.getLabel());
    }

    @Test
    void addDocumentRelationshipTypeTest() {
        ObjectTypes objectTypes = new ObjectTypes();
        VeevaBrowser.addDocumentRelationshipType(objectTypes);
        ObjectType documentRelationship = objectTypes.getTypes().get(0);
        assertEquals("relationships", documentRelationship.getId());
        assertEquals("Document Relationships", documentRelationship.getLabel());
    }
}
