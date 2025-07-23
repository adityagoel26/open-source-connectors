// Copyright (c) 2025 Boomi, Inc

package com.boomi.connector.veeva.operation;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.Sort;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutilopensource.MockOperationContext;
import com.boomi.connector.veeva.VeevaConnector;
import com.boomi.connector.veeva.browser.VeevaBrowser;
import com.boomi.util.Base64Util;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Create/Get/Delete in that order
 */

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "JUNIT_USERNAME", matches = ".+")
public class VeevaOperationTestIT {

    public static final String MAXDOCUMENTS = "MAXDOCUMENTS";
    public static final String PAGESIZE = "PAGESIZE";
    private static final Logger LOG = LogUtil.getLogger(VeevaOperationTestIT.class);
    private static final String MANUFACTURER_NAME_V_VEEVA_LABS = "\"manufacturer_name__v\": \"Veeva Labs\",";
    private static final String FILE_NULL = "\"file\": null,";
    private static final String CLASSIFICATION_V = "\"classification__v\": \"\",";
    private static final String MAJOR_VERSION_NUMBER_V_0 = "\"major_version_number__v\": \"0\",";
    private static final String MINOR_VERSION_NUMBER_V_1 = "\"minor_version_number__v\": \"1\",";
    private static final String NAME_V = "name__v";
    private static final String NAME_V_COLON = "\"" + NAME_V + "\":";
    private static final String SLASH_NAME_V = "/" + NAME_V;
    private static final Map<String, Object> _connProps = new HashMap<>();
    private static final long MAX_DOCS_28 = 28L;
    private static final long PAGE_SIZE_2 = 2L;
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static VeevaConnector CONNECTOR;
    private static ConnectorTester CONNECTOR_TESTER;

    @BeforeAll
    public static void beforeAll() {
        _connProps.put("authenticationType", "USER_CREDENTIALS");
        _connProps.put("vaultSubdomain", "vvtechpartner-boomi-clinical01.veevavault.com");
        _connProps.put("apiVersion", "v21.1");
        _connProps.put("username", System.getenv("JUNIT_USERNAME"));
        _connProps.put("password", System.getenv("JUNIT_PASSWORD"));
        CONNECTOR = new VeevaConnector();
        CONNECTOR_TESTER = new ConnectorTester(CONNECTOR);
    }

    private static void setOperationContext(String objectTypeId, String customOpType) {
        setOperationContext(objectTypeId, customOpType, new HashMap<>());
    }

    private static void setOperationContext(String objectTypeId, String customOpType, Map<String, Object> opProps) {
        MockOperationContext context = new MockOperationContext(CONNECTOR, OperationType.EXECUTE, _connProps, opProps,
                objectTypeId, null, null);
        context.setCustomOperationType(customOpType);
        CONNECTOR_TESTER.setOperationContext(context);
    }

    private static List<SimpleOperationResult> getSimpleOperationResults(String payload) {
        List<InputStream> inputs = new ArrayList<>();
        inputs.add(new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
        return CONNECTOR_TESTER.executeExecuteOperation(inputs);
    }

    private static void addSort(QueryFilter qf) {
        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);
    }

    // testGetFilesAtFolder will fail until we mock an operation_cookie and get the path parameter substitution to work

    private static List<SimpleOperationResult> getQueryOperationResults(String objectTypeId,
            Map<String, Object> opProps, QueryFilter qf, List<String> selectedFields) {
        MockOperationContext context = new MockOperationContext(CONNECTOR, OperationType.QUERY, _connProps, opProps,
                objectTypeId, null, selectedFields);
        CONNECTOR_TESTER.setOperationContext(context);

        return CONNECTOR_TESTER.executeQueryOperation(qf);
    }

    @Test
    void testCreateVObjectOperationIT() {
        setOperationContext("product__v", "POST");
        String payload =
                "[" + System.lineSeparator() + "{" + System.lineSeparator() + "    " + NAME_V_COLON + " \"Cholecap2\","
                        + System.lineSeparator() + "    \"generic_name__vs\": \"cholepriol phosphate\","
                        + System.lineSeparator() + "    \"product_family__vs\": \"cholepriol__c\","
                        + System.lineSeparator() + "\"abbreviation__vs\": \"CHO\"," + System.lineSeparator()
                        + "    \"external_id__v\": \"CHO-11101\"," + System.lineSeparator()
                        + "    \"therapeutic_area__vs\": \"cardiology__vs\"," + System.lineSeparator() + "    "
                        + MANUFACTURER_NAME_V_VEEVA_LABS + System.lineSeparator()
                        + "    \"regions__c\": \"north_america__c\"" + System.lineSeparator() + "},"
                        + System.lineSeparator() + "{" + System.lineSeparator() + "    " + NAME_V_COLON + " \"Nyaxa2\","
                        + System.lineSeparator() + "    \"generic_name__vs\": \"nitroprinaline oxalate\","
                        + System.lineSeparator() + "    \"product_family__vs\": \"nitroprinaline__c\","
                        + System.lineSeparator() + "\"abbreviation__vs\": \"NYA\"," + System.lineSeparator()
                        + "    \"external_id__v\": \"NYA-22201\"," + System.lineSeparator()
                        + "    \"therapeutic_area__vs\": \"veterinary__c\"," + System.lineSeparator() + "    "
                        + MANUFACTURER_NAME_V_VEEVA_LABS + System.lineSeparator() + "    \"regions__c\": \"europe__c\""
                        + System.lineSeparator() + "}," + System.lineSeparator() + "{" + System.lineSeparator() + "    "
                        + NAME_V_COLON + " \"VeevaProm2\"," + System.lineSeparator()
                        + "    \"generic_name__vs\": \"veniladrine sulfate\"," + System.lineSeparator()
                        + "    \"product_family__vs\": \"veniladrine__c,vendolepene__c\"," + System.lineSeparator()
                        + "\"abbreviation__vs\": \"VPR\"," + System.lineSeparator()
                        + "    \"external_id__v\": \"VPR-33301\"," + System.lineSeparator()
                        + "    \"therapeutic_area__vs\": \"psychiatry__vs\"," + System.lineSeparator() + "    "
                        + MANUFACTURER_NAME_V_VEEVA_LABS + System.lineSeparator()
                        + "    \"regions__c\": \"asia_pacific__c,australasia__c\"" + System.lineSeparator() + "}"
                        + System.lineSeparator() + "]";

        List<SimpleOperationResult> actual = getSimpleOperationResults(payload);

        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200", actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testCreateDocumentOperationIT() {
        setOperationContext("POST::/objects/documents/batch", "OPEN_API");
        String payload =
                "[" + System.lineSeparator() + "{" + System.lineSeparator() + FILE_NULL + System.lineSeparator()
                        + NAME_V_COLON + " \"Cholecap FAQ\"," + System.lineSeparator()
                        + "\"external_id__v\": \"CHO-100111\"," + System.lineSeparator() + "\"type__v\": \"Resource2\","
                        + System.lineSeparator() + "\"subtype__v\": \"\"," + System.lineSeparator() + CLASSIFICATION_V
                        + System.lineSeparator() + "\"lifecycle__v\": \"General Lifecycle\"," + System.lineSeparator()
                        + "\"major_version_number__v\": \"1\"," + System.lineSeparator()
                        + "\"minor_version_number__v\": \"0\"," + System.lineSeparator() + "\"product__v\": \"00P1110\""
                        + System.lineSeparator() + "}," + System.lineSeparator() + "{" + System.lineSeparator()
                        + FILE_NULL + System.lineSeparator() + NAME_V_COLON + " \"Nyaxa Poster2\","
                        + System.lineSeparator() + "\"external_id__v\": \"NYA-100222\"," + System.lineSeparator()
                        + "\"type__v\": \"Promotional Piece\"," + System.lineSeparator()
                        + "\"subtype__v\": \"Convention Item\"," + System.lineSeparator() + CLASSIFICATION_V
                        + System.lineSeparator() + "\"lifecycle__v\": \"Promotional Piece\"," + System.lineSeparator()
                        + MAJOR_VERSION_NUMBER_V_0 + System.lineSeparator() + MINOR_VERSION_NUMBER_V_1
                        + System.lineSeparator() + "\"product__v\": \"00P2220\"" + System.lineSeparator() + "},"
                        + System.lineSeparator() + "{" + System.lineSeparator() + FILE_NULL + System.lineSeparator()
                        + NAME_V_COLON + " \"Gludacta Ad\"," + System.lineSeparator()
                        + "\"external_id__v\": \"GLU-100333\"," + System.lineSeparator()
                        + "\"type__v\": \"Promotional Piece\"," + System.lineSeparator()
                        + "\"subtype__v\": \"Advertisement\"," + System.lineSeparator()
                        + "\"classification__v\": \"Web\"," + System.lineSeparator()
                        + "\"lifecycle__v\": \"Promotional Piece\"," + System.lineSeparator() + MAJOR_VERSION_NUMBER_V_0
                        + System.lineSeparator() + MINOR_VERSION_NUMBER_V_1 + System.lineSeparator()
                        + "\"product__v\": \"00P3330\"" + System.lineSeparator() + "}," + System.lineSeparator() + "{"
                        + System.lineSeparator() + FILE_NULL + System.lineSeparator() + NAME_V_COLON
                        + " \"VeevaProm Info\"," + System.lineSeparator() + "\"external_id__v\": \"VPR-100444\","
                        + System.lineSeparator() + "\"type__v\": \"Reference Document\"," + System.lineSeparator()
                        + "\"subtype__v\": \"Data on File\"," + System.lineSeparator() + CLASSIFICATION_V
                        + System.lineSeparator() + "\"lifecycle__v\": \"Reference Documents\"," + System.lineSeparator()
                        + MAJOR_VERSION_NUMBER_V_0 + System.lineSeparator() + MINOR_VERSION_NUMBER_V_1
                        + System.lineSeparator() + "\"product__v\": \"00P4440\"" + System.lineSeparator() + "}"
                        + System.lineSeparator() + "]";

        List<SimpleOperationResult> actual = getSimpleOperationResults(payload);

        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200", actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testGetFilesAtRootIT() {
        String objectTypeId = "GET::/services/file_staging/items";
        setOperationContext(objectTypeId, "OPEN_API");

        List<SimpleOperationResult> actual = getSimpleOperationResults("");

        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200", actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testCreateFileIT() {
        String objectTypeId = "POST::/services/file_staging/items";
        setOperationContext(objectTypeId, "OPEN_API");

        Map<String, String> dynamicProps = new HashMap<>();
        dynamicProps.put("path_item", "u9289999");

        List<SimpleTrackedData> trackedInputs = new ArrayList<>();
        SimpleTrackedData trackedData = new SimpleTrackedData(0,
                new ByteArrayInputStream("xxxxxxxx".getBytes(StandardCharsets.UTF_8)), null, dynamicProps);
        trackedInputs.add(trackedData);

        List<SimpleOperationResult> actual = CONNECTOR_TESTER.executeExecuteOperationWithTrackedData(trackedInputs);

        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200", actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testUpdateNonexistentFolderIT() {
        String objectTypeId = "PUT::/services/file_staging/items/{item}";
        String pathParamEncodedId = Base64Util.encodeToUrlSafeString("path_item");
        String cookieStr = "{\"queryParameters\":[],\"requestBodyRequired\":true,\"rootSchemaType\":\"object\","
                + "\"requestContentType\":\"application/json\",\"headerParameters\":[],"
                + "\"pathParameters\":[{\"dataType\":\"STRING\",\"id\":\"" + pathParamEncodedId
                + "\",\"style\":\"SIMPLE\"," + "\"explode\":false,\"parameterType\":\"PATH\"}]}";
        Map<ObjectDefinitionRole, String> cookies = Collections.singletonMap(ObjectDefinitionRole.INPUT, cookieStr);
        String payloadStr = "{\n" + " \"parent\": \"/u9289999\",\n" + "  \"name\": \"FolderUpdated\"\n" + "}";

        MockOperationContext context = new MockOperationContext(CONNECTOR, OperationType.EXECUTE, _connProps,
                new HashMap<>(), objectTypeId, cookies, null);
        context.setCustomOperationType("OPEN_API");
        CONNECTOR_TESTER.setOperationContext(context);

        Map<String, Object> dynamicOpProps = new HashMap<>();
        dynamicOpProps.put(pathParamEncodedId, "u9289999/Folder");

        List<SimpleTrackedData> trackedInputs = new ArrayList<>();
        SimpleTrackedData trackedData = new SimpleTrackedData(0,
                new ByteArrayInputStream(payloadStr.getBytes(StandardCharsets.UTF_8)), null, null,
                new MutableDynamicPropertyMap(dynamicOpProps));
        trackedInputs.add(trackedData);

        List<SimpleOperationResult> actual = CONNECTOR_TESTER.executeExecuteOperationWithTrackedData(trackedInputs);
        String responsePayload = new String(actual.get(0).getPayloads().get(0));

        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200", actual.get(0).getStatusCode());
        assertTrue(responsePayload.contains("FAILURE") && responsePayload.contains("MALFORMED_URL"));
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testDeleteNonexistentFolderIT() {
        String objectTypeId = "DELETE::/services/file_staging/items/{item}";
        String pathParamEncodedId = Base64Util.encodeToUrlSafeString("path_item");
        String cookieStr = "{\"rootSchemaType\":null,\"headerParameters\":[],"
                + "\"queryParameters\":[{\"dataType\":\"BOOLEAN\",\"id\":\"cXVlcnlfcmVjdXJzaXZl\","
                + "\"parameterType\":\"QUERY\",\"explode\":false,\"style\":\"FORM\"}],"
                + "\"pathParameters\":[{\"dataType\":\"STRING\",\"id\":\"" + pathParamEncodedId
                + "\",\"parameterType\":\"PATH\","
                + "\"explode\":false,\"style\":\"SIMPLE\"}],\"requestBodyRequired\":false,"
                + "\"requestContentType\":null}";
        Map<ObjectDefinitionRole, String> cookies = Collections.singletonMap(ObjectDefinitionRole.INPUT, cookieStr);

        MockOperationContext context = new MockOperationContext(CONNECTOR, OperationType.EXECUTE, _connProps,
                new HashMap<>(), objectTypeId, cookies, null);
        context.setCustomOperationType("OPEN_API");
        CONNECTOR_TESTER.setOperationContext(context);

        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(pathParamEncodedId, "u9289999/Folder");

        List<SimpleTrackedData> trackedInputs = new ArrayList<>();
        SimpleTrackedData trackedData = new SimpleTrackedData(0, null, null, null, dynamicOpProps);
        trackedInputs.add(trackedData);

        List<SimpleOperationResult> actual = CONNECTOR_TESTER.executeExecuteOperationWithTrackedData(trackedInputs);
        String responsePayload = new String(actual.get(0).getPayloads().get(0));

        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200", actual.get(0).getStatusCode());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
        assertTrue(responsePayload.contains("FAILURE") && responsePayload.contains("MALFORMED_URL"));
    }

    @Test
    void testQueryOperationIT() {
        String objectTypeId = "documents";

        Map<String, Object> opProps = new HashMap<>();
        opProps.put(MAXDOCUMENTS, MAX_DOCS_28);
        opProps.put(PAGESIZE, PAGE_SIZE_2);
        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0")

                , new QuerySimpleBuilder(NAME_V, "LIKE", "Tr%"))).toFilter();

        addSort(qf);

        List<String> selectedFields = new ArrayList<>();
        selectedFields.add("id");
        selectedFields.add(NAME_V);

        List<SimpleOperationResult> actual = getQueryOperationResults(objectTypeId, opProps, qf, selectedFields);
        assertEquals("200", actual.get(0).getStatusCode());
        assertFalse(actual.get(0).getPayloads().isEmpty());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testQueryGetItemsAtPathListOperationIT() {
        String objectTypeId = "get_items_at_path_list";

        Map<String, Object> opProps = new HashMap<>();
        opProps.put("PATH", "");
        opProps.put("RECURSIVE", "True");

        QueryFilter qf = new QueryFilterBuilder().toFilter();

        addSort(qf);

        List<String> selectedFields = new ArrayList<>();

        List<SimpleOperationResult> actual = getQueryOperationResults(objectTypeId, opProps, qf, selectedFields);
        assertEquals("200", actual.get(0).getStatusCode());
        assertFalse(actual.get(0).getPayloads().isEmpty());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testQueryRelationshipsOperationIT() {
        String objectTypeId = "relationships";

        Map<String, Object> opProps = new HashMap<>();
        opProps.put(MAXDOCUMENTS, MAX_DOCS_28);
        opProps.put(PAGESIZE, PAGE_SIZE_2);
        QueryFilter qf = new QueryFilterBuilder(
                QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"))).toFilter();

        addSort(qf);

        List<String> selectedFields = new ArrayList<>();
        selectedFields.add("id");
        selectedFields.add("source_doc_id__v");
        selectedFields.add("source_major_version__v");
        selectedFields.add("source_minor_version__v");
        selectedFields.add("target_doc_id__v");
        selectedFields.add("target_major_version__v");
        selectedFields.add("target_minor_version__v");
        selectedFields.add("relationship_type__v");
        selectedFields.add("created_date__v");
        selectedFields.add("created_by__v");

        List<SimpleOperationResult> actual = getQueryOperationResults(objectTypeId, opProps, qf, selectedFields);

        assertEquals("200", actual.get(0).getStatusCode());
        assertFalse(actual.get(0).getPayloads().isEmpty());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    @Test
    void testQueryOperationFailureIT() {
        String objectTypeId = "documents";

        Map<String, Object> opProps = new HashMap<>();
        opProps.put(MAXDOCUMENTS, MAX_DOCS_28);
        opProps.put(PAGESIZE, PAGE_SIZE_2);

        List<String> selectedFields = new ArrayList<>();
        selectedFields.add("id");
        selectedFields.add(NAME_V);
        selectedFields.add("link__sys");
        selectedFields.add("link__sys");
        selectedFields.add("parent1" + VeevaBrowser.SUBQUERY_SUFFIX + SLASH_NAME_V);
        selectedFields.add("parent/child");
        selectedFields.add("parent2" + VeevaBrowser.SUBQUERY_SUFFIX + "/id");
        selectedFields.add("parent2" + VeevaBrowser.SUBQUERY_SUFFIX + SLASH_NAME_V);
        selectedFields.add("parent3" + VeevaBrowser.SUBQUERY_SUFFIX + "/id");
        selectedFields.add("parent3" + VeevaBrowser.SUBQUERY_SUFFIX + SLASH_NAME_V);

        List<SimpleOperationResult> actual = getQueryOperationResults(objectTypeId, opProps, null, selectedFields);

        LOG.info(actual.toString());
        assertEquals("ERROR", actual.get(0).getStatusCode());
        assertEquals("Verify VQL parameters and privileges for all selected fields. View response for details.",
                actual.get(0).getMessage());
        assertContentTypeHeaderAddedToTrackedGroupProperties(actual);
    }

    private void assertContentTypeHeaderAddedToTrackedGroupProperties(List<SimpleOperationResult> results) {
        results.forEach(result -> {
            SimplePayloadMetadata metadata = CollectionUtil.getFirst(result.getPayloadMetadatas());
            Map<String, String> responseHeaders = metadata.getTrackedGroups().get("response");
            assertNotNull(responseHeaders, "the tracked group property 'response' should not be null");
            assertTrue(StringUtil.isNotBlank(responseHeaders.get(CONTENT_TYPE_HEADER)),
                    "there should be a value available for the property " + CONTENT_TYPE_HEADER);
        });
    }
}
