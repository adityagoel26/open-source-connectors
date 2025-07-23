// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutilopensource.TestUtilExtended;
import com.boomi.connector.veeva.VeevaConnector;
import com.boomi.util.TestUtil;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VeevaBrowseTestIT {

    private static final Map<String, Object> CONN_PROPS = new HashMap<>();
    private static final Map<String, Object> OP_PROPS = new HashMap<>();
    private static final String JOIN_DEPTH = "JOIN_DEPTH";
    private static final String REQUIRED_FIELDS_ONLY = "REQUIRED_FIELDS_ONLY";
    private static final String OBJECT_TYPE_PRODUCT = "product__v";

    //record regression data for actual vs expected
    private static final boolean CAPTURE_EXPECTED = false;

    private static VeevaConnector CONNECTOR;
    private static ConnectorTester CONNECTOR_TESTER;

    @BeforeAll
    public static void setup() {
        CONN_PROPS.put("vaultSubdomain", "vvtechpartner-boomi-clinical01.veevavault.com");
        CONN_PROPS.put("apiVersion", "v23.3");
        CONN_PROPS.put("username", System.getenv("JUNIT_USERNAME"));
        CONN_PROPS.put("password", System.getenv("JUNIT_PASSWORD"));
        CONNECTOR = new VeevaConnector();
        CONNECTOR_TESTER = new ConnectorTester(CONNECTOR);

        TestUtil.disableBoomiLog();
    }

    private static void setBrowseContext(OperationType execute, String customOpType) {
        SimpleBrowseContext context = new SimpleBrowseContext(null, CONNECTOR, execute, customOpType, CONN_PROPS,
                OP_PROPS);
        CONNECTOR_TESTER.setBrowseContext(context);
    }

    // always fails due to special characters for item: label="CTN Attached Document"
    //Maybe we aren't handling UTF-8 correctly?
    @Test
    public void testBrowseTypesQueryIT() throws IOException {
        TestUtilExtended.testBrowseTypes("testBrowseTypesQUERY", CONNECTOR, OperationType.QUERY, "", CONN_PROPS, null,
                true);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsVObjectCreateIT() throws IOException {
        OP_PROPS.put(REQUIRED_FIELDS_ONLY, true);

        setBrowseContext(OperationType.EXECUTE, "POST");
        String actual = CONNECTOR_TESTER.browseProfiles(OBJECT_TYPE_PRODUCT);

        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsVObjectCreate", CAPTURE_EXPECTED);
    }

    @Test
    public void testBrowseDefinitionsVObjectQueryWithJoinIT() throws IOException {
        OP_PROPS.put(REQUIRED_FIELDS_ONLY, true);
        OP_PROPS.put(JOIN_DEPTH, 1L);

        setBrowseContext(OperationType.QUERY, "QUERY");
        String actual = CONNECTOR_TESTER.browseProfiles("browse_integration_test_parent_object__c");

        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsVObjectQueryWithJoin", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsVObjectUpdateIT() throws IOException {
        OP_PROPS.put(REQUIRED_FIELDS_ONLY, true);

        setBrowseContext(OperationType.EXECUTE, "PUT");
        String actual = CONNECTOR_TESTER.browseProfiles(OBJECT_TYPE_PRODUCT);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsVObjectUpdate", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsVObjectQueryIT() throws IOException {
        OP_PROPS.put(JOIN_DEPTH, 0L);
        setBrowseContext(OperationType.QUERY, "");
        String actual = CONNECTOR_TESTER.browseProfiles(OBJECT_TYPE_PRODUCT);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsVObjectQUERY", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsVObjectQueryChildJoinIT() throws IOException {
        OP_PROPS.put(JOIN_DEPTH, 1L);
        setBrowseContext(OperationType.QUERY, "");
        String objectTypeId = "child__c";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsVObjectQUERYChildJOIN", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsVObjectQueryParentJoinIT() throws IOException {
        OP_PROPS.put(JOIN_DEPTH, 1L);
        setBrowseContext(OperationType.QUERY, "");
        String objectTypeId = "parent__c";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsVObjectQUERYParentJOIN", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsDocumentCreateIT() throws IOException {
        OP_PROPS.put(REQUIRED_FIELDS_ONLY, true);

        setBrowseContext(OperationType.EXECUTE, "POST");
        String objectTypeId = "/objects/documents/batch";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsDocumentCreate", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsDocumentUpdateIT() throws IOException {
        OP_PROPS.put(REQUIRED_FIELDS_ONLY, true);

        setBrowseContext(OperationType.EXECUTE, "PUT");
        String objectTypeId = "/objects/documents/batch";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsDocumentUpdate", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsDocumentQueryIT() throws IOException {
        OP_PROPS.put(JOIN_DEPTH, 0L);
        setBrowseContext(OperationType.QUERY, "");
        String objectTypeId = "documents";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsDocumentQUERY", CAPTURE_EXPECTED);
    }

    @Test
    @Disabled("this test breaks everytime Veeva applies trivial changes to the document definition")
    public void testBrowseDefinitionsDocumentQUERYJoinIT() throws IOException {
        OP_PROPS.put(JOIN_DEPTH, 1L);
        setBrowseContext(OperationType.QUERY, "");
        String objectTypeId = "documents";
        String actual = CONNECTOR_TESTER.browseProfiles(objectTypeId);
        TestUtilExtended.compareXML(actual, "testBrowseDefinitionsDocumentQUERYJoin", CAPTURE_EXPECTED);
    }
}
