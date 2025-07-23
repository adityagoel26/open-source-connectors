// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.testutilopensource.TestUtilExtended;
import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.util.TestUtil;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryProfileFactoryTest {

    private static final String PARENT_OBJECT_METADATA_RESPONSE =
            "src/test/java/resources/customProfiles/parentObjectMetadataResponse.json";
    private static final String CHILD_OBJECT_METADATA_RESPONSE =
            "src/test/java/resources/customProfiles/childObjectMetadataResponse.json";
    private static final String EXPECTED_QUERY_WITH_JOINS_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedQueryWithJoinsProfile.json";
    private static final String EXPECTED_QUERY_WITHOUT_JOINS_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedQueryWithoutJoinsProfile.json";
    private static final String EXPECTED_QUERY_RELATIONSHIPS_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedQueryRelationshipsProfile.json";

    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    @Test
    void generateQueryProfileWithJoins() {
        BrowseContext context = mock(BrowseContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getOperationProperties().getBooleanProperty("INCLUDE_SYSTEM_FIELDS")).thenReturn(false);
        when(context.getOperationProperties().getBooleanProperty("REQUIRED_FIELDS_ONLY")).thenReturn(false);

        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        JSONObject parentResponse = new JSONObject(TestUtilExtended.readFile(PARENT_OBJECT_METADATA_RESPONSE));
        JSONObject childResponse = new JSONObject(TestUtilExtended.readFile(CHILD_OBJECT_METADATA_RESPONSE));

        when(metadataRetriever.getObjectMetadata("browse_integration_test_parent_object__c")).thenReturn(
                parentResponse);
        when(metadataRetriever.getObjectMetadata("browse_integration_test_child_object__c")).thenReturn(childResponse);

        ArrayList<FieldSpecField> queryFields = new ArrayList<>();
        String profile = new QueryProfileFactory(queryFields, 1, context, metadataRetriever).getJsonProfile(
                "browse_integration_test_parent_object__c");

        assertEquals(20, queryFields.size());
        assertEquals(TestUtilExtended.readFile(EXPECTED_QUERY_WITH_JOINS_PROFILE_FILE), profile.toString());
    }

    @Test
    void generateQueryProfileWithoutJoins() {
        BrowseContext context = mock(BrowseContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getOperationProperties().getBooleanProperty("INCLUDE_SYSTEM_FIELDS")).thenReturn(false);
        when(context.getOperationProperties().getBooleanProperty("REQUIRED_FIELDS_ONLY")).thenReturn(false);

        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        JSONObject parentResponse = new JSONObject(TestUtilExtended.readFile(PARENT_OBJECT_METADATA_RESPONSE));

        when(metadataRetriever.getObjectMetadata("browse_integration_test_parent_object__c")).thenReturn(
                parentResponse);

        ArrayList<FieldSpecField> queryFields = new ArrayList<>();
        String profile = new QueryProfileFactory(queryFields, 0, context, metadataRetriever).getJsonProfile(
                "browse_integration_test_parent_object__c");

        assertEquals(9, queryFields.size());
        assertEquals(TestUtilExtended.readFile(EXPECTED_QUERY_WITHOUT_JOINS_PROFILE_FILE), profile.toString());
    }

    @Test
    void generateQueryProfileForRelationships() {
        BrowseContext context = mock(BrowseContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getOperationProperties().getBooleanProperty("INCLUDE_SYSTEM_FIELDS")).thenReturn(false);
        when(context.getOperationProperties().getBooleanProperty("REQUIRED_FIELDS_ONLY")).thenReturn(false);

        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        ArrayList<FieldSpecField> queryFields = new ArrayList<>();
        String profile = new QueryProfileFactory(queryFields, 0, context, metadataRetriever).getJsonProfile(
                "relationships");

        assertEquals(10, queryFields.size());
        assertEquals(TestUtilExtended.readFile(EXPECTED_QUERY_RELATIONSHIPS_PROFILE_FILE), profile.toString());
    }
}
