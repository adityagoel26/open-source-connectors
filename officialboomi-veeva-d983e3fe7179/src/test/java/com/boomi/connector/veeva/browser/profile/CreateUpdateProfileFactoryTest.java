// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.testutilopensource.TestUtilExtended;
import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.connector.veeva.browser.VeevaOperationType;
import com.boomi.util.TestUtil;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CreateUpdateProfileFactoryTest {

    private static final String PARENT_OBJECT_METADATA_RESPONSE =
            "src/test/java/resources/customProfiles/parentObjectMetadataResponse.json";
    private static final String CHILD_OBJECT_METADATA_RESPONSE =
            "src/test/java/resources/customProfiles/childObjectMetadataResponse.json";
    private static final String EXPECTED_UPDATE_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedUpdateProfile.json";
    private static final String EXPECTED_CREATE_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedCreateProfile.json";

    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    @Test
    void generateCreateProfile() {
        BrowseContext context = mock(BrowseContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getOperationProperties().getBooleanProperty("INCLUDE_SYSTEM_FIELDS")).thenReturn(false);
        when(context.getOperationProperties().getBooleanProperty("REQUIRED_FIELDS_ONLY")).thenReturn(false);

        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        JSONObject parentResponse = new JSONObject(TestUtilExtended.readFile(PARENT_OBJECT_METADATA_RESPONSE));
        JSONObject childResponse = new JSONObject(TestUtilExtended.readFile(CHILD_OBJECT_METADATA_RESPONSE));

        when(metadataRetriever.getObjectMetadata("browse_integration_test_parent_object__c")).thenReturn(
                parentResponse);
        when(metadataRetriever.getObjectMetadata("browse_integration_test_child_object__c")).thenReturn(childResponse);

        String profile = new CreateUpdateProfileFactory(VeevaOperationType.CREATE, context,
                metadataRetriever).getJsonProfile("browse_integration_test_parent_object__c");

        assertEquals(TestUtilExtended.readFile(EXPECTED_CREATE_PROFILE_FILE), profile.toString());
    }

    @Test
    void generateUpdateProfile() {
        BrowseContext context = mock(BrowseContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getOperationProperties().getBooleanProperty("INCLUDE_SYSTEM_FIELDS")).thenReturn(false);
        when(context.getOperationProperties().getBooleanProperty("REQUIRED_FIELDS_ONLY")).thenReturn(false);

        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        JSONObject parentResponse = new JSONObject(TestUtilExtended.readFile(PARENT_OBJECT_METADATA_RESPONSE));
        JSONObject childResponse = new JSONObject(TestUtilExtended.readFile(CHILD_OBJECT_METADATA_RESPONSE));

        when(metadataRetriever.getObjectMetadata("browse_integration_test_parent_object__c")).thenReturn(
                parentResponse);
        when(metadataRetriever.getObjectMetadata("browse_integration_test_child_object__c")).thenReturn(childResponse);

        String profile = new CreateUpdateProfileFactory(VeevaOperationType.UPDATE, context,
                metadataRetriever).getJsonProfile("browse_integration_test_parent_object__c");
        assertEquals(TestUtilExtended.readFile(EXPECTED_UPDATE_PROFILE_FILE), profile.toString());
    }
}
