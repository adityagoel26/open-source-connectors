// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import io.swagger.v3.oas.models.PathItem;
import com.boomi.connector.testutilopensource.TestUtilExtended;
import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.util.TestUtil;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchDocumentsProfileFactoryTest {

    private static final String DOCUMENTS_METADATA_RESPONSE_FILE =
            "src/test/java/resources/customProfiles/documentsMetadataResponse.json";
    private static final String EXPECTED_UPDATE_DOCUMENTS_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedUpdateDocumentsProfile.json";
    private static final String EXPECTED_CREATE_DOCUMENTS_PROFILE_FILE =
            "src/test/java/resources/customProfiles/expectedCreateDocumentsProfile.json";

    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    /**
     * When the API is {@code /objects/documents/batch}, the profile is generated using 'Documents' as Object Type ID
     */
    @Test
    void generateCreateProfileForBatchDocuments() {
        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        JSONObject parentResponse = new JSONObject(TestUtilExtended.readFile(DOCUMENTS_METADATA_RESPONSE_FILE));
        when(metadataRetriever.getDocumentsMetadata()).thenReturn(parentResponse);

        String profile = ProfileFactory.getBatchDocumentsFactory(PathItem.HttpMethod.POST, metadataRetriever)
                .getJsonProfile("documents");

        assertEquals(TestUtilExtended.readFile(EXPECTED_CREATE_DOCUMENTS_PROFILE_FILE), profile.toString());
    }

    /**
     * When the API is {@code /objects/documents/batch}, the profile is generated using 'Documents' as Object Type ID
     */
    @Test
    void generateUpdateProfileForBatchDocuments() {
        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class, RETURNS_DEEP_STUBS);

        JSONObject parentResponse = new JSONObject(TestUtilExtended.readFile(DOCUMENTS_METADATA_RESPONSE_FILE));
        when(metadataRetriever.getDocumentsMetadata()).thenReturn(parentResponse);

        String profile = ProfileFactory.getBatchDocumentsFactory(PathItem.HttpMethod.PUT, metadataRetriever)
                .getJsonProfile("documents");
        assertEquals(TestUtilExtended.readFile(EXPECTED_UPDATE_DOCUMENTS_PROFILE_FILE), profile.toString());
    }
}
