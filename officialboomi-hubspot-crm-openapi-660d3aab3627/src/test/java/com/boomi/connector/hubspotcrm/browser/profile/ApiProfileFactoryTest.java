// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.browser.profile;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmObjectMetadataRetriever;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmOperationType;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiProfileFactoryTest {

    @Mock
    private HubspotcrmObjectMetadataRetriever metadataRetriever;

    @Mock
    private BrowseContext context;

    private ApiProfileFactory profileFactory;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        profileFactory = new ApiProfileFactory(HubspotcrmOperationType.CREATE, metadataRetriever);
    }

    @Test
    public void testAddFieldsToProfile() {
        String objectTypeId = "contact";
        JSONObject profile = new JSONObject();
        String path = "POST::/crm/v3/objects/contacts";
        int depth = 1;
        String lookupRelationshipName = "relationship-name";

        // Set up mock metadata to return a sample JSON array of fields
        JSONObject fieldMetadata = new JSONObject();
        fieldMetadata.put("name", "field-name");
        fieldMetadata.put("type", "string");

        JSONArray fieldsArray = new JSONArray();
        fieldsArray.put(fieldMetadata);

        JSONObject metadata = new JSONObject();
        metadata.put("results", fieldsArray);

        when(metadataRetriever.getObjectMetadata(objectTypeId)).thenReturn(metadata);

        profileFactory.addFieldsToProfile(objectTypeId, profile, path, depth, lookupRelationshipName);

        assertTrue(profile.has("field-name"));
        assertEquals("string", profile.getJSONObject("field-name").getString("type"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddFieldsToProfileWithInvalidOperationType() {
        BrowseContext invalidContext = mock(BrowseContext.class);

        ApiProfileFactory invalidProfileFactory = new ApiProfileFactory(
                HubspotcrmOperationType.from(invalidContext), metadataRetriever);

        try {
            invalidProfileFactory.addFieldsToProfile("contact", new JSONObject(), "POST::/crm/v3/objects/contacts", 1,
                    "relationship-name");
            fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("unexpected operation type: INVALID_OPERATION", e.getMessage());
        }
    }

    @Test
    public void testGetJsonProfile() {
        String objectTypeId = "contact";

        JSONObject fieldMetadata = new JSONObject();
        fieldMetadata.put("name", "field-name");
        fieldMetadata.put("type", "string");

        JSONArray fieldsArray = new JSONArray();
        fieldsArray.put(fieldMetadata);

        JSONObject metadata = new JSONObject();
        metadata.put("results", fieldsArray);

        when(metadataRetriever.getObjectMetadata(objectTypeId)).thenReturn(metadata);

        String jsonProfile = profileFactory.getJsonProfile(objectTypeId, HubspotcrmOperationType.CREATE,"contact");

        JSONObject profile = new JSONObject(jsonProfile);

        assertTrue(profile.has("contact"));
        JSONObject data = profile.getJSONObject("contact");
        assertTrue(data.has("properties"));
        JSONObject properties = data.getJSONObject("properties");
        JSONObject properties2 = properties.getJSONObject("properties");
        JSONObject properties3 = properties2.getJSONObject("properties");
        assertTrue(properties3.has("field-name"));
    }
}