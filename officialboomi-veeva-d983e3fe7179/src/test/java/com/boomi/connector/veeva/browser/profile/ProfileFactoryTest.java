// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.connector.veeva.browser.VeevaOperationType;
import com.boomi.util.CollectionUtil;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProfileFactoryTest {

    private static final String FIRST_EXPECTED_FIELD = "{\"description\":\"MedSource ID\",\"type\":\"string\"}";
    private static final int EXPECTED_METADATA_FIELDS_NUMBER = 52;

    @Test
    void addJSONSchemaFieldDescriptionTest() {
        JSONObject property = new JSONObject();
        JSONObject metadataField = new JSONObject();
        metadataField.put("label", "label description");
        metadataField.put("helpContent", "helpContent description");
        ProfileFactory.addJSONSchemaFieldDescription(property, metadataField);
        assertEquals("label description - helpContent description", property.getString("description"));
    }

    @Test
    void addJSONSchemaFieldDescriptionHelpUnderscoreTest() {
        JSONObject property = new JSONObject();
        JSONObject metadataField = new JSONObject();
        metadataField.put("label", "label description");
        metadataField.put("help_content", "helpContent description");
        ProfileFactory.addJSONSchemaFieldDescription(property, metadataField);
        assertEquals("label description - helpContent description", property.getString("description"));
    }

    @Test
    void processVObjectRelationshipsTest() throws IOException {
        JSONObject response = new JSONObject(new JSONTokener(
                Files.newInputStream(new File("src/test/java/resources/case__v metadata.json").toPath())));

        VeevaObjectMetadataRetriever metadataRetriever = mock(VeevaObjectMetadataRetriever.class);
        when(metadataRetriever.getObjectMetadata("case__v")).thenReturn(response);

        ProfileFactory profileFactory = new ProfileFactory(VeevaOperationType.QUERY, metadataRetriever) {
            @Override
            void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
                    String lookupRelationshipName) {

            }
        };

        Iterable<ProfileField> fields = profileFactory.getProfileFields("case__v");

        ProfileField firstField = CollectionUtil.getFirst(fields);
        assertEquals(FIRST_EXPECTED_FIELD, firstField.getSchema().toString());
        assertEquals("id", firstField.getName());
        assertEquals("string", firstField.getSpecFilterType());
        assertNull(firstField.getRelationshipObject());
        assertNull(firstField.getRelationshipName());
        assertFalse(firstField.isEditable());
        assertFalse(firstField.isRequired());
        assertFalse(firstField.isQueryable());
        assertFalse(firstField.isSubQuery());
        assertFalse(firstField.isSetOnCreateOnly());
        assertFalse(firstField.isSystemAttribute());
        assertFalse(firstField.isChild());
        assertFalse(firstField.isObjectReference());

        int count = 1;
        for (ProfileField field : fields) {
            count++;
        }
        assertEquals(EXPECTED_METADATA_FIELDS_NUMBER, count);
    }
}
