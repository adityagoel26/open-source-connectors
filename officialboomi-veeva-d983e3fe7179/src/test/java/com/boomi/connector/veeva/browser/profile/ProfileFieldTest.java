// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ProfileFieldTest {

    @Test
    void getOutboundRelationShipNameTest() {
        JSONObject outbound = new JSONObject();
        outbound.put("relationship_outbound_name", "outbound_relationship_value");
        ProfileField profileField = new ProfileField(true, true, null, "", outbound);

        assertEquals("outbound_relationship_value", profileField.getRelationshipName());
    }

    @Test
    void getVObjectRelationShipNameTest() {
        JSONObject vObject = new JSONObject();
        vObject.put("relationshipName", "relationshipName_value");
        ProfileField profileField = new ProfileField(true, true, null, "", vObject);
        assertEquals("relationshipName_value", profileField.getRelationshipName());
    }

    @Test
    void getChildRelationShipNameTest() {
        JSONObject child = new JSONObject();
        child.put("relationship_name", "relationship_name_value");
        ProfileField profileField = new ProfileField(true, true, null, "", child);
        assertEquals("relationship_name_value", profileField.getRelationshipName());
    }

    @Test
    void getNullRelationshipNameTest() {

        ProfileField profileField = new ProfileField(true, true, null, "", new JSONObject());
        assertNull(profileField.getRelationshipName());
    }

    @Test
    void getRelationshipObjectTypeTest() {
        JSONObject objectType = new JSONObject();
        objectType.put("objectType", "objectType_value");
        ProfileField profileField = new ProfileField(true, true, null, "", objectType);
        assertEquals("objectType_value", profileField.getRelationshipObject());
    }

    @Test
    void getRelationshipObjectTest() {
        JSONObject object = new JSONObject();
        JSONObject name = new JSONObject();
        name.put("name", "name_value");
        object.put("object", name);
        ProfileField profileField = new ProfileField(true, true, null, "", object);
        assertEquals("name_value", profileField.getRelationshipObject());
    }

    @Test
    void getNullRelationshipObjectTest() {
        ProfileField profileField = new ProfileField(true, true, null, "", new JSONObject());
        assertNull(profileField.getRelationshipObject());
    }
}
