// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.ProfileUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class ProfileUtilsTest {

    private static final String IDTYPE = "object";
    private static final String PROFILEDATA =
            "{\"type\": \"object\", \"properties\": {\"_id\": { \"type\": \"object\", \"properties\": "
                    + "{\"$oid\": { \"type\": \"string\" }}},\"name\": { \"type\": \"string\" },\"location\": { "
                    + "\"type\": " + "\"string\" },\"email\": { \"type\": \"string\" }}}";
    private static final String FIELD = "_id";
    private static final String INVALIDPROFILE =
            "{\"type\": \"object\", \"properties\": [\"_id\": { \"type\": \"object\", \"properties\": "
                    + "{\"$oid\": { \"type\": \"string\" }}},\"name\": { \"type\": \"string\" },\"location\": { "
                    + "\"type\": " + "\"string\" },\"email\": { \"type\": \"string\" }]}";
    private ProfileUtils profileUtils;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetTypeWithValidObjectId() throws MongoDBConnectException, IOException {
        String actual = null;
        profileUtils = new ProfileUtils(PROFILEDATA);

        actual = profileUtils.getType(FIELD);

        Assert.assertNotNull(actual);
        Assert.assertEquals(IDTYPE, actual);
    }

    @Test
    public void testGetTypeInvalidProfile() throws MongoDBConnectException, IOException {
        profileUtils = new ProfileUtils(INVALIDPROFILE);

        expectedException.expect(MongoDBConnectException.class);
        expectedException.expectMessage("Error while fetching type for field");

        profileUtils.getType(FIELD);
    }
}
