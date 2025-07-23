// Copyright (c) 2022 Boomi, LP
package com.boomi.connector.mongodb;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MongoDBBrowserTest {

    private static final String JSONINPUT = "{\"type\": \"object\", \"properties\": {\"_id\": { \"type\": "
            + "\"integer\" },\"Name\": { \"type\": \"string\" },\"Age\": { \"type\": \"integer\" }}}";

    private MongoDBConnectorConnection connection = mock(MongoDBConnectorConnection.class);

    private MongoDBConnectorBrowser mongoDBConnectorBrowser = new MongoDBConnectorBrowser(connection);

    @Test
    public void callGetConnection() {
        assertNotNull(mongoDBConnectorBrowser.getConnection());
    }

    @Test
    public void testgGetJsonParser() throws IOException {
        JsonParser jsonParser = mongoDBConnectorBrowser.getJsonParser(JSONINPUT);
        int index = 0;
        String[] expectedParsedData = {
                "type", "properties", "_id", "type", "Name", "type", "Age", "type" };
        boolean isDataOk = false;

        while (!jsonParser.isClosed()) {
            JsonToken token = jsonParser.nextToken();

            if (JsonToken.FIELD_NAME.equals(token)) {
                String fieldName = jsonParser.getCurrentName();

                isDataOk = expectedParsedData[index++].equals(fieldName);
            }
        }
        assertTrue(isDataOk);
    }
}
