// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.json;

import com.boomi.connector.testutil.doubles.MapMessageDouble;
import com.boomi.connector.testutil.NoLoggingTest;
import com.boomi.util.StringUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jms.MapMessage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RunWith(Parameterized.class)
public class MapMessageParserFillMapMessageTest extends NoLoggingTest {

    private final InputStream _payload;
    private final Map<String, Object> _expectedValues;

    @Parameterized.Parameters(name = "{2}")
    public static Iterable<Object[]> jsonAndValuesProvider() {
        Collection<Object[]> testCases = new ArrayList<>();
        {
            String description = "valid json with several data types";
            String jsonPayload =
                    "{\"key1\": \"some value\", \"key2\": 42, \"key3\": true, \"key4\": false, \"key5\": 1037.5}";
            InputStream stream = new ByteArrayInputStream(jsonPayload.getBytes(StringUtil.UTF8_CHARSET));
            Map<String, Object> expectedValues = new HashMap<>();
            expectedValues.put("key1", "some value");
            expectedValues.put("key2", 42);
            expectedValues.put("key3", true);
            expectedValues.put("key4", false);
            expectedValues.put("key5", 1037.5);

            testCases.add(new Object[] { stream, expectedValues, description });
        }

        {
            String description = "valid json with null values";
            String jsonPayload = "{\"key1\": \"some other value\", \"key2\": null}";
            InputStream stream = new ByteArrayInputStream(jsonPayload.getBytes(StringUtil.UTF8_CHARSET));
            Map<String, Object> expectedValues = new HashMap<>();
            expectedValues.put("key1", "some other value");
            expectedValues.put("key2", null);

            testCases.add(new Object[] { stream, expectedValues, description });
        }

        {
            String description = "invalid json due to array being present";
            String jsonPayload = "{\"key1\": \"value1\", \"key2\": 42, \"key3\": [42, 43]}";
            InputStream stream = new ByteArrayInputStream(jsonPayload.getBytes(StringUtil.UTF8_CHARSET));

            testCases.add(new Object[] { stream, null, description });
        }

        {
            String description = "invalid json due to a nested object being present";
            String jsonPayload = "{\"key1\": \"value1\", \"key2\": 42, \"key3\": {\"key4\": \"some value\"}}";
            InputStream stream = new ByteArrayInputStream(jsonPayload.getBytes(StringUtil.UTF8_CHARSET));

            testCases.add(new Object[] { stream, null, description });
        }

        {
            String description = "malformed json";
            String jsonPayload = "{\"key1\": \"value1\", \"\"key2\": 42}";
            InputStream stream = new ByteArrayInputStream(jsonPayload.getBytes(StringUtil.UTF8_CHARSET));

            testCases.add(new Object[] { stream, null, description });
        }

        return testCases;
    }

    public MapMessageParserFillMapMessageTest(InputStream payload, Map<String, Object> expectedValues, String testCaseDescription) {
        _payload = payload;
        _expectedValues = expectedValues;
    }

    @Test
    public void fillMapMessageTest() {
        Map<String, Object> messageValues = new HashMap<>();
        MapMessage message = new MapMessageDouble("destination", messageValues);

        // a null map means that the parsing should fail
        if (_expectedValues == null) {
            boolean hasSucceed = false;
            try {
                MapMessageParser.fillMapMessage(_payload, message);
                hasSucceed = true;
            } catch (Exception e) {
                // no-op
            }

            Assert.assertFalse("an exception was expected, but it was not thrown", hasSucceed);
        } else {
            MapMessageParser.fillMapMessage(_payload, message);

            for (Map.Entry<String, Object> expectedValue : _expectedValues.entrySet()) {
                String key = expectedValue.getKey();
                Assert.assertEquals(_expectedValues.get(key), messageValues.get(key));
            }
        }
    }
}
