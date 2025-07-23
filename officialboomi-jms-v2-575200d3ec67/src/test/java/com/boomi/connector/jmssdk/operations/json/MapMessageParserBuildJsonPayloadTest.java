// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.json;

import com.boomi.connector.testutil.doubles.MapMessageDouble;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.Assert;
import org.junit.Test;

import javax.jms.JMSException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class MapMessageParserBuildJsonPayloadTest {

    @Test
    public void buildJsonPayloadTest() throws IOException, JMSException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", 42);
        properties.put("key3", true);
        properties.put("key4", false);
        properties.put("key5", 1035.2);

        MapMessageDouble message = new MapMessageDouble("destination", properties);

        InputStream stream = MapMessageParser.buildJsonPayload(message);

        String json = StreamUtil.toString(stream, StringUtil.UTF8_CHARSET);
        Assert.assertEquals("{\"key1\":\"value1\",\"key2\":42,\"key5\":1035.2,\"key3\":true,\"key4\":false}", json);
    }
}
