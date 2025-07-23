// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite.batch;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SFCompositeCombinerTest {

    private static final String EXPECTED_PAYLOAD_WITH_ALL_OR_NONE =
            "<result><allOrNone>True</allOrNone><example-node-1>example1</example-node-1><example-node-2"
            + ">example2</example-node-2><example-node-3>example3</example-node-3></result>";
    private static final String EXPECTED_PAYLOAD_WITHOUT_ALL_OR_NONE =
            "<result><example-node-1>example1</example-node-1><example-node-2>example2</example-node-2><example-node"
            + "-3>example3</example-node-3></result>";

    private static final String INPUT_WITHOUT_TYPE_TAG = "<records><Name>name3 create</Name></records>";
    private static final String INPUT_WITH_TYPE_TAG = "<records type=\"Pricebook\"><Name>name3 create</Name></records>";
    private static final String EXPECTED_PAYLOAD_WITH_TYPE_TAG =
            "<result><records type=\"Pricebook\"><Name>name3 create</Name></records></result>";
    private static final String SOBJECT_NAME = "Pricebook";

    @Test
    public void buildCompositeDeleteTest() {
        SFCompositeCombiner combiner = new SFCompositeCombiner();
        String result = combiner.buildCompositeDelete(buildObjectDataListForDelete());

        assertEquals("example1,example2,example3", result);
    }

    private static List<ObjectIdData> buildObjectDataListForDelete() {
        ObjectIdData data1 = new SimpleTrackedData(1, "example1");
        ObjectIdData data2 = new SimpleTrackedData(2, "example2");
        ObjectIdData data3 = new SimpleTrackedData(2, "example3");

        return Arrays.asList(data1, data2, data3);
    }

    @Test
    public void buildCompositeBodyWithAllOrNoneTrueTest() throws IOException {
        buildCompositeBodyTest(true, EXPECTED_PAYLOAD_WITH_ALL_OR_NONE);
    }

    @Test
    public void buildCompositeBodyWithAllOrNoneFalseTest() throws IOException {
        buildCompositeBodyTest(false, EXPECTED_PAYLOAD_WITHOUT_ALL_OR_NONE);
    }

    private void buildCompositeBodyTest(boolean allOrNoneFlag, String expected) throws IOException {
        SFCompositeCombiner combiner = new SFCompositeCombiner();

        InputStream stream = null;
        try {
            stream = combiner.buildCompositeBody(buildObjectDataListForComposite(), allOrNoneFlag,
                    StringUtil.EMPTY_STRING);

            assertEquals(expected, StreamUtil.toString(stream, StringUtil.UTF8_CHARSET));
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    private static List<ObjectData> buildObjectDataListForComposite() {
        ObjectData data1 = toObjectData(1, "<example-node-1>example1</example-node-1>");
        ObjectData data2 = toObjectData(2, "<example-node-2>example2</example-node-2>");
        ObjectData data3 = toObjectData(3, "<example-node-3>example3</example-node-3>");

        return Arrays.asList(data1, data2, data3);
    }

    @ValueSource(strings = { INPUT_WITHOUT_TYPE_TAG, INPUT_WITH_TYPE_TAG })
    @ParameterizedTest
    public void buildCompositeBodyShouldInjectTypeTagIfNotPresent(String inputDocument) throws IOException {
        ObjectData data = toObjectData(0, inputDocument);

        SFCompositeCombiner combiner = new SFCompositeCombiner();
        InputStream stream = null;
        try {
            stream = combiner.buildCompositeBody(Collections.singletonList(data), false, SOBJECT_NAME);

            assertEquals(EXPECTED_PAYLOAD_WITH_TYPE_TAG, StreamUtil.toString(stream, StringUtil.UTF8_CHARSET));
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    private static ObjectData toObjectData(int id, String payload) {
        return new SimpleTrackedData(id, new ByteArrayInputStream(payload.getBytes(StringUtil.UTF8_CHARSET)));
    }
}
