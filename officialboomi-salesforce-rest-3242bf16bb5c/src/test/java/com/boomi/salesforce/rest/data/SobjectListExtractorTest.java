// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SobjectListExtractorTest {
    private static final String SAMPLE_JSON = "sobjectListResponse.json";

    @Test
    public void shouldParseSobjectResponse() throws Exception {
        InputStream jsonInput = SFRestTestUtil.getContent(SAMPLE_JSON);

        List<String> list = Arrays.asList("updateable", "queryable", "name", "createable");
        SobjectListExtractor splitter = new SobjectListExtractor(jsonInput, list);
        int counter = 0;
        while (splitter.parseSObject()) {
            ++counter;
        }
        jsonInput.close();
        splitter.close();

        assertEquals(4, counter);
    }
}
