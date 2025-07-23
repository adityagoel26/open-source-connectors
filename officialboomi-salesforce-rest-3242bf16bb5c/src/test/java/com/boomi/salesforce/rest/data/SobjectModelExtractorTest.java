// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SobjectModelExtractorTest {

    private static final String SAMPLE_JSON = "describeAccountResponse.json";

    @Test
    public void shouldParseSobjectResponse() throws Exception {
        InputStream jsonInput = SFRestTestUtil.getContent(SAMPLE_JSON);

        List<String> list = Arrays.asList("createable", "relationshipName", "referenceTo", "name", "type", "scale",
                "precision", "childSObject");
        SobjectModelExtractor splitter = new SobjectModelExtractor(jsonInput, list);
        int counterChildren = 0, counterFields = 0;
        while (splitter.parseSObject()) {
            if (splitter.isChildModel()) {
                counterChildren++;
            } else if (splitter.isFieldModel()) {
                counterFields++;
            }
        }
        jsonInput.close();
        splitter.close();

        assertEquals(3, counterChildren);
        assertEquals(5, counterFields);
    }
}
