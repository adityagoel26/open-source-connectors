// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.builder.schema;

import com.boomi.salesforce.rest.controller.metadata.MetadataParser;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.testutil.SFRestTestUtil;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XMLQuerySchemaBuilderTest {

    private static final String EXPECTED_SCHEMA = "builder/schema/schemaParentsDefinitions0.xml";
    private static final String SF_RESPONSE = "describeCaseResponse.json";

    @Test
    public void shouldDefine_0_LevelOfParentsAndFields() throws Exception {
        SObjectController controller = Mockito.mock(SObjectController.class, Mockito.RETURNS_DEEP_STUBS);
        try (InputStream stream = SFRestTestUtil.getContent(SF_RESPONSE)) {
            SObjectModel sobject = MetadataParser.parseFieldsForOperation(stream, "QUERY");
            Mockito.when(controller.buildSObject("Case")).thenReturn(sobject);
        }

        XMLQuerySchemaBuilder builder = new XMLQuerySchemaBuilder(controller, 0, 0);

        builder.defineSObjectParents("Case", 0);

        String strActual = SFRestTestUtil.toString(builder._schema);
        String strExpected = SFRestTestUtil.getDocumentFromResource(EXPECTED_SCHEMA);

        assertEquals(strExpected, strActual);
    }
}
