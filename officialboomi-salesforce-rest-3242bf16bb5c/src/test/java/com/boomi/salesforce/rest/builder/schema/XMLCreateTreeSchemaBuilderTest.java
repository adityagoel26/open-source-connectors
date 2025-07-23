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
import static org.mockito.Mockito.mock;

public class XMLCreateTreeSchemaBuilderTest {

    private static final String EXPECTED_SCHEMA = "builder/schema/createTree_0_Children.xml";
    private static final String SF_RESPONSE = "describeCaseResponse.json";

    @Test
    public void shouldBuild_0_LevelOfChildrenAndFields() throws Exception {
        SObjectController controller = mock(SObjectController.class, Mockito.RETURNS_DEEP_STUBS);
        try (InputStream stream = SFRestTestUtil.getContent(SF_RESPONSE)) {
            SObjectModel sobject = MetadataParser.parseFieldsForOperation(stream, "CREATE");
            Mockito.when(controller.buildSObject("Case")).thenReturn(sobject);
        }

        XMLCreateTreeSchemaBuilder builder = new XMLCreateTreeSchemaBuilder(controller, 0);
        builder.generateSchema("Case");
        String strActual = SFRestTestUtil.toString(builder._schema);
        String strExpected = SFRestTestUtil.getDocumentFromResource(EXPECTED_SCHEMA);

        assertEquals(strExpected, strActual);
    }
}
