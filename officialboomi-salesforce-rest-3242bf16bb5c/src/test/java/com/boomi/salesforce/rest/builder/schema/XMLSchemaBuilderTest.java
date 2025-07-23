// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.builder.schema;

import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.salesforce.rest.testutil.SFRestTestUtil;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class XMLSchemaBuilderTest {

    private static final String STRING_CH = "builder/schema/fieldString.xml";
    private static final String DATE_TIME_DTT = "builder/schema/fieldDateTime.xml";
    private static final String STRING_WITH_PRECISION = "builder/schema/fieldWithPrecision_12_3.xml";
    private static final String TYPE_DATE_TIME_FORMAT = "builder/schema/typeDateTimeFormat.xml";

    @Test
    public void shouldCreateStringElementWithChType() throws Exception {
        SObjectField field = new SObjectField("name", "string");
        generateFieldTest(field, STRING_CH);
    }

    @Test
    public void shouldCreateStringElementWithDoubleTypeWithPrecision12_3() throws Exception {
        SObjectField field = new SObjectField("name", "double", 12, 3);
        generateFieldTest(field, STRING_WITH_PRECISION);
    }

    @Test
    public void shouldCreateStringElementWithDTTType() throws Exception {
        SObjectField field = new SObjectField("date__c", "datetime");
        generateFieldTest(field, DATE_TIME_DTT);
    }

    @Test
    public void shouldCreateTypeElementWithDateTimeFormat() throws Exception {
        XMLSchemaBuilder builder = createXmlSchemaBuilder();
        Element result = builder.generateFieldDefinition("dtt", "datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        verifyElement(TYPE_DATE_TIME_FORMAT, result);
    }

    private static void generateFieldTest(SObjectField field, String expected) throws Exception {
        XMLSchemaBuilder builder = createXmlSchemaBuilder();

        Element element = builder.generateField(field);
        verifyElement(expected, element);
    }

    private static void verifyElement(String expected, Element element) throws Exception {
        String actual = SFRestTestUtil.toString(element);
        assertEquals(SFRestTestUtil.getDocumentFromResource(expected), actual);
    }

    private static XMLSchemaBuilder createXmlSchemaBuilder() {
        return new XMLQuerySchemaBuilder(mock(SObjectController.class), 3, 0);
    }
}

