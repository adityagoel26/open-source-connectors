// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.amazon_redshift_data;

import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class ValidateXMLTest {

    @Test
    public void validateConnectorConfig() {
        String xml = "/connector-config-amazon_redshift_data.xml";
        String xsd = "/genericconnector.xsd";
        try {
            validateAgainstXSD(xml, xsd);
        } catch (Exception e) {
            Assert.fail("Exception: " + e);
        }
    }

    @Test
    public void validateConnectorDescriptor() {
        String xml = "/connector-descriptor-amazon_redshift_data.xml";
        String xsd = "/genericconnectordesc.xsd";
        try {
            validateAgainstXSD(xml, xsd);
        } catch (Exception e) {
            Assert.fail("Exception: " + e);
        }
    }

    void validateAgainstXSD(String xmlPath, String xsdPath) throws IOException, SAXException {
        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        InputStream xmlStream = this.getClass().getResourceAsStream(xmlPath);

        URL xsdUrl = this.getClass().getResource(xsdPath);

        Schema schema = factory.newSchema(xsdUrl);
        Validator validator = schema.newValidator();
        validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        validator.validate(new StreamSource(xmlStream));

    }
}
