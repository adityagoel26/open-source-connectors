// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.mongodb.util;

import com.boomi.connector.api.ConnectorException;
import com.fasterxml.jackson.core.JsonFactory;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Test {@link JsonSchemaUtil}
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(fullyQualifiedNames = "com.boomi.connector.mongodb.*")
@PowerMockIgnore({ "javax.net.ssl.*", "javax.security.*" })
public class JsonSchemaUtilTest {

    private static final String SRC_TEST_RESOURCES = "src/test/resources/";

    /**
     * Test if empty arrays are handled correctly when creating JSON schema.
     * @throws IOException
     */
    @Test
    public void testJsonSchemaBlankArray() throws IOException 
    {
       Path pathJson = Paths.get(SRC_TEST_RESOURCES + "emptyArr.json");
       Path pathExp = Paths.get(SRC_TEST_RESOURCES + "emptyArrExpected.json");
       testJsonSchema(pathJson, pathExp); 
    }

    /**
     * Test if nested arrays are handled correctly when creating JSON schema.
     * @throws IOException
     */
    @Test
    public void testJsonSchemaBlankArrayLevelN() throws IOException 
    {
       Path pathJson = Paths.get(SRC_TEST_RESOURCES + "emptyArrLevelN.json");
       Path pathExp = Paths.get(SRC_TEST_RESOURCES + "emptyArrLevelNExpected.json");
       testJsonSchema(pathJson, pathExp);
    }

    /**
     * Test if complex nested arrays are handled correctly when creating JSON schema.
     * @throws IOException
     */
    @Test
    public void testJsonSchemaComplexNestedArrays() throws IOException 
    {
       Path pathJson = Paths.get(SRC_TEST_RESOURCES + "complexNestedArrays.json");
       Path pathExp = Paths.get(SRC_TEST_RESOURCES + "complexNestedArraysExpected.json");
       testJsonSchema(pathJson, pathExp);  
    }

    /**
     * Test that correct exception is thrown.
     * @throws Exception
     */
    @Test
    public void testJsonSchemaException() throws Exception {
        Path     pathJson;
        String   json;
        Document doc;

        JsonFactory mockJsonFactory = Mockito.mock(JsonFactory.class);
        Mockito.when(mockJsonFactory.createParser(Mockito.anyString())).thenThrow(new IOException("Mocked error!"));
        PowerMockito.whenNew(JsonFactory.class).withNoArguments().thenReturn(mockJsonFactory);

        pathJson = Paths.get(SRC_TEST_RESOURCES + "nonEmptyArr.json");
        json = getStringFromPath(pathJson);
        doc = Document.parse(json);

        try {
            JsonSchemaUtil.createJsonSchema(doc);
            Assert.fail();
        } catch (ConnectorException exception) {
            Assert.assertEquals("message is not correct", "Failed to create JSON Schema: Mocked error!",
                    exception.getStatusMessage());
            Assert.assertNotNull("stack trace is null!", exception.getStackTrace());
            Assert.assertTrue("exception is correct", exception.getCause() instanceof IOException);
        }
    }

    /**
     * Test if non empty arrays are handled correctly when creating JSON schema.
     * @throws IOException
     */
    @Test
    public void testJsonSchemaNonEmptyArray() throws IOException
    {
        Path pathJson = Paths.get(SRC_TEST_RESOURCES + "nonEmptyArr.json");
        Path pathExp = Paths.get(SRC_TEST_RESOURCES + "nonEmptyArrExpected.json");
        testJsonSchema(pathJson, pathExp);
    }
     
    private void testJsonSchema(Path pathJson, Path pathExp) throws IOException {
        
        String actualSchema;
        String expectedSchema;
        Stream <String> lines;
        String data;
        Document doc;

        data = getStringFromPath(pathJson);
        expectedSchema = getStringFromPath(pathExp);
        
        doc = Document.parse(data);
        actualSchema = JsonSchemaUtil.createJsonSchema(doc);
        
        assertEquals(expectedSchema.replace("\n", "").replace("\r", ""), actualSchema.replace("\n", "").replace("\r", "")); 
    }

    private static String getStringFromPath(Path pathJson) throws IOException {
        Stream<String> lines;
        String         data;
        lines = Files.lines(pathJson);
        data = lines.collect(Collectors.joining("\n"));
        return data;
    }
}
