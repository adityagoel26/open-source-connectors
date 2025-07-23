// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.mongodb.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.bean.OutputDocument;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.testutil.SimpleAtomConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.model.Filters;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class DocumentUtilTest {

    private static final String ERROR_INFO_MESSAGE = "Result returned is not same";
    private static final String INPUT =
            "{\r\n" + "\"Serial_number\":\"10057-2015-ENFO\",\r\n" + "\"certificate_number\":\"newdirectory\"\r\n"
                    + "\"name\":\"LD SOLUTIONS\"\r\n" + '}';
    private static final String EXPECTED_RESULT =
            "{\"Serial_number\":\"10057-2015-ENFO\",\"certificate_number\":\"newdirectory\"\"name\":\"LD SOLUTIONS\"}";
    private InputStream inputStream;
    private SimpleAtomConfig atomConfig;

    private LinkedHashMap<String, Object> createMap(String name, String streetAddress, String email) {
        LinkedHashMap<String, Object> docValueMap = new LinkedHashMap<>();
        docValueMap.put("name", name);
        docValueMap.put("address", streetAddress);
        docValueMap.put("email", email);
        return docValueMap;
    }

    @Before
    public void setUp() {
        inputStream = new ByteArrayInputStream(INPUT.getBytes());
        atomConfig = new SimpleAtomConfig();
    }

    @Test
    public void testGetDocsFromInputBatch() {
        int errorCode = -3;
        String errorMessage = "Connection failed due to bad network";

        TrackedData trackedData = mock(TrackedData.class);

        Document resultDocumentOne = new Document(createMap("John Doe", "UK, Australia", "johndoe@gmail.com"));
        Document resultDocumentTwo = new Document(createMap("Jane Doe", "New Zealand", "Janedoe@gmail.com"));

        List<TrackedDataWrapper> trackedDataWrapperList = Arrays.asList(
                new TrackedDataWrapper(trackedData, resultDocumentOne, errorCode, errorMessage),
                new TrackedDataWrapper(trackedData, resultDocumentTwo, errorCode, errorMessage));

        List<Document> actualDocumentsList = DocumentUtil.getDocsFromInputBatch(trackedDataWrapperList);

        assertEquals(trackedDataWrapperList.size(), actualDocumentsList.size());
        assertEquals("Expected document at the index 0 is different", resultDocumentOne, actualDocumentsList.get(0));
        assertEquals("Expected document at the index 1 is different", resultDocumentTwo, actualDocumentsList.get(1));
    }

    @Test
    public void testGetJsonSchema() throws JsonProcessingException, JSONException {
        String expectedJsonSchema = "{\"type\":\"object\",\"id\":\"urn:jsonschema:com:boomi:connector:mongodb:bean"
                + ":OutputDocument\",\"properties\":{\"errorDetails\":{\"type\":\"object\","
                + "\"id\":\"urn:jsonschema:com:boomi:connector:mongodb:bean:ErrorDetails\","
                + "\"properties\":{\"errorCode\":{\"type\":\"integer\"},\"errorMessage\":{\"type\":\"string\"}}}}}";

        String actualJsonSchema = DocumentUtil.getJsonSchema(OutputDocument.class);
        assertNotNull("JsonSchema returned is NULL but it should be not null", actualJsonSchema);
        Assert.assertEquals("Jsonschema returned is not same", expectedJsonSchema, actualJsonSchema.replaceAll("\\s",""));
    }

    @Test
    public void testInputStreamToStringWithCharSetNull() throws IOException {
        String actualResult = DocumentUtil.inputStreamToString(inputStream, null);
        assertEquals(ERROR_INFO_MESSAGE, EXPECTED_RESULT, actualResult);
    }

    @Test
    public void testInputStreamToStringWithCharSetNotNull() throws IOException {
        String actualResult = DocumentUtil.inputStreamToString(inputStream, StandardCharsets.UTF_8);
        assertEquals(ERROR_INFO_MESSAGE, EXPECTED_RESULT, actualResult);
    }


   @Test
    public void testToPayload() throws IOException {
        Document document = new Document(createMap("John Doe", "UK, Australia", "johndoe@gmail.com"));
        Payload payload=DocumentUtil.toPayLoad(document);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        payload.writeTo(outputStream);
        String jsonString = outputStream.toString(StandardCharsets.UTF_8.name());
        assertEquals("{\"name\": \"John Doe\", \"address\": \"UK, Australia\", \"email\": \"johndoe@gmail.com\"}",jsonString);
    }

    @Test
    public void testToPayloadWithEmptyDocument() throws IOException {
        Document document = new Document();
        Payload payload=DocumentUtil.toPayLoad(document);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        payload.writeTo(outputStream);
        String jsonString = outputStream.toString(StandardCharsets.UTF_8.name());
        assertEquals("{}",jsonString);
    }

    @Test
    public void testToPayloadWithNullDocument(){
        Document document = null;
        Payload payload=DocumentUtil.toPayLoad(document);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        assertThrows(ConnectorException.class,()-> payload.writeTo(outputStream));
    }

    @Test
    public void testToPayloadDocument() throws IOException {
        ObjectId fixedObjectId = new ObjectId("65e5647da12cf7740c1324f8");
        Document bsonDocument = new Document();
        bsonDocument.append("stringField", new BsonString("Hello, MongoDB!"))
                .append("integerField", new BsonInt32(42))
                .append("booleanField", new BsonBoolean(true))
                .append("doubleField", new BsonDouble(3.14))
                .append("minMaxField", new BsonDocument("$minKey", new BsonInt32(1)).append("$maxKey", new BsonInt32(1)))
                .append("arrayField", new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))))
                .append("timestampField", new BsonTimestamp(1234567890, 1))
                .append("objectField", new BsonDocument("nestedField", new BsonString("value")))
                .append("nullField", new BsonNull())
                .append("symbolField", new BsonDocument("$symbol", new BsonString("symbolValue")))
                .append("dateField", new BsonDateTime(1709532285044L))
                .append("objectIdField", new BsonObjectId(fixedObjectId))
                .append("binaryField", new BsonBinary("Hello, MongoDB!".getBytes()))
                .append("codeField", new BsonDocument("$code", new BsonString("function() { return 'Hello, World!'; }")))
                .append("regexField", new BsonDocument("$regex", new BsonString("^Hello")).append("$options", new BsonString("i")));

        Payload payload = DocumentUtil.toPayLoad(bsonDocument);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        payload.writeTo(outputStream);
        String jsonString = outputStream.toString(StandardCharsets.UTF_8.name());
        String expected = "{\"stringField\": \"Hello, MongoDB!\", \"integerField\": 42, \"booleanField\": true, \"doubleField\": 3.14, \"minMaxField\": {\"$minKey\": 1, \"$maxKey\": 1}, \"arrayField\": [1, 2, 3], \"timestampField\": {\"$timestamp\": {\"t\": 1234567890, \"i\": 1}}, \"objectField\": {\"nestedField\": \"value\"}, \"nullField\": null, \"symbolField\": {\"$symbol\": \"symbolValue\"}, \"dateField\": {\"$date\": 1709532285044}, \"objectIdField\": {\"$oid\": \"65e5647da12cf7740c1324f8\"}, \"binaryField\": {\"$binary\": \"SGVsbG8sIE1vbmdvREIh\", \"$type\": \"00\"}, \"codeField\": {\"$code\": \"function() { return 'Hello, World!'; }\"}, \"regexField\": {\"$regex\": \"^Hello\", \"$options\": \"i\"}}";
        assertEquals(jsonString.trim(), expected);
    }

    /**
     * Tests the behavior of parsing a long value or returning a default value when the input is null.
     */
    @Test
    public void testParseLongOrDefaultWithNull() {
        assertEquals(MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE,
                DocumentUtil.parseLongOrDefault(null, MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE));
    }

    /**
     * Tests the behavior of parsing a long value or returning a default value when the input is invalid.
     */
    @Test
    public void testParseLongOrDefaultWithInvalidData() {
        assertEquals(MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE,
                DocumentUtil.parseLongOrDefault("10a", MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE));
    }

    /**
     * Tests the behavior of parsing a valid long value or returning a default value when the input is valid.
     */
    @Test
    public void testParseLongOrDefault() {
        assertEquals(10L, DocumentUtil.parseLongOrDefault("10", MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE));
    }

    /**
     * When bsonFilter is null and maxDocumentSize is zero,
     * the method should return an empty filter.
     */
    @Test
    public void testGetFilterFromAtomConfigWhenBsonFilterIsNullAndMaxDocumentSizeIsZero() {
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"0");
        Bson filter = DocumentUtil.buildFilterWithMaxDocumentSize(atomConfig, null);
        assertEquals(Filters.empty(), filter);
    }

    /**
     * Test case: When bsonFilter is not null and maxDocumentSize is zero,
     * the method should return the provided bsonFilter.
     */
    @Test
    public void testGetFilterFromAtomConfigWhenBsonFilterIsNotNullAndMaxDocumentSizeIsZero() {
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"0");
        Bson bsonFilter = Filters.eq("fieldName", "value");
        Bson filter = DocumentUtil.buildFilterWithMaxDocumentSize(atomConfig, bsonFilter);
        assertEquals(bsonFilter, filter);
    }

    /**
     * When bsonFilter is null and maxDocumentSize is greater than zero,
     * the method should return a filter based on maxDocumentSize.
     */
    @Test
    public void testGetFilterFromAtomConfigWhenBsonFilterIsNullAndMaxDocumentSizeIsGreaterThanZero() {
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"100");
        Bson filter = DocumentUtil.buildFilterWithMaxDocumentSize(atomConfig, null);
        Bson expectedFilter = Filters.and(Filters.empty(), Filters.where(DocumentUtil.getSizeLimitQuery(100)));
        assertEquals(expectedFilter, filter);
    }

    /**
     * When bsonFilter is not null and maxDocumentSize is greater than zero,
     * the method should return a filter based on maxDocumentSize and the provided bsonFilter.
     */
    @Test
    public void testGetFilterFromAtomConfigWhenBsonFilterIsNotNullAndMaxDocumentSizeIsGreaterThanZero() {
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"100");
        Bson bsonFilter = Filters.eq("fieldName", "value");
        Bson filter = DocumentUtil.buildFilterWithMaxDocumentSize(atomConfig, bsonFilter);
        Bson expectedFilter = Filters.and(bsonFilter, Filters.where(DocumentUtil.getSizeLimitQuery(100)));
        assertEquals(expectedFilter, filter);
    }

    /**
     * When bsonFilter is Not null and atomConfig is null,
     * the method should return a filter based on defaultMaxDocumentSize and the provided bsonFilter.
     */
    @Test
    public void testBuildFilterWithMaxDocumentSizeWhenAtomConfigIsNullAndBsonFilterIsNotNull() {
        Bson bsonFilter = Filters.eq("fieldName", "value");
        Bson filter = DocumentUtil.buildFilterWithMaxDocumentSize(null, bsonFilter);
        Bson expectedFilter = Filters.and(bsonFilter,
                Filters.where(DocumentUtil.getSizeLimitQuery(MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE)));
        assertEquals(expectedFilter, filter);
    }

    /**
     * When bsonFilter is  null and atomConfig is null,
     * the method should return a filter based on defaultMaxDocumentSize.
     */
    @Test
    public void testBuildFilterWithMaxDocumentSizeWhenAtomConfigIsNullAndBsonFilterIsNull() {
        Bson filter = DocumentUtil.buildFilterWithMaxDocumentSize(null, null);
        Bson expectedFilter = Filters.and(Filters.empty(),
                Filters.where(DocumentUtil.getSizeLimitQuery(MongoDBConstants.DEFAULT_MAX_DOCUMENT_SIZE)));
        assertEquals(expectedFilter, filter);
    }
}