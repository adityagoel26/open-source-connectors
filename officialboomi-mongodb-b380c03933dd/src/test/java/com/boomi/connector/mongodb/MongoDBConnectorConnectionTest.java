// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.mongodb.bean.BatchResult;
import com.boomi.connector.mongodb.bean.ErrorDetails;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.DocumentUtil;
import com.boomi.connector.testutil.MongoDBConnection;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleAtomConfig;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.util.BaseConnection;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.Projections;

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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.AUTHENTICATION_TYPE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.REPLICA_SET_MEMBERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDBConnectorConnectionTest {

    private static final String ERROR_CODE_INFO = "Status code is different";
    private static final String ERROR_MESSAGE_INFO = "Message is different";
    private static final String EXPECTED_STATUS_CODE = "200";
    private static final String EXPECTED_MESSAGE = "processed successfully";
    private static final String EMAIL = "email";
    private static final String ADDRESS = "address";
    private static final String NAME = "name";
    private static final String INPUT =
            "{\r\n" + "\"Serial_number\":\"10057-2015-ENFO\",\r\n" + "\"certificate_number\":\"newdirectory\"\r\n"
                    + "\"name\":\"LD SOLUTIONS\"\r\n" + '}';
    private static final MockedStatic<DocumentUtil> DOCUMENT_UTIL = Mockito.mockStatic(DocumentUtil.class);
    private static final MockedStatic<MongoClients> MONGO_CLIENTS = Mockito.mockStatic(MongoClients.class);
    private final Logger logger = mock(Logger.class);
    private final SimpleAtomConfig atomConfig = new SimpleAtomConfig(100, 500, 204800);

    private static final String COLLECTION_NAME = "initial-test";
    private static final String ERROR_MESSAGE = "errorMessage";
    private static final InputWrapper inputWrapper = mock(InputWrapper.class);
    private final SimpleOperationResponse operationResponse = new SimpleOperationResponse();
    private final GetRequest getRequest = mock(GetRequest.class);
    private final BatchResult batchResult = mock(BatchResult.class);
    private BaseConnection baseConnection = mock(BaseConnection.class);

    private final BrowseContext browseContext = mock(BrowseContext.class);
    private final OperationContext operationContext = mock(OperationContext.class);
    private final Document doc = mock(Document.class);
    private final MongoCursor mongoCursor = mock(MongoCursor.class);
    private final ErrorDetails errorDetails = mock(ErrorDetails.class);
    private final MongoClient mongoClient = mock(MongoClient.class);
    private final MongoDBConnection mongoDBConnection = mock(MongoDBConnection.class);
    private final MongoDatabase mongoDatabase = mock(MongoDatabase.class);
    private final MongoCollection mongoCollection = mock(MongoCollection.class);
    private final WriteError writeError = mock(WriteError.class);
    private final ServerAddress serverAddress = mock(ServerAddress.class);
    private final SimpleOperationResponseWrapper simpleOperationResponseWrapper = new SimpleOperationResponseWrapper();
    private boolean isBatchFailedExpected;
    private Exception exception;
    private MongoDBConnectorConnection mongoDBConnectorConnection;
    private SimpleTrackedData trackedData;
    private Bson bsonprojection;
    private Bson bsonFilter;
    private Bson sortKeys;

    @AfterClass
    public static void tearDown() {
        DOCUMENT_UTIL.close();
        MONGO_CLIENTS.close();
    }

    private static LinkedHashMap<String, Object> createMap(String name, String address, String email) {
        LinkedHashMap<String, Object> docValueMap = new LinkedHashMap<>();
        docValueMap.put(NAME, name);
        docValueMap.put(ADDRESS, address);
        docValueMap.put(EMAIL, email);
        return docValueMap;
    }

    @Before
    public void setup() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        isBatchFailedExpected = false; //Initialization

        InputStream result = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));
        trackedData = new SimpleTrackedData(1, result);
        simpleOperationResponseWrapper.addTrackedData(trackedData, operationResponse);

        PropertyMap propertyMap = mock(PropertyMap.class);
        when(operationContext.getOperationProperties()).thenReturn(propertyMap);
        when(browseContext.getConnectionProperties()).thenReturn(propertyMap);
        when(propertyMap.getProperty(MongoDBConstants.DATABASE)).thenReturn("build_on_db");
        when(propertyMap.getProperty(REPLICA_SET_MEMBERS)).thenReturn("build_on");
        when(propertyMap.getProperty(AUTHENTICATION_TYPE)).thenReturn("");
        when(propertyMap.getBooleanProperty(MongoDBConstants.MONGO_SRV, false)).thenReturn(true);
        when(propertyMap.getProperty(MongoDBConstants.CONNECTION_STRING)).thenReturn("connectionString");
        when(propertyMap.getProperty(MongoDBConstants.USER_NAME)).thenReturn("username");
        when(propertyMap.getProperty(MongoDBConstants.CONSTANT_MONGOPD)).thenReturn("password");
        when(propertyMap.getLongProperty(MongoDBConstants.QUERY_BATCHSIZE,
                MongoDBConstants.DEFAULTBATCHSIZE)).thenReturn(1L);

        when(mongoDBConnection.getMongoDBConnection()).thenReturn(mongoDatabase);

        when(mongoDatabase.getCollection(anyString())).thenReturn(mongoCollection);

        when(inputWrapper.getAppErrorRecords()).thenReturn(new ArrayList<>());

        mongoDBConnectorConnection = new MongoDBConnectorConnection(browseContext);
    }

    @Test
    public void testDoCreateWithInsertOne() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);

        Document expectedDocument1 = new Document(docValueMap);
        List<Document> documentList = Collections.singletonList(expectedDocument1);

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, expectedDocument1, 102,
                ERROR_MESSAGE);
        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        DOCUMENT_UTIL.when(() -> DocumentUtil.getDocsFromInputBatch(trackedDataWrapperList)).thenReturn(documentList);
        MONGO_CLIENTS.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);

        mongoDBConnectorConnection.setCollection(mongoCollection);
        mongoDBConnectorConnection.doCreate(trackedDataWrapperList, COLLECTION_NAME);

        verify(mongoCollection, times(1)).insertOne(expectedDocument1);
    }

    @Test
    public void testDoCreateWithInsertMany() {
        LinkedHashMap<String, Object> docValueMap1 = createMap(NAME, ADDRESS, EMAIL);

        LinkedHashMap<String, Object> docValueMap2 = createMap("MongoDB", "Database", "columns@123.com");

        Document doc1 = new Document(docValueMap1);
        Document doc2 = new Document(docValueMap2);
        List<Document> documentsList = Arrays.asList(doc1, doc2);
        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc1, 102, ERROR_MESSAGE);
        TrackedDataWrapper trackedDataWrapper2 = new TrackedDataWrapper(trackedData, doc2, 300, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrappersList = Arrays.asList(trackedDataWrapper1, trackedDataWrapper2);

        DOCUMENT_UTIL.when(() -> DocumentUtil.getDocsFromInputBatch(trackedDataWrappersList)).thenReturn(documentsList);
        MONGO_CLIENTS.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);

        mongoDBConnectorConnection.setCollection(mongoCollection);
        mongoDBConnectorConnection.doCreate(trackedDataWrappersList, COLLECTION_NAME);

        verify(mongoCollection, atMost(1)).insertOne(doc1);
        verify(mongoCollection, atMost(1)).insertOne(doc2);
    }

    @Test
    public void testDoDeleteWithDeleteOne() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);
        Document expectedDocument1 = new Document(docValueMap);
        List<Document> documentList = Collections.singletonList(expectedDocument1);

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, "12345");

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        DOCUMENT_UTIL.when(() -> DocumentUtil.getDocsFromInputBatch(trackedDataWrapperList)).thenReturn(documentList);
        MONGO_CLIENTS.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);

        mongoDBConnectorConnection.setCollection(mongoCollection);
        mongoDBConnectorConnection.doDelete(trackedDataWrapperList, COLLECTION_NAME);

        verify(mongoCollection, times(1)).deleteOne(any());
    }

    @Test
    public void testDoDeleteWithDeleteMany() {
        LinkedHashMap<String, Object> docValueMap1 = createMap(NAME, ADDRESS, EMAIL);
        LinkedHashMap<String, Object> docValueMap2 = createMap("MongoDB", "Database", "columns@123.com");

        Document doc1 = new Document(docValueMap1);
        Document doc2 = new Document(docValueMap2);
        List<Document> documentsList = Arrays.asList(doc1, doc2);
        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, "54321");
        TrackedDataWrapper trackedDataWrapper2 = new TrackedDataWrapper(trackedData, "ABCDE");

        List<TrackedDataWrapper> trackedDataWrappersList = Arrays.asList(trackedDataWrapper1, trackedDataWrapper2);

        DOCUMENT_UTIL.when(() -> DocumentUtil.getDocsFromInputBatch(trackedDataWrappersList)).thenReturn(documentsList);
        MONGO_CLIENTS.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);

        mongoDBConnectorConnection.setCollection(mongoCollection);
        mongoDBConnectorConnection.doDelete(trackedDataWrappersList, COLLECTION_NAME);

        verify(mongoCollection, times(1)).deleteMany(any());
    }

    @Test
    public void testDoModifyWithCreateOne() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);
        Document expectedDocument1 = new Document(docValueMap);

        List<Document> documentList = Collections.singletonList(expectedDocument1);
        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, expectedDocument1);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        DOCUMENT_UTIL.when(() -> DocumentUtil.getDocsFromInputBatch(trackedDataWrapperList)).thenReturn(documentList);
        MONGO_CLIENTS.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);

        mongoDBConnectorConnection.setCollection(mongoCollection);

        FindOneAndReplaceOptions findOneAndReplace = mock(FindOneAndReplaceOptions.class);

        Bson filtersReturn = Filters.in("_id", "$in", "Domodify");
        when(mongoCollection.findOneAndReplace(filtersReturn, trackedDataWrapperList.get(0),
                findOneAndReplace)).thenReturn("Testing");
        mongoDBConnectorConnection.doModify(trackedDataWrapperList, COLLECTION_NAME, true);

        verify(mongoCollection, times(1)).insertOne(expectedDocument1);
    }

    @Test
    public void testUpdateOperationResponseWithExceptionNull() {
        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc, 2, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        mongoDBConnectorConnection.updateOperationResponse(operationResponse, exception, trackedDataWrapperList,
                inputWrapper, batchResult);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        String actualMessage = operationResponse.getResults().get(0).getMessage();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, EXPECTED_MESSAGE, actualMessage);
    }

    /**
     * Tests the updateOperationResponse method when an exception is not null.
     * This test verifies that the operation response contains the expected error status code
     * and error message when a MongoDB query exception occurs.
     *
     * Test steps:
     * 1. Creates a MongoQueryException
     * 2. Creates a TrackedDataWrapper with error message
     * 3. Updates operation response with the exception
     * 4. Verifies the status code and message in the operation response
     *
     * Expected results:
     * - The status code should match the EXPECTED_STATUS_CODE
     * - The error message should match the EXPECTED_MESSAGE
     */
    @Test
    public void testUpdateOperationResponseWithExceptionNotNUll() {
        exception = new MongoQueryException(new BsonDocument(), new ServerAddress());

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc, 2, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        mongoDBConnectorConnection.updateOperationResponse(operationResponse, exception, trackedDataWrapperList,
                inputWrapper, batchResult);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        String actualMessage = operationResponse.getResults().get(0).getMessage();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, EXPECTED_MESSAGE, actualMessage);
    }

    @Test
    public void testUpdateOperationResponseWithIsBatchFailedTrue() {
        exception = new MongoTimeoutException("Time exceeded");

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc, 2, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        mongoDBConnectorConnection.updateOperationResponse(operationResponse, exception, trackedDataWrapperList,
                inputWrapper, batchResult);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        String actualMessage = operationResponse.getResults().get(0).getMessage();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, EXPECTED_MESSAGE, actualMessage);
    }

    @Test
    public void testUpdateOperationResponseWithMongoWriteException() {
        exception = new MongoWriteException(writeError, serverAddress);

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc, 2, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        mongoDBConnectorConnection.updateOperationResponse(operationResponse, exception, trackedDataWrapperList,
                inputWrapper, batchResult);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        String actualMessage = operationResponse.getResults().get(0).getMessage();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, EXPECTED_MESSAGE, actualMessage);
    }

    @Test
    public void testUpdateOperationResponseWithMongoBulkWriteException() {
        exception = mock(MongoBulkWriteException.class);

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc, 2, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        mongoDBConnectorConnection.updateOperationResponse(operationResponse, exception, trackedDataWrapperList,
                inputWrapper, batchResult);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        String actualMessage = operationResponse.getResults().get(0).getMessage();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, EXPECTED_MESSAGE, actualMessage);
    }

    @Test
    public void testUpdateOperationResponseWithMongoWriteConcernException() {
        exception = new MongoWriteConcernException(mock(WriteConcernError.class), mock(WriteConcernResult.class),
                serverAddress);

        TrackedDataWrapper trackedDataWrapper1 = new TrackedDataWrapper(trackedData, doc, 2, ERROR_MESSAGE);

        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper1);

        mongoDBConnectorConnection.updateOperationResponse(operationResponse, exception, trackedDataWrapperList,
                inputWrapper, batchResult);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        String actualMessage = operationResponse.getResults().get(0).getMessage();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, EXPECTED_MESSAGE, actualMessage);
    }

    @Test
    public void testIsBatchFailedWhenExceptionIsNull() {
        boolean isBatchFailedActual = mongoDBConnectorConnection.isBatchFailed(exception);

        assertEquals("Batch failed flag is wrong", isBatchFailedExpected, isBatchFailedActual);
    }

    @Test
    public void testIsBatchFailedWhenExceptionNotNull() {
        exception = new IOException();
        boolean isBatchFailedActual = mongoDBConnectorConnection.isBatchFailed(exception);

        assertEquals("Batch failed flag returns different value", isBatchFailedExpected, isBatchFailedActual);
    }

    @Test
    public void testIsBatchFailedWhenExceptionWithMongoException() {
        exception = new MongoTimeoutException("Time exceeded");
        isBatchFailedExpected = true;
        boolean isBatchFailedActual = mongoDBConnectorConnection.isBatchFailed(exception);

        assertEquals("Different flag returned in batch failed", isBatchFailedExpected, isBatchFailedActual);
    }

    @Test
    public void testupdateQueryResponseNoDocFound() {
        when(mongoCursor.hasNext()).thenReturn(false);

        mongoDBConnectorConnection.updateQueryResponse(mongoCursor, trackedData, operationResponse, errorDetails,
                operationContext);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.SUCCESS, actualStatus);
    }

    @Test
    public void testupdateQueryResponseDocFound() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);
        Document document = new Document(docValueMap);

        when(mongoCursor.hasNext()).thenReturn(true, true, false);
        when(mongoCursor.next()).thenReturn(document);

        mongoDBConnectorConnection.updateQueryResponse(mongoCursor, trackedData, operationResponse, errorDetails,
                operationContext);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.SUCCESS, actualStatus);
    }

    @Test
    public void testUpdateQueryResponseZeroBatchSize() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);
        Document document = new Document(docValueMap);

        when(mongoCursor.hasNext()).thenReturn(true, true, false);
        when(mongoCursor.next()).thenReturn(document);
        when(operationContext.getOperationProperties().getLongProperty(MongoDBConstants.QUERY_BATCHSIZE,
                MongoDBConstants.DEFAULTBATCHSIZE)).thenReturn(0L);

        mongoDBConnectorConnection.updateQueryResponse(mongoCursor, trackedData, operationResponse, errorDetails,
                operationContext);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.SUCCESS, actualStatus);
    }

    @Test
    public void testUpdateQueryResponseNegativeBatchSize() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);
        Document document = new Document(docValueMap);

        when(mongoCursor.hasNext()).thenReturn(true, true, false);
        when(mongoCursor.next()).thenReturn(document);
        when(operationContext.getOperationProperties().getLongProperty(MongoDBConstants.QUERY_BATCHSIZE,
                MongoDBConstants.DEFAULTBATCHSIZE)).thenReturn(-1L);

        mongoDBConnectorConnection.updateQueryResponse(mongoCursor, trackedData, operationResponse, errorDetails,
                operationContext);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.SUCCESS, actualStatus);
    }

    @Test
    public void testupdateQueryResponseException() {
        mongoDBConnectorConnection.updateQueryResponse(null, trackedData, operationResponse, errorDetails,
                operationContext);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, "0", actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.FAILURE, actualStatus);
    }

    @Test
    public void testUpdateOperationResponseforGetDocNotNull() {
        LinkedHashMap<String, Object> docValueMap = createMap(NAME, ADDRESS, EMAIL);
        Document document = new Document(docValueMap);

        mongoDBConnectorConnection.updateOperationResponseforGet(getRequest, operationResponse, errorDetails, document,
                "objId", trackedData, COLLECTION_NAME);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.SUCCESS, actualStatus);
    }

    /**
     * Tests updating the response of a document retrieval operation.
     *
     * @throws JsonProcessingException if there's an error during JSON processing
     */
    @Test
    public void testUpdateOperationResponseforGetDoc() throws JsonProcessingException {
        ObjectId fixedObjectId = new ObjectId("65e5647da12cf7740c1324f8");
        Document bsonDocument = new Document();
        bsonDocument.append("stringField", new BsonString("Hello, MongoDB!")).append("integerField", new BsonInt32(42))
                .append("booleanField", new BsonBoolean(true)).append("doubleField", new BsonDouble(3.14)).append(
                        "minMaxField", new BsonDocument("$minKey", new BsonInt32(1)).append("$maxKey",
                                new BsonInt32(1)))
                .append("arrayField",
                        new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)))).append(
                        "timestampField", new BsonTimestamp(1234567890, 1)).append("objectField",
                        new BsonDocument("nestedField", new BsonString("value"))).append("nullField", new BsonNull()).append(
                        "symbolField", new BsonDocument("$symbol", new BsonString("symbolValue"))).append("dateField",
                        new BsonDateTime(1709532285044L)).append("objectIdField", new BsonObjectId(fixedObjectId)).append(
                        "binaryField", new BsonBinary("Hello, MongoDB!".getBytes())).append("codeField",
                        new BsonDocument("$code", new BsonString("function() { return 'Hello, World!'; }"))).append(
                        "regexField",
                        new BsonDocument("$regex", new BsonString("^Hello")).append("$options", new BsonString("i")));

        when(DocumentUtil.toPayLoad(any())).thenCallRealMethod();

        mongoDBConnectorConnection.updateOperationResponseforGet(getRequest, operationResponse, errorDetails,
                bsonDocument, "objId", trackedData, COLLECTION_NAME);

        String actualStatusCode = operationResponse.getResults().get(0).getStatusCode();
        OperationStatus actualStatus = operationResponse.getResults().get(0).getStatus();

        assertEquals(ERROR_CODE_INFO, EXPECTED_STATUS_CODE, actualStatusCode);
        assertEquals(ERROR_MESSAGE_INFO, OperationStatus.SUCCESS, actualStatus);
        String jsonString = new String(operationResponse.getResults().get(0).getPayloads().get(0),
                StandardCharsets.UTF_8);
        String expected = "{\"stringField\": \"Hello, MongoDB!\", \"integerField\": 42, \"booleanField\": true, "
                + "\"doubleField\": 3.14, \"minMaxField\": {\"$minKey\": 1, \"$maxKey\": 1}, \"arrayField\": "
                + "[1, 2, 3], \"timestampField\": {\"$timestamp\": {\"t\": 1234567890, \"i\": 1}}, "
                + "\"objectField\": {\"nestedField\": \"value\"}, \"nullField\": null, \"symbolField\": "
                + "{\"$symbol\": \"symbolValue\"}, \"dateField\": {\"$date\": 1709532285044}, "
                + "\"objectIdField\": {\"$oid\": \"65e5647da12cf7740c1324f8\"}, \"binaryField\": "
                + "{\"$binary\": \"SGVsbG8sIE1vbmdvREIh\", \"$type\": \"00\"}, \"codeField\": {\"$code\": "
                + "\"function() { return 'Hello, World!'; }\"}, \"regexField\": {\"$regex\": \"^Hello\", "
                + "\"$options\": \"i\"}}";
        assertEquals(expected, jsonString);
    }

    @Test
    public void testPrepareInputConfig() {
        baseConnection = mock(BaseConnection.class);
        PropertyMap operationsProperty = mock(PropertyMap.class);

        when(browseContext.getOperationProperties()).thenReturn(operationsProperty);
        when(baseConnection.getContext()).thenReturn(browseContext);
        when(browseContext.getOperationType()).thenReturn(OperationType.QUERY);
        when(operationsProperty.getLongProperty(MongoDBConstants.QUERY_BATCHSIZE,
                MongoDBConstants.DEFAULTBATCHSIZE)).thenReturn(1L);
        when(operationContext.getConfig()).thenReturn(atomConfig);

        PropertyMap map = new MutablePropertyMap();
        map.put("batchSize", null);
        when(operationContext.getOperationProperties()).thenReturn(map);

        Map<String, Object> output = mongoDBConnectorConnection.prepareInputConfig(operationContext, logger);

        assertNotNull(output);
        assertEquals(3, output.size());
    }

    @Test
    public void testFormatInputForStringType() throws MongoDBConnectException {
        Object returnVal = mongoDBConnectorConnection.formatInputForStringType("object", "63e4b15f9aa89e7c4168ba51");
        assertEquals("63e4b15f9aa89e7c4168ba51", returnVal.toString());
    }

    @Test
    public void testformatInputForNullType() {
        Object returnVal = mongoDBConnectorConnection.formatInputForNullType("null", "null");
        assertEquals(null, returnVal);
    }

    @Test
    public void testformatInputForDoubleType() {
        Object returnVal = mongoDBConnectorConnection.formatInputForDoubleType("double", "12345.6789");
        assertEquals("12345.6789", returnVal.toString());
    }

    @Test
    public void testformatInputForDecimal128Type() {
        Object returnVal = mongoDBConnectorConnection.formatInputForDecimal128Type("decimal 128", "9823.1297");
        assertEquals("9823.1297", returnVal.toString());
    }

    @Test
    public void testformatInputForLongType() {
        Object returnVal = mongoDBConnectorConnection.formatInputForLongType("long", "20908458868678099");
        assertEquals("20908458868678099", returnVal.toString());
    }

    @Test
    public void testformatInputForNumberType() {
        Object returnVal = mongoDBConnectorConnection.formatInputForNumberType("integer", "15");
        assertEquals("15", returnVal.toString());
    }

    @Test
    public void testformatInputForBooleanType() {
        Object returnVal = mongoDBConnectorConnection.formatInputForBooleanType("boolean", "true");
        assertEquals("true", returnVal.toString());
    }

    private void inputForDoQuery() {
        List<String> fields = Arrays.asList("_id", EMAIL, ADDRESS, NAME);
        List<Bson> projectionConfig = Arrays.asList(Projections.include(fields));

        bsonprojection = Projections.fields(projectionConfig);
        bsonFilter = Filters.eq("name", "ridhi");
        sortKeys = null;

        FindIterable iter = mock(FindIterable.class);
        MongoCursor<Document> mongDoc = mock(MongoCursor.class);
        FindIterable<Document> resul = mock(FindIterable.class);

        mongoDBConnectorConnection = new MongoDBConnectorConnection(browseContext);
        mongoDBConnectorConnection.setCollection(mongoCollection);

        when(browseContext.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"1");
        when(DocumentUtil.getSizeLimitQuery(anyLong())).thenCallRealMethod();
        when(mongoCollection.find((Bson) any())).thenReturn(resul);
        when(mongoCollection.find()).thenReturn(resul);
        when(resul.batchSize(anyInt())).thenReturn(iter);
        when(iter.iterator()).thenReturn(mongDoc);
        when(DocumentUtil.parseLongOrDefault(anyString(),anyLong())).thenCallRealMethod();
    }

    @Test
    public void testdoQuery() {
        inputForDoQuery();

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, bsonFilter, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    @Test
    public void testdoQueryWithoutFilter() {
        inputForDoQuery();

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, null, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests querying when the maximum document size is negative.
     */
    @Test
    public void testdoQueryWithMaxDocumentNegativeSize() {
        inputForDoQuery();
        when(browseContext.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"-1");

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, bsonFilter, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests querying when the maximum document size is negative with an empty filter.
     */
    @Test
    public void testdoQueryWithMaxDocumentNegativeSizeWithEmptyFilter() {
        inputForDoQuery();
        when(browseContext.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"-1");

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, null, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests querying with a specific maximum document size.
     */
    @Test
    public void testdoQueryWithMaxDocumentSize() {
        inputForDoQuery();
        when(browseContext.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"10");

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, bsonFilter, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests querying with a specific maximum document size and an empty filter.
     */
    @Test
    public void testdoQueryWithMaxDocumentSizeWithEmptyFilter() {
        inputForDoQuery();
        when(browseContext.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"1677216");

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, null, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests querying with a maximum document size specified by an invalid property.
     */
    @Test
    public void testdoQueryWithMaxDocumentWithInvalidProperty() {
        inputForDoQuery();
        when(browseContext.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"0a");

        MongoCursor<Document> mongCur = mongoDBConnectorConnection.doQuery(COLLECTION_NAME, bsonFilter, bsonprojection,
                sortKeys, 1);

        assertNotNull(mongCur);
        verify(mongoCollection, times(1)).find((Bson) any());
    }
}