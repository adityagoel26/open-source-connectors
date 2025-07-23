// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.mongodb.util.DocumentUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchDocumentsTest {

    private static final String SERIAL_NUMBER_KEY = "Serial_number";
    private static final String CERTIFICATE_NUMBER_KEY = "certificate_number";
    private static final String NAME_KEY = "name";
    private static final String INPUT =
            "{\r\n" + "\"Serial_number\":\"10057-2015-ENFO\",\r\n" + "\"certificate_number\":\"newdirectory\"\r\n"
                    + "\"name\":\"LD SOLUTIONS\"\r\n" + '}';
    private static final Charset CHARSET_UTF_8 = StandardCharsets.UTF_8;
    private static final int BATCH_SIZE = 1;
    private static final MockedStatic<DocumentUtil> DOCUMENT_UTIL = Mockito.mockStatic(DocumentUtil.class);
    private static final MockedStatic<Document> DOC = Mockito.mockStatic(Document.class);
    private static final Logger LOGGER = mock(Logger.class);
    private final SimpleOperationResponseWrapper simpleOperationResponseWrapper = new SimpleOperationResponseWrapper();
    private Iterable<ObjectData> objDataItr;
    private AtomConfig atomConfig;
    private SimpleOperationResponse operationResponse;
    private SimpleTrackedData trackedData;

    @AfterClass
    public static void tearDown() {
        DOCUMENT_UTIL.close();
        DOC.close();
    }

    private LinkedHashMap<String, Object> createMap(String serialNo, String certificateNo, String name) {
        LinkedHashMap<String, Object> docValueMap = new LinkedHashMap<>();
        docValueMap.put(SERIAL_NUMBER_KEY, serialNo);
        docValueMap.put(CERTIFICATE_NUMBER_KEY, certificateNo);
        docValueMap.put(NAME_KEY, name);
        return docValueMap;
    }

    @Before
    public void setup() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        operationResponse = mock(SimpleOperationResponse.class);
        when(operationResponse.getLogger()).thenReturn(LOGGER);

        atomConfig = mock(AtomConfig.class);
        InputStream result = new ByteArrayInputStream(INPUT.getBytes(CHARSET_UTF_8));
        trackedData = new SimpleTrackedData(1, result);
    }

    @Test
    public void processObjectDataWhenInput() {
        String parsingInput = "{\"Serial_number\":\"10057-2015-ENFO\","
                + "\"certificate_number\":\"newdirectory\"\"name\":\"LD SOLUTIONS\"}";

        DOCUMENT_UTIL.when(() -> DocumentUtil.inputStreamToString(trackedData.getData(), CHARSET_UTF_8)).thenReturn(
                parsingInput);

        Document document = new Document(createMap("10057-2015-ENFO", "newdirectory", "LD SOLUTIONS"));

        DOC.when(() -> Document.parse(parsingInput)).thenReturn(document);

        BatchDocuments batchDocuments = new BatchDocuments(objDataItr, BATCH_SIZE, atomConfig, CHARSET_UTF_8,
                operationResponse, false);

        TrackedDataWrapper trackedDataWrapper = batchDocuments.processObjectData(trackedData, CHARSET_UTF_8, 1);

        assertNotNull("TrackedDataWrapper returned is null", trackedDataWrapper);
        assertEquals("TrackedData returned is not match", trackedData, trackedDataWrapper.getTrackedData());
        assertEquals("Document returned is not match", document, trackedDataWrapper.getDoc());
    }

    @Test
    public void processObjectDataWhenNullInput() {
        String expectedErrorMessage = "Error while parsing JSON record to Document for inputRecord: 1";
        String parsingInput = "{\"Serial_number\":\"10057-2015-ENFO\","
                + "\"certificate_number\":\"newdirectory\"\"name\":\"LD SOLUTIONS\"}";

        DOCUMENT_UTIL.when(() -> DocumentUtil.inputStreamToString(trackedData.getData(), CHARSET_UTF_8)).thenThrow(
                IOException.class);

        LinkedHashMap<String, Object> documentValueMap = createMap("10057-2015-ENFO", "newdirectory", "LD SOLUTIONS");

        DOC.when(() -> Document.parse(parsingInput)).thenReturn(new Document(documentValueMap));

        BatchDocuments batchDocuments = new BatchDocuments(objDataItr, BATCH_SIZE, atomConfig, CHARSET_UTF_8,
                operationResponse, false);

        TrackedDataWrapper trackedDataWrapper = batchDocuments.processObjectData(trackedData, CHARSET_UTF_8, 1);

        String actualErrorMessage = trackedDataWrapper.getErrorDetails().getErrorMessage();

        assertEquals("Error message returned is not same", expectedErrorMessage, actualErrorMessage);
    }

    @Test
    public void testUpdtaeMemoryUsed() throws IOException {
        ObjectData objectData = mock(ObjectData.class);
        BatchDocuments batchDocuments = new BatchDocuments(objDataItr, BATCH_SIZE, atomConfig, CHARSET_UTF_8,
                operationResponse, false);

        Long expectedMemoryUsed = Long.valueOf(1000);

        when(objectData.getDataSize()).thenReturn(expectedMemoryUsed);
        Long actualMemoryUsed = batchDocuments.updateMemUsed(objectData);

        assertEquals("Memory used is not equal to the expected", expectedMemoryUsed, actualMemoryUsed);
    }

    @Test
    public void testUpdtaeMemoryUsedWhenThrowingException() throws IOException {
        trackedData = mock(SimpleTrackedData.class);
        operationResponse = new SimpleOperationResponse();

        simpleOperationResponseWrapper.addTrackedData(trackedData, operationResponse);

        BatchDocuments batchDocuments = new BatchDocuments(objDataItr, BATCH_SIZE, atomConfig, CHARSET_UTF_8,
                operationResponse, false);

        when(trackedData.getDataSize()).thenThrow(IOException.class);
        batchDocuments.updateMemUsed(trackedData);

        assertEquals("Status code is different from expected", "413",
                operationResponse.getResults().get(0).getStatusCode());
        assertEquals("Status message is different from expected", "max size exceeded",
                operationResponse.getResults().get(0).getMessage());
    }
}