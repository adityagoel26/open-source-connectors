// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.testutil.SimpleAtomConfig;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDBConnectorConnectionExtTest {

    private static final String COLL_NAME = "initial-test";
    private static final String ID = "63e4b15f9aa89e7c4168ba51";
    private MongoDBConnectorConnection mongoDBConnectorConnection;
    private FindIterable<Document> results;
    private BrowseContext context = mock(BrowseContext.class);
    private MongoCollection<Document> mongoCollection;
    private PropertyMap connectionProperty;
    private FindIterable<Document> setProjResults;
    private Document doc;
    private final SimpleAtomConfig atomConfig = new SimpleAtomConfig();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        connectionProperty = mock(PropertyMap.class);
        results = mock(FindIterable.class);
        mongoCollection = mock(MongoCollection.class);
        setProjResults = mock(FindIterable.class);
        doc = mock(Document.class);

        when(context.getConnectionProperties()).thenReturn(connectionProperty);
        when(connectionProperty.getProperty(anyString())).thenReturn("mongoDB");
        when(mongoCollection.find((Bson) any())).thenReturn(results);
        when(results.projection(any())).thenReturn(setProjResults);
        when(setProjResults.first()).thenReturn(doc);

        mongoDBConnectorConnection = new MongoDBConnectorConnection(context);
        mongoDBConnectorConnection.setCollection(mongoCollection);
        when(context.getConfig()).thenReturn(atomConfig);
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"1");
    }

    @Test
    public void testFindDocumentById() throws MongoDBConnectException {
        String projField = "projField";

        Document resultDoc = MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, ID,
                projField, null);

        assertNotNull(resultDoc);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    @Test
    public void testFindDocumentByIdDocNotFound() throws MongoDBConnectException {
        String projField = null;

        expectedException.expect(MongoDBConnectException.class);
        expectedException.expectMessage("id is not in collection");

        MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, ID, projField, null);
    }

    @Test
    public void testFindDocumentByIdGetNonHexaDecDocument() throws MongoDBConnectException {
        String projField = "projectionField";

        String obId = "{\r\n" + "\"_id\":\"10057-2015\",\r\n" + "\"certificate_number\":\"newdirectory\"\r\n"
                + "\"name\":\"LD SOLUTIONS\"\r\n" + '}';

        Document resultDoc = MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, obId,
                projField, null);

        assertNotNull(resultDoc);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests the behavior of finding a document by ID with valid document fields.
     *
     * @throws MongoDBConnectException if there's an issue connecting to MongoDB
     */
    @Test
    public void testFindDocumentByIdValidDocumnet() throws MongoDBConnectException {
        String projField = "projectionField";
        String obId = "65c1d256bc62185cd792e3aa";
        Document doc = new Document();
        doc.append("name", "john");
        doc.append("age", 25);
        doc.append("mail", "sample@gmail.com");

        when(setProjResults.first()).thenReturn(doc);
        Document resultDoc = MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, obId,
                projField, null);

        assertNotNull(resultDoc);
        String expectedJson = "{\"name\": \"john\", \"age\": 25, \"mail\": \"sample@gmail.com\"}";
        assertEquals(expectedJson, resultDoc.toJson());
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests the behavior of finding a document by ID when the maximum document size is negative.
     *
     * @throws MongoDBConnectException if there's an issue connecting to MongoDB
     */
    @Test
    public void testFindDocumentByIdWithNegativeDocumentSize() throws MongoDBConnectException {
        String projField = "projField";
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"-1");
        Document resultDoc = MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, ID,
                projField, null);

        assertNotNull(resultDoc);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests finding a document by an invalid ID with a negative document size configuration.
     *
     * @throws MongoDBConnectException if there's an issue connecting to MongoDB
     */
    @Test
    public void testFindDocumentByInvalidIdWithNegativeDocumentSize() throws MongoDBConnectException {
        String projField = "projField";
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"-1");
        String obId = "{\r\n" + "\"_id\":\"10057-2015\",\r\n" + "\"certificate_number\":\"newdirectory\"\r\n"
                + "\"name\":\"LD SOLUTIONS\"\r\n" + '}';

        Document resultDoc = MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, obId,
                projField, null);

        assertNotNull(resultDoc);
        verify(mongoCollection, times(1)).find((Bson) any());
    }

    /**
     * Tests finding a document by ID when encountering a parser error for document size configuration.
     *
     * @throws MongoDBConnectException if there's an issue connecting to MongoDB
     */
    @Test
    public void testFindDocumentByIdWithParserError() throws MongoDBConnectException {
        String projField = "projField";
        atomConfig.withContainerProperty(MongoDBConstants.MAX_DOCUMENT_SIZE_PROPERTY_KEY,"0a");
        Document resultDoc = MongoDBConnectorConnectionExt.findDocumentById(mongoDBConnectorConnection, COLL_NAME, ID,
                projField, null);

        assertNotNull(resultDoc);
        verify(mongoCollection, times(1)).find((Bson) any());
    }
}
