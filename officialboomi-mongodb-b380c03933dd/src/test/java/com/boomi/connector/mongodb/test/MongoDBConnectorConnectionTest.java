//Copyright (c) 2022 Boomi, LP
package com.boomi.connector.mongodb.test;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.bean.ErrorDetails;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.DATABASE;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoDBConnectorConnectionTest {

    private FindIterable findIterable = mock(FindIterable.class);
    private FindIterable<Document> query = mock(FindIterable.class);

    @Test
    public void testProcessQueryError() {
        int errorCode = 100;
        String errorMessage = "Unexpected Mongo Exception";
        MongoException exception = new MongoException(errorCode, errorMessage);
        BrowseContext context = mock(BrowseContext.class);
        PropertyMap connectionProperties = mock(PropertyMap.class);

        when(connectionProperties.getProperty(DATABASE)).thenReturn("mongo");
        when(context.getConnectionProperties()).thenReturn(connectionProperties);

        MongoDBConnectorConnection connectorConnection = new MongoDBConnectorConnection(context);
        ErrorDetails errorDetails = connectorConnection.processQueryError(exception);

        assertEquals(errorCode, (int) errorDetails.getErrorCode());
        assertEquals("This method shouldn't throw an error when Expected ProcessError is not same",
                errorMessage, errorDetails.getErrorMessage());
    }
    @Test
    public void testProcessQueryErrorWithNull() {
        PropertyMap propertyMap = mock(PropertyMap.class);
        BrowseContext context = mock(BrowseContext.class);

        when(context.getConnectionProperties()).thenReturn(propertyMap);
        when(propertyMap.getProperty(MongoDBConstants.DATABASE)).thenReturn("build_on");

        MongoDBConnectorConnection connectorConnection = new MongoDBConnectorConnection(context);
        ErrorDetails errorDetails = connectorConnection.processQueryError(null);

        assertNull("This method shouldn't throw an error when Expected ErrorDetails is not same", errorDetails);
    }
    @Test
    public void testSetProjectionsQuery() {
        PropertyMap propertyMap = mock(PropertyMap.class);
        BrowseContext context = mock(BrowseContext.class);

        when(context.getConnectionProperties()).thenReturn(propertyMap);
        when(propertyMap.getProperty(MongoDBConstants.DATABASE)).thenReturn("build_onConnector");

        MongoDBConnectorConnection connectorConnection = new MongoDBConnectorConnection(context);
        FindIterable<Document> projectionQuery = connectorConnection.setProjectionsInQuery(query, null);

        assertNotNull("This method shouldn't throw an error when projectionQuery details are not same",projectionQuery);

    }
    @Test
    public void testSetProjectionsQueryWithDataType() {
        PropertyMap propertyMap = mock(PropertyMap.class);
        BrowseContext context = mock(BrowseContext.class);

        when(findIterable.projection(ArgumentMatchers.<Bson>any())).thenReturn(findIterable);
        when(context.getConnectionProperties()).thenReturn(propertyMap);
        when(propertyMap.getProperty(MongoDBConstants.DATABASE)).thenReturn("build_on1");

        MongoDBConnectorConnection connectorConnection = new MongoDBConnectorConnection(context);

        FindIterable<Document> projectionQuery = connectorConnection.setProjectionsInQuery(findIterable, "hello");

        assertNotNull(projectionQuery);
        assertNotNull("When Projection Query not matches",projectionQuery.projection(ArgumentMatchers.<Bson>any()));

    }
    @Test
    public void testProcessErrorForGet() {
        int errorCode = 100;
        String errorMessage = "Unexpected Mongo Exception";
        MongoException exception = new MongoException(errorCode, errorMessage);
        BrowseContext context = mock(BrowseContext.class);
        PropertyMap connectionProperties = mock(PropertyMap.class);

        when(connectionProperties.getProperty(DATABASE)).thenReturn("mongo");
        when(context.getConnectionProperties()).thenReturn(connectionProperties);

        MongoDBConnectorConnection connectorConnection = new MongoDBConnectorConnection(context);
        ErrorDetails errorDetails = connectorConnection.processErrorForGet(exception);

        assertEquals(errorCode, (int) errorDetails.getErrorCode());
        assertEquals("This method shouldn't throw an error when Expected ProcessError is not same",
                errorMessage, errorDetails.getErrorMessage());
    }
    @Test
    public void testProcessErrorForGetWithDataType() {
        PropertyMap propertyMap = mock(PropertyMap.class);
        BrowseContext context = mock(BrowseContext.class);

        when(context.getConnectionProperties()).thenReturn(propertyMap);
        when(propertyMap.getProperty(MongoDBConstants.DATABASE)).thenReturn("build_on2");

        MongoDBConnectorConnection connectorConnection = new MongoDBConnectorConnection(context);
        ErrorDetails errorDetails = connectorConnection.processErrorForGet(null);

        assertNull("Return value expected is null but it's not", errorDetails);
    }
}

