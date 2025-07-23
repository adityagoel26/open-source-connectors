// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.mongodb.bean.OutputDocument;
import com.boomi.connector.mongodb.util.MongoDBConnectorPayloadUtil;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoDBConnectorPayloadUtilTest {

    @Test
    public void testGetPaylaod() {
        TrackedDataWrapper trackedDataWrapper = mock(TrackedDataWrapper.class);
        Document doc = mock(Document.class);
        ObjectId objectId = mock(ObjectId.class);

        when(trackedDataWrapper.getDoc()).thenReturn(doc);
        when(doc.get(anyString())).thenReturn(objectId);

        when(objectId.toString()).thenReturn("63eb6b31cdcc146e67a7df84");

        OutputDocument od = new OutputDocument(OperationStatus.SUCCESS, trackedDataWrapper);

        assertNotNull(MongoDBConnectorPayloadUtil.toPayload(od));
    }
}
