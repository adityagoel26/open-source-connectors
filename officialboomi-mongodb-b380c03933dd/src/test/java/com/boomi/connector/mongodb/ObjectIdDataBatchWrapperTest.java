// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.RequestUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ObjectIdDataBatchWrapperTest {

    private static Logger logger = mock(Logger.class);
    private static MockedStatic<RequestUtil> requestUtilMockedStatic = Mockito.mockStatic(RequestUtil.class);
    private Iterable<ObjectIdData> objDataItr;
    private AtomConfig atomConfig;
    private OperationResponse response;

    @Before
    public void setup() {
        atomConfig = mock(AtomConfig.class);
        response = mock(SimpleOperationResponse.class);
        objDataItr = mock(Iterable.class);
    }

    @Test
    public void testObjectIdDataBatchWrapperNextMethod() {
        int batchSize = 1;

        Iterable<List<ObjectIdData>> iterable = mock(Iterable.class);
        Iterator<List<ObjectIdData>> iterator = mock(Iterator.class);
        ArrayList<ObjectIdData> nextBatch = mock(ArrayList.class);
        ObjectIdData input = mock(ObjectIdData.class);

        when(iterator.next()).thenReturn(nextBatch);
        when(nextBatch.size()).thenReturn(1);
        when(nextBatch.get(anyInt())).thenReturn(input);
        when(input.getObjectId()).thenReturn("63ee3e6abba17705ffac62ac");
        when(response.getLogger()).thenReturn(logger);
        requestUtilMockedStatic.when(
                        () -> RequestUtil.pageIterable((Iterable<Object>) Mockito.any(), anyInt(),
                                Mockito.<AtomConfig>any()))
                .thenReturn(iterable);
        when(iterable.iterator()).thenReturn(iterator);

        ObjectIdDataBatchWrapper objectIdDataBatchWrapper = new ObjectIdDataBatchWrapper(objDataItr, batchSize,
                response, atomConfig);

        List<TrackedDataWrapper> output = objectIdDataBatchWrapper.next();

        assertNotNull(output);
        assertEquals(1, output.size());
    }

    @Test
    public void testgetNumberParser() {
        int batchSize = 1;
        ObjectIdDataBatchWrapper objectIdDataBatchWrapper = new ObjectIdDataBatchWrapper(objDataItr, batchSize,
                response, atomConfig);

        assertNotNull(objectIdDataBatchWrapper.getNumberParser());
    }
}
