// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2;

import com.boomi.salesforce.rest.controller.bulkv2.BulkV2CUDController;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2QueryController;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkManagerTest {

    private static final long MINIMUM_WAIT_TIME = 1500L;

    @Test
    void waitCUD() {
        BulkV2CUDController controller = mock(BulkV2CUDController.class, Mockito.RETURNS_DEEP_STUBS);
        when(controller.isFinishedProcessing()).thenReturn(true);

        long start = System.currentTimeMillis();
        BulkManager.waitCUD(controller);

        assertTrue((System.currentTimeMillis() - start) >= MINIMUM_WAIT_TIME,
                "it should wait at least 1500 milliseconds");
    }

    @Test
    void waitQuery() {
        BulkV2QueryController controller = mock(BulkV2QueryController.class, Mockito.RETURNS_DEEP_STUBS);
        when(controller.isFinishedProcessing()).thenReturn(true);

        long start = System.currentTimeMillis();
        BulkManager.waitQuery(controller);

        assertTrue((System.currentTimeMillis() - start) >= MINIMUM_WAIT_TIME,
                "it should wait at least 1500 milliseconds");
    }
}
