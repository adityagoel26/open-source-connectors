// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2CUDController;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2Controller;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2QueryController;

public class BulkManager {

    private static final int INITIAL_WAIT_TIME = 1500;
    private static final int MAX_NUMBER_OF_RETRIES = 50;
    private static final int MAXIMUM_WAIT_TIME_AFTER_SINGLE_CHECK = 60000;
    private static final int AMOUNT_OF_TIME_TO_INCREMENT = 1000;

    private BulkManager() {
    }

    /**
     * Waits for salesforce to finish Bulk CUD processing
     *
     * @param controller used to check if Salesforce finished job processing
     */
    public static void waitCUD(BulkV2CUDController controller) {
        waitForBulkProcessing(controller);
    }

    /**
     * Waits for salesforce to finish Bulk Query processing
     *
     * @param controller used to check if Salesforce finished job processing
     */
    public static void waitQuery(BulkV2QueryController controller) {
        waitForBulkProcessing(controller);
    }

    /**
     * Waits until bulk processing is done, or 21 minutes has elapsed.
     */
    private static void waitForBulkProcessing(BulkV2Controller controller) {
        int waitingTime = INITIAL_WAIT_TIME;
        int safetyCount = MAX_NUMBER_OF_RETRIES;
        do {
            try {
                Thread.sleep(waitingTime);
                waitingTime = Math.min(MAXIMUM_WAIT_TIME_AFTER_SINGLE_CHECK, waitingTime + AMOUNT_OF_TIME_TO_INCREMENT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectorException("the thread was interrupted", e);
            }
        } while (!controller.isFinishedProcessing() && --safetyCount != 0);

        if (safetyCount == 0) {
            throw new ConnectorException("Salesforce is taking too long inProgress state for this bulk operation");
        }
    }
}
