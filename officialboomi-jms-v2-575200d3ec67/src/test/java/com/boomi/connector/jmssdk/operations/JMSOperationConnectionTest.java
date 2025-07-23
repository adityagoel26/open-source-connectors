// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.JMSTestContext;

import org.junit.Test;

public class JMSOperationConnectionTest {

    @Test(expected = ConnectorException.class)
    public void batchSizeCannotBeLessThanOneTest() {
        JMSTestContext context =
                new JMSTestContext.Builder().withGenericService().withPoolDisabled().withVersion1().withSendOperation()
                        .build();
        context.addOperationProperty("transaction_batch_size", 0L);
        JMSOperationConnection connection = new JMSOperationConnection(context);

        connection.getTransactionBatchSize();
    }

    @Test(expected = ConnectorException.class)
    public void batchSizeCannotBeGreaterThanIntMaxValueTest() {
        JMSTestContext context =
                new JMSTestContext.Builder().withGenericService().withPoolDisabled().withVersion1().withSendOperation()
                        .build();
        context.addOperationProperty("transaction_batch_size", Integer.MAX_VALUE + 1L);
        JMSOperationConnection connection = new JMSOperationConnection(context);

        connection.getTransactionBatchSize();
    }
}
