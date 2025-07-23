// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSSender;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.send.strategy.SendStrategy;
import com.boomi.connector.jmssdk.pool.AdapterPool;
import com.boomi.connector.jmssdk.pool.AdapterPoolManager;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;

import javax.jms.Session;

/**
 * JMS Send Operation
 */
public class JMSSendOperation extends BaseUpdateOperation {

    private final int _transactionalMode;
    private final int _transactionBatchSize;
    private final long _documentSizeThreshold;

    public JMSSendOperation(JMSOperationConnection conn) {
        super(conn);
        _transactionalMode = conn.getTransactionMode();
        _transactionBatchSize = conn.getTransactionBatchSize();
        _documentSizeThreshold = conn.getDocumentSizeThreshold();
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        AdapterPool adapterPool = AdapterPoolManager.getPool(getConnection());

        GenericJndiBaseAdapter adapter = null;
        JMSSender sender = null;
        try {
            adapter = adapterPool.createAdapter();
            sender = adapter.createSender(_transactionalMode);
            SendStrategy sendStrategy = getSendStrategy(adapter, sender);
            sendStrategy.send(request, response, getContext().getObjectTypeId());
        } finally {
            IOUtil.closeQuietly(sender);
            adapterPool.releaseAdapter(adapter);
        }
    }

    private SendStrategy getSendStrategy(GenericJndiBaseAdapter adapter, JMSSender sender) {
        if (Session.SESSION_TRANSACTED == _transactionalMode) {
            return SendStrategy.transactedStrategy(adapter, sender, _documentSizeThreshold, _transactionBatchSize);
        } else {
            return SendStrategy.simpleStrategy(adapter, sender, _documentSizeThreshold);
        }
    }

    @Override
    public JMSOperationConnection getConnection() {
        return (JMSOperationConnection) super.getConnection();
    }
}
