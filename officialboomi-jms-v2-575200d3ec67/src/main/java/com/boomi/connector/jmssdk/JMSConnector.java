// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.get.JMSGetOperation;
import com.boomi.connector.jmssdk.operations.listen.JMSListenOperation;
import com.boomi.connector.jmssdk.operations.send.JMSSendOperation;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.util.listen.UnmanagedListenConnector;
import com.boomi.connector.util.listen.UnmanagedListenOperation;

public class JMSConnector extends UnmanagedListenConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        JMSConstants.ServerType serverType = JMSConstants.ServerType.valueOf(
                context.getConnectionProperties().getProperty(JMSConstants.PROPERTY_SERVER_TYPE));
        if (JMSConstants.ServerType.ORACLE_AQ == serverType) {
            return new JMSAQBrowser(new JMSConnection<>(context));
        } else {
            return new JMSBrowser(new JMSConnection<>(context));
        }
    }

    @Override
    public Operation createCreateOperation(OperationContext context) {
        return new JMSSendOperation(new JMSOperationConnection(context));
    }

    @Override
    public Operation createQueryOperation(OperationContext context) {
        return new JMSGetOperation(new JMSOperationConnection(context));
    }

    @Override
    public UnmanagedListenOperation createListenOperation(OperationContext context) {
        return new JMSListenOperation(new JMSOperationConnection(context));
    }
}
