// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutilopensource;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.Connector;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleOperationContext;

import java.util.List;
import java.util.Map;

public class MockOperationContext extends SimpleOperationContext {

    private String _customOperationType;

    public MockOperationContext(Connector connector, OperationType opType, Map<String, Object> connProps,
            Map<String, Object> opProps, String objectTypeId, Map<ObjectDefinitionRole, String> cookies,
            List<String> selectedFields) {
        super(null, connector, opType, connProps, opProps, objectTypeId, cookies, selectedFields);
    }

    @Override
    public String getCustomOperationType() {
        return _customOperationType;
    }

    public void setCustomOperationType(String customOperationType) {
        this._customOperationType = customOperationType;
    }

    public AtomConfig getConfig() {
        return new MockAtomConfig();
    }
}
