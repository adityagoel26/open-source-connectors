// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OperationType;
import com.boomi.util.StringUtil;

/**
 * Enum representing the different Operation Types supported by the connector
 */
public enum VeevaOperationType {

    UPDATE("PUT"), QUERY("QUERY"), CREATE("POST"), EXECUTE("OPEN_API"), TEST_CONNECTION(StringUtil.EMPTY_STRING);

    private final String _operationTypeId;

    VeevaOperationType(String operationTypeId) {
        _operationTypeId = operationTypeId;
    }

    /**
     * Get the {@link VeevaOperationType} from the given context
     *
     * @param context to extract the operation type
     * @return the {@link VeevaOperationType}
     */
    public static VeevaOperationType from(BrowseContext context) {
        if (context.getOperationType() == OperationType.GET && StringUtil.isBlank(context.getCustomOperationType())) {
            return TEST_CONNECTION;
        }

        if (context.getOperationType() == OperationType.QUERY) {
            return QUERY;
        }

        for (VeevaOperationType value : values()) {
            if (value._operationTypeId.equals(context.getCustomOperationType())) {
                return value;
            }
        }

        throw new IllegalArgumentException("unexpected operation type: " + context.getCustomOperationType());
    }
}
