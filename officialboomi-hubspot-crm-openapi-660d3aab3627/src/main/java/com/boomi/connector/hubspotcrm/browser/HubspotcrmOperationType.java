// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.browser;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.OperationType;
import com.boomi.util.StringUtil;

/**
 * Represents the operation types for the HubSpot CRM connector.
 * This enum defines the various operations that can be performed
 * within the HubSpot CRM integration.
 */
public enum HubspotcrmOperationType {

    DELETE("DELETE"),
    CREATE("POST"),
    TEST_CONNECTION(StringUtil.EMPTY_STRING),
    UPDATE("PATCH"),
    GET("GET"),
    QUERY("QUERY");

    private final String _operationTypeId;

    HubspotcrmOperationType(String operationTypeId) {
        _operationTypeId = operationTypeId;
    }

    /**
     * Gets the operation type identifier.
     *
     * @return The operation type identifier as a String
     */
    public String getOperationTypeId() {
        return _operationTypeId;
    }

    /**
     * Get the {@link HubspotcrmOperationType} from the given context
     *
     * @param context to extract the operation type
     * @return the {@link HubspotcrmOperationType}
     */
    public static HubspotcrmOperationType from(BrowseContext context) {
        String customOperationType = context.getCustomOperationType();
        if (context.getOperationType() == OperationType.GET && StringUtil.isBlank(customOperationType)) {
            return TEST_CONNECTION;
        }
        for (HubspotcrmOperationType value : values()) {
            if (value._operationTypeId.equals(customOperationType)) {
                return value;
            }
        }
        throw new IllegalArgumentException("unexpected operation type: " + customOperationType);
    }
}