// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.constant;

/**
 * Enum representing the three types of Operation APIs in Salesforce REST: Bulk API 2.0, REST and Composite
 */
public enum OperationAPI {
    BULK_V2, REST, COMPOSITE;

    /**
     * Descriptor field ID
     */
    public static final String FIELD_ID = "OperationAPI";

    /**
     * Parse the {@link OperationAPI} from the given string
     *
     * @param id representing the {@link OperationAPI}
     * @return the {@link OperationAPI} enum
     */
    public static OperationAPI from(String id) {
        switch (id) {
            case "BulkAPI2.0":
                return BULK_V2;
            case "RESTAPI":
                return REST;
            case "COMPOSITEAPI":
                return COMPOSITE;
            default:
                throw new IllegalArgumentException("invalid Operation API ID: " + id);
        }
    }
}
