// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

// This file is to declare required constants for the Hubspot CRM connector.
public final class HubspotcrmConstant {

    private HubspotcrmConstant() {
        // No instances required.
    }

    public static final String VERSION = "version";

    // API Response Fields
    public static final String RESULTS = "results";
    public static final String PROPERTIES = "properties";

    // Query Parameters
    public static final String LIMIT = "limit";
    public static final String AFTER = "after";
    public static final Integer QUERY_LIMIT = 200;

    // Filter Related Constants
    public static final String FILTER_GROUPS = "filterGroups";
    public static final String FILTERS = "filters";
    public static final String PROPERTY_NAME = "propertyName";
    public static final String OPERATOR = "operator";
    public static final String VALUE = "value";
    public static final String VALUES = "values";
    public static final String HIGH_VALUE = "highValue";

    // Operator Constants
    public static final String BETWEEN = "BETWEEN";
    public static final String IN = "IN";
    public static final String NOT_IN = "NOT_IN";
    public static final String HAS_PROPERTY = "HAS_PROPERTY";
    public static final String NOT_HAS_PROPERTY = "NOT_HAS_PROPERTY";

    // Sort Related Constants
    public static final String SORTS = "sorts";
    public static final String DIRECTION = "direction";

    // Delimiters
    public static final String COMMA = ",";
    public static final String SLASH = "/";

    public static final int EXPECTED_AMOUNT_OF_ELEMENTS_IN_OBJECT_TYPE = 2;
    public static final String CONNECTOR_STATUS_ERROR_CODE = "-1";
    public static final int ZERO = 0;
    public static final int ONE = 1;
    public static final int NEGATIVE_ONE = -1;
    public static final int TWO = 2;
    public static final int MAX_QUERY_LIMIT = 10000;

    // Error Messages
    public static final String MISSING_LIMIT_VALUE_ERROR = "Missing maximum documents value";
    public static final String EXCEEDS_MAX_LIMIT_ERROR = "Limit value cannot exceed " + MAX_QUERY_LIMIT;

    //Sort Field message
    public static final String SORT_FIELD_SUPPORT_MSG = "Only one field supported for sorting.";
    public static final String PROPERTY_EMPTY_MSG = "property cannot be empty.";
    public static final String PROPERTY_FORMAT_MSG = "Invalid property format.";
    public static final String BETWEEN_VALUE_FORMAT_MSG = "Invalid between value format.";
    public static final String SORT_FIELD_SPECIFICATION_MSG = "Invalid sort field specification:";
    public static final String QUERY_PARAMETER_FAILED_MSG = "Failed to create query parameters.";
    public static final String SEARCH_OPERATION_FAILED_MSG = "Failed executing search operation.";
    public static final String ARGUMENTS_EMPTY_ERROR = "Argument cannot be null or empty";

    //HTTP Methods
    public static final String GET = "GET";
}
