// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.util;

import io.swagger.v3.oas.models.PathItem;
import com.boomi.connector.api.ConnectorException;

/**
 * Value object modeling the OpenAPI action being executed. This object contains the endpoint path, HTTP method and
 * ID of the action.
 */
public class OpenAPIAction {

    private static final String SEPARATOR = "::";
    private static final int EXPECTED_AMOUNT_OF_ELEMENTS_IN_OBJECT_TYPE = 2;

    private final String _path;
    private final PathItem.HttpMethod _method;
    private final String _id;

    public OpenAPIAction(String objectTypeId) {
        String[] parts = objectTypeId.split(SEPARATOR);
        if (parts.length != EXPECTED_AMOUNT_OF_ELEMENTS_IN_OBJECT_TYPE) {
            throw new ConnectorException("unexpected object type id value: " + objectTypeId);
        }
        _id = objectTypeId;
        _method = PathItem.HttpMethod.valueOf(parts[0]);
        _path = parts[1];
    }

    public OpenAPIAction(PathItem.HttpMethod method, String path) {
        _method = method;
        _path = path;
        _id = method + SEPARATOR + path;
    }

    public String getId() {
        return _id;
    }

    public PathItem.HttpMethod getMethod() {
        return _method;
    }

    public String getPath() {
        return _path;
    }
}
