// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectType;
import com.boomi.util.LogUtil;
import com.boomi.util.NumberUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

public enum GoogleBqObjectType {
    QUERY("Query"), LOAD("Load"), EXTRACT("Extract"), COPY("Copy");
    private static final Logger LOG = LogUtil.getLogger(GoogleBqObjectType.class);
    private final String _label;

    GoogleBqObjectType(String label) {
        _label = label;
    }

    /**
     * Returns a {@link ObjectType} with the values constant from this class.
     *
     * @return an instance of {@link ObjectType} using the constant values
     */
    public ObjectType toObjectType() {
        return new ObjectType().withId(name()).withLabel(_label);
    }

    /**
     * Returns the {@link GoogleBqObjectType} constant corresponding to the provided object type ID.
     *
     * @param objectTypeId
     * @return the corresponding enum constant or <code>null</code> if there is no constant matching the provided Object
     * Type ID
     */
    public static GoogleBqObjectType fromString(String objectTypeId) {
        try {
            return NumberUtil.toEnum(GoogleBqObjectType.class, objectTypeId);
        } catch (IllegalArgumentException e) {
            LOG.log(Level.WARNING, "Incorrect Object Type ID", e);
            throw new ConnectorException("Unsupported Job type");
        }
    }
}
