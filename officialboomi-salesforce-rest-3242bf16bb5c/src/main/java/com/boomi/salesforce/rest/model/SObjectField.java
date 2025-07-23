// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.model;


import org.apache.commons.lang3.StringUtils;

/**
 * Model represents Salesforce field name and its type
 */
public class SObjectField {
    private final String _fieldName;
    private final String _fieldType;
    private final int _precision;
    private final int _scale;

    public SObjectField(String name, String type) {
        _fieldName = name;
        _fieldType = type;
        _precision = 0;
        _scale = 0;
    }

    public SObjectField(String name, String type, int precision, int scale) {
        _fieldName = name;
        _fieldType = type;
        _precision = precision;
        _scale = scale;
    }

    public String getName() {
        return _fieldName;
    }

    public String getType() {
        return _fieldType;
    }


    public int getScale() {
        return _scale;
    }

    public int getIntegerPlaces() {
        return _precision - _scale;
    }

    public String getPlatformFormat() {
        String ret = StringUtils.repeat("#", getIntegerPlaces()) + "." + StringUtils.repeat("#", getScale());
        if (ret.equals(".")) {
            return null;
        }
        return ret;
    }
}
