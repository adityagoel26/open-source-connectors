// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model;

import com.boomi.connector.jmssdk.client.DestinationType;

public class GenericTargetDestination implements TargetDestination {

    private final String _id;
    private final String _type;
    private final String _dataType;
    private final String _name;
    private DestinationType _destinationType;
    private final boolean _isProfileRequired;

    public GenericTargetDestination(String objectId) {
        _id = objectId;
        _type = null;
        _dataType = null;
        _name = objectId;
        _isProfileRequired = false;
        _destinationType = null;
    }

    @Override
    public String getType() {
        return _type;
    }

    @Override
    public String getDataType() {
        return _dataType;
    }

    @Override
    public String getDestinationName() {
        return getName();
    }

    @Override
    public Boolean isProfileRequired() {
        return _isProfileRequired;
    }

    @Override
    public String getId() {
        return _id;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public DestinationType getDestinationType() {
        return _destinationType;
    }

    @Override
    public void setDestinationType(DestinationType destinationType) {
        _destinationType = destinationType;
    }
}
