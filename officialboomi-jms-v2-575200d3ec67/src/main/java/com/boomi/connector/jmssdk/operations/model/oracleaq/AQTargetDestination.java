// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.util.StringUtil;

public class AQTargetDestination implements TargetDestination {

    private static final String JMS_OBJECT_PREFIX = "SYS.AQ$";
    private static final String RAW_TYPE = "RAW";
    private static final int POS_TYPE = 0;
    private static final int POS_DATA_TYPE = 1;
    private static final int POS_NAME = 2;
    private static final int ORACLEAQ_SECTION_NUMBER = 3;

    private final String _id;
    private final String _type;
    private final String _dataType;
    private final String _name;
    private final boolean _isProfileRequired;
    private DestinationType _destinationType;

    public AQTargetDestination(String objectTypeId) {
        String[] fields = objectTypeId.split(":");
        if (fields.length < ORACLEAQ_SECTION_NUMBER) {
            throw new IllegalArgumentException("Object Id bad formatting");
        }
        _id = objectTypeId;
        _type = fields[POS_TYPE];
        _dataType = fields[POS_DATA_TYPE];
        _name = fields[POS_NAME];
        _isProfileRequired = isRequired();
    }

    private boolean isRequired() {
        return StringUtil.isNotBlank(_dataType) && !RAW_TYPE.equals(_dataType) && !_dataType.startsWith(
                JMS_OBJECT_PREFIX);
    }

    public AQTargetDestination(String name, String type, String dataType) {
        _id = null;
        _name = name;
        _type = type;
        _dataType = dataType;
        _isProfileRequired = isRequired();
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
        return getType() + ":" + getName();
    }

    @Override
    public Boolean isProfileRequired() {
        return _isProfileRequired;
    }

    @Override
    public String getId() {
        return _id;
    }

    public String getName() {
        return _name;
    }

    @Override
    public DestinationType getDestinationType() {
        if (_destinationType == null) {
            switch (_dataType) {
                case "SYS.AQ$_JMS_TEXT_MESSAGE":
                    _destinationType = DestinationType.TEXT_MESSAGE;
                    break;
                case "SYS.AQ$_JMS_BYTES_MESSAGE":
                case "SYS.AQ$_JMS_STREAM_MESSAGE":
                case "RAW":
                    _destinationType = DestinationType.BYTE_MESSAGE;
                    break;
                case "SYS.AQ$_JMS_MAP_MESSAGE":
                    _destinationType = DestinationType.MAP_MESSAGE;
                    break;
                default:
                    _destinationType = DestinationType.ADT_MESSAGE;
                    break;
            }
        }
        return _destinationType;
    }

    @Override
    public void setDestinationType(DestinationType destinationType) {
        _destinationType = destinationType;
    }
}
