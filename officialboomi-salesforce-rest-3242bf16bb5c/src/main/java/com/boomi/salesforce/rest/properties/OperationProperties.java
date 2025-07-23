// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.properties;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.TrackedData;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.constant.OperationAPI;
import com.boomi.util.NumberUtil;
import com.boomi.util.ObjectUtil;
import com.boomi.util.StringUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple helper class that contains the properties needed for execute an operation
 */
public class OperationProperties {

    private static final int MAX_BATCH_SIZE = 150;
    private static final int MAX_BATCH_COUNT = 200;
    private static final long DEFAULT_BATCH_COUNT = 200L;
    private static final long DEFAULT_BATCH_SIZE = 100L;
    private static final long MB_TO_BYTES_FACTOR = 1000000L;
    private static final long QUERY_UNLIMITED_DOCUMENTS = -1L;

    private final PropertyMap _operationProperties;
    private final Map<String, String> _extraProperties;
    private Map<String, String> _restHeaders;

    public OperationProperties(PropertyMap operationProperties, String operationName) {
        _operationProperties = operationProperties;
        _extraProperties = new HashMap<>();

        if (Constants.CREATE_TREE_CUSTOM_DESCRIPTOR.equals(operationName)) {
            setBoomiOperation(OperationType.CREATE.name());
        } else {
            setBoomiOperation(operationName);
        }
    }

    /**
     * @return the assignment rule ID property
     */
    public String getAssignmentRuleId() {
        String ret = _extraProperties.get(Constants.ASSIGNMENT_RULE_ID_DESCRIPTOR);
        if (StringUtil.isBlank(ret)) {
            return null;
        }
        return ret;
    }

    public String getColumnDelimiter() {
        String ret = _operationProperties.getProperty(Constants.COLUMN_DELIMITER_BULKV2);
        if (StringUtil.isBlank(ret)) {
            return null;
        }
        return ret;
    }

    public String getExternalIdFieldName() {
        String ret = _operationProperties.getProperty(Constants.EXTERNAL_ID_FIELD_BULKV2);
        if (StringUtil.isBlank(ret)) {
            return null;
        }
        return ret;
    }

    public String getExternalIdValue() {
        return ObjectUtil.defaultIfNull(_extraProperties.get(Constants.EXTERNAL_ID_VALUE), StringUtil.EMPTY_STRING);
    }

    public String getLineEnding() {
        String ret = _operationProperties.getProperty(Constants.LINE_ENDING_BULKV2);
        if (StringUtil.isBlank(ret)) {
            return null;
        }
        return ret;
    }

    public String getSObject() {
        String manuallySetObject = _extraProperties.get(Constants.OBJECT_SOBJECT);
        if (StringUtil.isNotBlank(manuallySetObject)) {
            return manuallySetObject;
        }
        String ret = _operationProperties.getProperty(Constants.OBJECT_SOBJECT);
        if (StringUtil.isBlank(ret)) {
            return null;
        }
        return ret;
    }

    public void setSObject(String sobjectID) {
        _extraProperties.put(Constants.OBJECT_SOBJECT, sobjectID);
    }

    public String getBulkOperation() {
        String manuallySetOperation = _extraProperties.get(Constants.OPERATION_BULKV2);
        if (StringUtil.isNotBlank(manuallySetOperation)) {
            return manuallySetOperation;
        }
        String ret = _operationProperties.getProperty(Constants.OPERATION_BULKV2);
        if (StringUtil.isBlank(ret)) {
            return null;
        }
        return ret;
    }

    public void setBulkOperation(String operation) {
        _extraProperties.put(Constants.OPERATION_BULKV2, operation);
    }

    public final void setBoomiOperation(String operation) {
        _extraProperties.put(Constants.OPERATION_BOOMI, operation);
    }

    public String getOperationBoomiName() {
        String manuallySetOperation = _extraProperties.get(Constants.OPERATION_BOOMI);
        if (StringUtil.isNotBlank(manuallySetOperation)) {
            return manuallySetOperation;
        }
        return _operationProperties.getProperty(Constants.OPERATION_BOOMI, "");
    }

    public Long getLimit() {
        boolean applyLimit = _operationProperties.getBooleanProperty(Constants.LIMIT_NUMBER_OF_DOCUMENTS_DESCRIPTOR,
                false);
        if (!applyLimit) {
            return QUERY_UNLIMITED_DOCUMENTS;
        }

        String dynamicProperty = _extraProperties.get(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR);
        return NumberUtil.toLong(dynamicProperty, QUERY_UNLIMITED_DOCUMENTS);
    }

    public boolean getQueryAll() {
        return _operationProperties.getBooleanProperty(Constants.QUERY_ALL_DESCRIPTOR, false);
    }

    public boolean getLogSOQL() {
        return _operationProperties.getBooleanProperty(Constants.LOG_SOQL_DESCRIPTOR, false);
    }

    public Long getPageSize() {
        return _operationProperties.getLongProperty(Constants.QUERY_PAGE_SIZE_DESCRIPTOR);
    }

    public boolean isBulkOperationAPI() {
        if (StringUtil.isNotBlank(getBulkOperation())) {
            return true;
        }
        return OperationAPI.from(_operationProperties.getProperty(OperationAPI.FIELD_ID)) == OperationAPI.BULK_V2;
    }

    /**
     * Should be used in Bulk Operations only
     *
     * @return BatchSize in Bytes
     * @throws ConnectorException if the value was not in the valid range
     */
    public Long getBatchSize() {
        long val = _operationProperties.getLongProperty(Constants.BULK_BATCH_SIZE_DESCRIPTOR, DEFAULT_BATCH_SIZE);
        if (val <= 0 || val > MAX_BATCH_SIZE) {
            throw new ConnectorException(
                    "Invalid Batch Size given:" + val + ". The range of valid values, in MB, is 1-150.");
        }
        return val * MB_TO_BYTES_FACTOR;
    }

    public String getBulkHeader() {
        return _operationProperties.getProperty(Constants.BULK_HEADER_DESCRIPTOR, "");
    }

    public String getAPILimit() {
        return _extraProperties.get(Constants.API_LIMIT);
    }

    public void setAPILimit(String apiLimit) {
        _extraProperties.put(Constants.API_LIMIT, apiLimit);
    }

    /**
     * Return a Map containing the pairs of key/value rest headers.
     *
     * @return a map with the rest headers
     */
    public Map<String, String> getRestHeaders() {
        return ObjectUtil.defaultIfNull(_restHeaders, Collections.emptyMap());
    }

    public void initDynamicProperties(TrackedData input) {
        DynamicPropertyMap dynamicOperationProperties = input.getDynamicOperationProperties();
        _restHeaders = dynamicOperationProperties.getCustomProperties(Constants.REST_HEADERS_DESCRIPTOR);

        _extraProperties.put(Constants.ASSIGNMENT_RULE_ID_DESCRIPTOR,
                dynamicOperationProperties.getProperty(Constants.ASSIGNMENT_RULE_ID_DESCRIPTOR));

        _extraProperties.put(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR,
                dynamicOperationProperties.getProperty(Constants.NUMBER_OF_DOCUMENTS_DESCRIPTOR));

        _extraProperties.put(Constants.EXTERNAL_ID_VALUE,
                dynamicOperationProperties.getProperty(Constants.EXTERNAL_ID_VALUE));
    }

    public long getBatchCount() {
        long val = _operationProperties.getLongProperty(Constants.COMPOSITE_BATCH_COUNT_DESCRIPTOR,
                DEFAULT_BATCH_COUNT);
        if (val <= 0 || val > MAX_BATCH_COUNT) {
            throw new ConnectorException("Invalid Batch Count given:" + val + ". The range of valid values, is 1-200.");
        }
        return val;
    }

    public boolean getAllOrNone() {
        return _operationProperties.getBooleanProperty(Constants.ALL_OR_NONE_DESCRIPTOR, false);
    }

    public boolean getReturnUpdatedRecord() {
        return _operationProperties.getBooleanProperty(Constants.RETURN_UPDATED_RECORD_DESCRIPTOR, false);
    }
}
