// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.metadata;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.model.SObjectModel;

/**
 * Responsible for control SObject metadata for import profile
 */
public class SObjectController extends BrowseController {

    /**
     * @param connectionManager SFRestConnection instance
     */
    public SObjectController(SFRestConnection connectionManager) {
        super(connectionManager);
    }

    /**
     * If saved in connector cache returns it otherwise do the following then save it in cache:
     * <p>
     * Builds SObject with fields and relationships for a specific platform operation. Builds the SObject fields and the
     * direct children relations and parent relations if it is QUERY operation
     *
     * @param sobjectName target salesforce SObject name
     */
    public SObjectModel buildSObject(String sobjectName) {
        return buildSObject(sobjectName, true);
    }

    /**
     * If useCache is true and SObject i saved in connector cache returns it from cache, otherwise do the following then
     * save it in cache:
     * <p>
     * Builds SObject with fields and relationships for a specific platform operation. Builds the SObject fields and the
     * direct children relations and parent relations if it is QUERY operation
     *
     * @param sobjectName    target salesforce SObject name
     * @param isGlobalUnique boolean to get the SObjectModel if it was restored in cached
     */
    public SObjectModel buildSObject(String sobjectName, boolean isGlobalUnique) {
        String operationName = _connectionManager.getOperationProperties().getOperationBoomiName();

        if (_connectionManager.getConnectionProperties().isSObjectCached(sobjectName, operationName, isGlobalUnique)) {
            return _connectionManager.getConnectionProperties().getSObject(sobjectName, operationName, isGlobalUnique);
        }
        SObjectModel sobject = buildSObjectModel(sobjectName, operationName);
        _connectionManager.getConnectionProperties().cacheSObject(sobjectName, operationName, sobject);
        return sobject;
    }

}
