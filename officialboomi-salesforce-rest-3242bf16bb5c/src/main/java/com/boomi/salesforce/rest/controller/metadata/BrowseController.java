// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.metadata;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.util.IOUtil;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.IOException;
import java.util.List;

public class BrowseController {
    protected SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public BrowseController(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Requests and returns all SObjects matching a given attribute (for example 'queryable')
     *
     * @param attribute required SObject property name (for example 'queryable')
     * @return List<String> of SObjects names
     */
    public List<String> listSObjects(String attribute) {
        ClassicHttpResponse response = _connectionManager.getRequestHandler().executeGetObjects();
        try {
            return MetadataParser.parseSObjectsOfAttribute(SalesforceResponseUtil.getContent(response), attribute);
        } catch (IOException e) {
            throw new ConnectorException("[Failed to get the list of SObjects] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(response);
        }
    }

    /**
     * Requests salesforce to describe SObject and parse its fields for a specifice operation
     *
     * @param sobjectName target SObject
     * @param operation   target BOOMI operation
     * @return List<SObjectField> of SObject fields
     */
    public SObjectModel buildSObjectModel(String sobjectName, String operation) {
        ClassicHttpResponse response = _connectionManager.getRequestHandler().executeGetFields(sobjectName);
        try {
            return MetadataParser.parseFieldsForOperation(SalesforceResponseUtil.getContent(response), operation);
        } catch (IOException e) {
            throw new ConnectorException("[Failed to get the list of fields] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(response);
        }
    }

}
