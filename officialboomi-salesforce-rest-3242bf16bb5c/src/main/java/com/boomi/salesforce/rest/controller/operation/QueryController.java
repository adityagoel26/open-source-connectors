// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.QueryFilter;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.builder.soql.SOQLBuilder;
import com.boomi.salesforce.rest.controller.DocumentController;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.data.SOQLXMLSplitter;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

public class QueryController {
    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public QueryController(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Main QUERY executor for all query operations<br> Closing the returned SOQLXMLSplitter will close the Query
     * response
     *
     * @param queryString SOQL query to be executed
     * @return SOQLXMLSplitter XML splitter contains split XML payloads
     */
    private SOQLXMLSplitter executeQueryREST(String queryString) {
        Long pageSize = _connectionManager.getOperationProperties().getPageSize();
        boolean queryAll = _connectionManager.getOperationProperties().getQueryAll();
        boolean logSOQL = _connectionManager.getOperationProperties().getLogSOQL();

        if (logSOQL) {
            _connectionManager.getConnectionProperties().logInfo("Executing SOQL: " + queryString);
        }

        if (queryAll) {
            return executeQuery(
                    () -> _connectionManager.getRequestHandler().executeSOQLQueryAll(queryString, pageSize));
        }

        return executeQuery(() -> _connectionManager.getRequestHandler().executeSOQLQuery(queryString, pageSize));
    }

    /**
     * Next Page QUERY executor
     *
     * @param nextPageUrl locator to the query page
     * @return SOQLXMLSplitter XML splitter contains split XML payloads
     */
    public SOQLXMLSplitter nextPageQuery(String nextPageUrl) {
        Long pageSize = _connectionManager.getOperationProperties().getPageSize();
        return executeQuery(() -> _connectionManager.getRequestHandler().executeNextPageQuery(nextPageUrl, pageSize));
    }

    private static SOQLXMLSplitter executeQuery(Supplier<ClassicHttpResponse> executor) {
        boolean isSuccess = false;
        ClassicHttpResponse response = null;
        try {
            response = executor.get();
            SOQLXMLSplitter payloadSplitter = new SOQLXMLSplitter(response);
            isSuccess = true;
            return payloadSplitter;
        } catch (ConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("[Failed to read response] " + e.getMessage(), e);
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(response);
            }
        }
    }

    /**
     * Builds the SOQL Query String from the platform QUERY options. Validates the Query Length
     *
     * @param selectedFields the fields selected
     * @param queryFilter    the filters added
     * @return the SOQL query string
     */
    public String buildQueryString(List<String> selectedFields, QueryFilter queryFilter) {
        String sobjectName = _connectionManager.getOperationProperties().getSObject();
        Long limit = _connectionManager.getOperationProperties().getLimit();

        SObjectController controller = new SObjectController(_connectionManager);
        String queryString = new SOQLBuilder(controller, sobjectName, selectedFields, queryFilter, limit)
                .generateSOQLQuery();

        DocumentController.assertQueryLength((long) queryString.length(),
                                             _connectionManager.getOperationProperties().isBulkOperationAPI());
        return queryString;
    }

    /**
     * Handles QUERY operation, gets the appropriate SOQL query and calls startQuery
     *
     * @param selectedFields the list of selected fields of the SObject
     * @param filter         the filter to be added in the SOQL query
     * @return SOQLXMLSplitter contains the response result stream and it must be closed by the caller
     */
    public SOQLXMLSplitter query(List<String> selectedFields, QueryFilter filter) {
        String queryString = buildQueryString(selectedFields, filter);
        return executeQueryREST(queryString);
    }

    /**
     * CustomSOQL wrapper for executes customSOQL operation. Validates the input Length
     *
     * @param input ObjectData contains the input SOQL query
     * @return SOQLXMLSplitter contains the response result stream
     * @throws ConnectorException if the input length exceeded the limit
     */
    public SOQLXMLSplitter customQuery(ObjectData input) {
        String queryString = readSOQLInput(input);
        return executeQueryREST(queryString);
    }

    /**
     * For the Custom SOQL Operations,reads the input data as SOQL String. Validates the Query Length
     *
     * @param input ObjectData contains the input SOQL query
     * @return input SOQL String
     * @throws ConnectorException if the input length exceeded the limit
     */
    public String readSOQLInput(ObjectData input) {
        DocumentController.assertQueryLength(input, _connectionManager.getOperationProperties().isBulkOperationAPI());

        InputStream inputStream = input.getData();
        String queryString;
        try {
            queryString = StreamUtil.toString(inputStream, "UTF-8");
        } catch (IOException e) {
            throw new ConnectorException("[Failed to read input data] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(inputStream);
        }
        return queryString;
    }

}
