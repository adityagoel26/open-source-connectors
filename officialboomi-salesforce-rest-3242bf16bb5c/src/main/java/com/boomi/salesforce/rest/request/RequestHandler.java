// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.salesforce.rest.util.SalesforcePayloadUtil;
import com.boomi.util.IOUtil;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.w3c.dom.Document;

import java.io.InputStream;

/**
 * Helper class responsible for preparing and executing Requests to Salesforce and returns responses
 */
public class RequestHandler {
    private final RequestBuilder _requestBuilder;
    private final RequestExecutor _requestExecutor;

    /**
     * @param
     */
    public RequestHandler(RequestBuilder requestBuilder, RequestExecutor requestExecutor) {
        _requestBuilder = requestBuilder;
        _requestExecutor = requestExecutor;
    }


    /**
     * Executes list all SObjects request and returns the response body as response
     *
     * @return response contains the response of the list of SObjects
     * @throws ConnectorException if response returned error message
     */
    public ClassicHttpResponse executeGetObjects() {
        ClassicHttpRequest request = _requestBuilder.getSObjects();
        return _requestExecutor.executeStream(request, "list SObjects");
    }

    /**
     * Executes list all fields of a SObjects request and returns the response body response
     *
     * @param sobjectName the target object
     * @return response contains the response of the list of fields
     * @throws ConnectorException if response returned error message
     */
    public ClassicHttpResponse executeGetFields(String sobjectName) throws ConnectorException {
        ClassicHttpRequest request = _requestBuilder.describeSObject(sobjectName);
        return _requestExecutor.executeStream(request, "describe " + sobjectName + " fields");
    }

    /**
     * Executes SOQL Query request and returns the response body response
     *
     * @param queryString the SOQL Query string
     * @param pageSize    the number of documents returned in this request
     * @return response contains the response of query SOQL
     * @throws ConnectorException if response returned error message
     */
    public ClassicHttpResponse executeSOQLQuery(String queryString, Long pageSize) throws ConnectorException {
        ClassicHttpRequest request = _requestBuilder.querySOQL(queryString, pageSize);
        return _requestExecutor.executeStream(request, "query SOQL");
    }

    /**
     * Executes SOQL Query request and returns the response body response
     *
     * @param nextPageUrl url to the target Query result Page. Can be null
     * @param pageSize    number of documents in the returned request. Can be null
     * @return response contains the response of query SOQL
     * @throws ConnectorException if response returned error message
     */
    public ClassicHttpResponse executeNextPageQuery(String nextPageUrl, Long pageSize) {
        ClassicHttpRequest request = _requestBuilder.queryNextPage(nextPageUrl, pageSize);
        return _requestExecutor.executeStream(request, "query SOQL");
    }

    /**
     * Executes SOQL QueryAll request and returns the response body response
     *
     * @param queryString the SOQL Query string
     * @param pageSize    number of documents in the returned request. Can be null
     * @return response contains the response of queryAll SOQL
     * @throws ConnectorException if response returned error message
     */
    public ClassicHttpResponse executeSOQLQueryAll(String queryString, Long pageSize) {
        ClassicHttpRequest request = _requestBuilder.buildSOQLQueryAll(queryString, pageSize);
        return _requestExecutor.executeStream(request, "queryAll SOQL");
    }

    /**
     * Requests Salesforce to create new bulk job
     *
     * @return response contains json format contains the job metadata
     */
    public ClassicHttpResponse createBulkV2Job() {
        ClassicHttpRequest request = _requestBuilder.buildJob();
        return _requestExecutor.executeStream(request, "create bulk Job");
    }

    /**
     * Requests Salesforce to create new bulk query job
     *
     * @param queryString the SOQL Query string
     * @return response contains JSON format contains the job metadata
     */
    public ClassicHttpResponse createBulkV2QueryJob(String queryString) {
        ClassicHttpRequest request = _requestBuilder.buildQueryJob(queryString);
        return _requestExecutor.executeStream(request, "create query bulk Job");
    }

    /**
     * Requests Salesforce to return the successful records of a bulk job
     *
     * @param jobID Bulk Job Id
     * @return response contains CSV format contains the accepted records
     */
    public ClassicHttpResponse getBulkSuccessResult(String jobID) {
        ClassicHttpRequest request = _requestBuilder.getSuccessResult(jobID);
        return _requestExecutor.executeStream(request, "get bulk successful results");
    }

    /**
     * @param jobID       Bulk Job Id
     * @param nextPageUrl url to the target Query result Page. Can be null
     * @param pageSize    number of documents in the returned request. Can be null
     * @return response contains CSV format contains the query records
     */
    public ClassicHttpResponse getBulkQueryResult(String jobID, String nextPageUrl, Long pageSize) {
        ClassicHttpRequest request = _requestBuilder.getBulkQueryResult(jobID, nextPageUrl, pageSize);
        return _requestExecutor.executeStream(request, "get bulk successful results");
    }

    /**
     * Requests Salesforce to return the failed records of a bulk job
     *
     * @param jobID Bulk Job Id
     * @return response contains csv format contains the failed records
     */
    public ClassicHttpResponse getFailedResult(String jobID) {
        ClassicHttpRequest request = _requestBuilder.getFailedResult(jobID);
        return _requestExecutor.executeStream(request, "get bulk failed results");
    }

    /**
     * Requests Salesforce to return the bulk job status
     *
     * @param jobID Bulk Job Id
     * @return response contains JSON format contains the job status
     */
    public ClassicHttpResponse getBulkStatus(String jobID) {
        ClassicHttpRequest request = _requestBuilder.getJobStatus(jobID);
        return _requestExecutor.executeStream(request, "get bulk status");
    }

    /**
     * Requests Salesforce to return the query bulk job status
     *
     * @param jobID Bulk Job Id
     * @return response contains JSON format contains the job status
     */
    public ClassicHttpResponse getBulkQueryStatus(String jobID) {
        ClassicHttpRequest request = _requestBuilder.getQueryJobStatus(jobID);
        return _requestExecutor.executeStream(request, "get bulk status");
    }

    /**
     * Requests Salesforce to create new record in a SObject
     *
     * @param sobjectName      target SObject name of the record
     * @param inputData        record content to be created
     * @param assignmentRuleID
     * @return response contains the create response
     */
    public ClassicHttpResponse executeCreate(String sobjectName, InputStream inputData, long contentLength,
                                             String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder
                .createRecord(sobjectName, inputData, contentLength, assignmentRuleID);
        return _requestExecutor.executeStream(request, "create");
    }

    public ClassicHttpResponse executeCreateTree(String sobjectName, InputStream inputData, long contentLength,
                                                 String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder
                .createTree(sobjectName, inputData, contentLength, assignmentRuleID);
        return _requestExecutor.executeStream(request, "create tree");
    }

    /**
     * Requests Salesforce to Upsert a record in a SObject
     *
     * @param sobjectName      target SObject name of the record
     * @param externalIdField  the name of the external ID field
     * @param externalIdValue  the external ID value
     * @param inputData        record content to be Upserted
     * @param assignmentRuleID the ID of the assignment rule
     * @return response the response from Salesforce
     */
    public ClassicHttpResponse executeUpsert(String sobjectName, String externalIdField, String externalIdValue,
            InputStream inputData, String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder.upsertRecord(sobjectName, externalIdField, externalIdValue,
                inputData, assignmentRuleID);
        return _requestExecutor.executeStream(request, "upsert");
    }

    /**
     * Requests Salesforce to create Composite records to sObject Collection resource
     *
     * @param compositeBatch   content batch records to be created
     * @param assignmentRuleID
     * @return response contains the create sObject Collection response
     */
    public ClassicHttpResponse createCompositeCollection(InputStream compositeBatch, String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder.createCompositeCollection(compositeBatch, assignmentRuleID);
        return _requestExecutor.executeStream(request, "Composite create");
    }

    /**
     * Requests Salesforce to update Composite records to sObject Collection resource
     *
     * @param compositeBatch   content batch records to be updated
     * @param assignmentRuleID
     * @return response contains the update sObject Collection response
     */
    public ClassicHttpResponse updateCompositeCollection(InputStream compositeBatch, String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder.updateCompositeCollection(compositeBatch, assignmentRuleID);
        return _requestExecutor.executeStream(request, "Composite update");
    }

    /**
     * Requests Salesforce to upsert Composite records to sObject Collection resource
     *
     * @param compositeBatch   content batch records to be upserted
     * @param assignmentRuleID
     * @return response contains the upsert sObject Collection response
     */
    public ClassicHttpResponse upsertCompositeCollection(String sobjectName, String externalIdField,
                                                         InputStream compositeBatch, String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder
                .upsertCompositeCollection(sobjectName, externalIdField, compositeBatch, assignmentRuleID);
        return _requestExecutor.executeStream(request, "Composite upsert");
    }

    public ClassicHttpResponse deleteCompositeCollection(String deleteParameters, Boolean allOrNone) {
        ClassicHttpRequest request = _requestBuilder.deleteCompositeCollection(deleteParameters, allOrNone);
        return _requestExecutor.executeStream(request, "Composite delete");
    }

    /**
     * Requests Salesforce to delete a record from a SObject
     *
     * @param sobjectName target SObject name of the record
     * @param recordId    Id of the target record
     */
    public void executeDelete(String sobjectName, String recordId) {
        ClassicHttpRequest request = _requestBuilder.deleteRecord(sobjectName, recordId);
        _requestExecutor.executeVoid(request, "delete");
    }

    /**
     * Requests Salesforce to update a record from a SObject.
     *
     * @param sobjectName      target SObject name of the record
     * @param recordId         Id of the target record
     * @param inputData        SizeLimited XML Document contains updated content
     * @param assignmentRuleID
     */
    public void executeUpdate(String sobjectName, String recordId, Document inputData, String assignmentRuleID) {
        InputStream inputStream = null;
        Payload tempPayload = PayloadUtil.toPayload(inputData);
        try {
            inputStream = SalesforcePayloadUtil.payloadToInputStream(tempPayload);

            ClassicHttpRequest request = _requestBuilder
                    .updateRecord(sobjectName, recordId, inputStream, assignmentRuleID);
            _requestExecutor.executeVoid(request, "update");
        } finally {
            IOUtil.closeQuietly(tempPayload, inputStream);
        }
    }

    public ClassicHttpResponse executeUpdateReturnUpdatedRecord(String sobjectName, String recordId, Document inputData,
                                                                String assignmentRuleID) {
        ClassicHttpRequest request = _requestBuilder
                .updateReturnUpdatedRecord(sobjectName, recordId, inputData, assignmentRuleID);
        return _requestExecutor.executeStream(request, "update and return updated record");
    }

    /**
     * Uploads CSV data batch to bulk job
     *
     * @param jobID         Bulk Job Id
     * @param data          batch CSV data
     * @param contentLength length of data batch
     */
    public void uploadData(String jobID, InputStream data, long contentLength) {
        ClassicHttpRequest request = _requestBuilder.uploadBatch(jobID, data, contentLength);
        _requestExecutor.executeVoid(request, "upload bulk batch");
    }

    /**
     * Marks data batch as finished to bulk job so Salesforce starts processing
     *
     * @param jobID Bulk Job Id
     */
    public void finishUpload(String jobID) {
        ClassicHttpRequest request = _requestBuilder.finishBatch(jobID);
        _requestExecutor.executeVoid(request, "finish upload bulk batch");
    }

    /**
     * Tests connection by sending request to retrieve information about the Salesforce version
     */
    public void testConnection() {
        ClassicHttpRequest request = _requestBuilder.testConnection();
        _requestExecutor.executeVoid(request, "initialize session");
    }
}
