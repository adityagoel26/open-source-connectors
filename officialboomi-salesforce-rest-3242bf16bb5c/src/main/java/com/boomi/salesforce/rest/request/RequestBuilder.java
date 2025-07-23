// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.salesforce.rest.util.SalesforcePayloadUtil;
import com.boomi.util.DOMUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.message.BasicHeader;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.net.URI;

/**
 * Builds ClassicHttpRequests ready for execution for all the requests to Salesforce
 */
public class RequestBuilder {
    private final SFURIBuilder _sfuriBuilder;
    private final OperationProperties _operationProperties;

    /**
     * @param sfuriBuilder        SFUriBuilder instance responsible for building URI to SF REST API
     * @param operationProperties OperationProperties responsible for retrieving properties
     */
    public RequestBuilder(SFURIBuilder sfuriBuilder, OperationProperties operationProperties) {
        _sfuriBuilder = sfuriBuilder;
        _operationProperties = operationProperties;
    }

    /**
     * Builds the ClassicHttpRequest to SOAP API login
     *
     * @param soapLoginBody HttpEntity the content SOAP Login message with the username/password
     * @return ClassicHttpRequest to get new sessionID. Ready for execution
     */
    public ClassicHttpRequest loginSoap(HttpEntity soapLoginBody) {
        URI uri = _sfuriBuilder.soapLogin();
        return ClassicRequestBuilder.post(uri).addHeader(Constants.CONTENT_TYPE_REQUEST, Constants.TEXT_XML)
                                    .addHeader(Constants.SOAP_ACTION_REQUEST, Constants.LOGIN_ACTION_REQUEST)
                                    .setEntity(soapLoginBody).build();
    }

    /**
     * Builds the Content-Type XML header for the requests
     *
     * @return Header Content-Type XML Header
     */
    private static Header contentXML() {
        return new BasicHeader(Constants.CONTENT_TYPE_REQUEST, Constants.XML);
    }

    /**
     * Builds the Accept XML header for the requests
     *
     * @return Header Accept XML Header
     */
    private static Header acceptXML() {
        return new BasicHeader(Constants.ACCEPT_REQUEST, Constants.XML);
    }

    /**
     * Builds the Content-Type JSON header for the requests
     *
     * @return Header Content-Type JSON Header
     */
    private static Header contentJSON() {
        return new BasicHeader(Constants.CONTENT_TYPE_REQUEST, Constants.JSON);
    }

    /**
     * Builds the Accept JSON header for the requests
     *
     * @return Header Accept JSON Header
     */
    protected static Header acceptJSON() {
        return new BasicHeader(Constants.ACCEPT_REQUEST, Constants.JSON);
    }

    /**
     * Builds the Content-Type CSV header for the requests
     *
     * @return Header Content-Type CSV Header
     */
    private static Header contentCSV() {
        return new BasicHeader(Constants.CONTENT_TYPE_REQUEST, Constants.CSV);
    }

    /**
     * Builds the Accept CSV header for the requests
     *
     * @return Header Accept CSV Header
     */
    private static Header acceptCSV() {
        return new BasicHeader(Constants.ACCEPT_REQUEST, Constants.CSV);
    }

    /**
     * Builds the Query Page Size header for the REST QUERY page size
     *
     * @return Header query page size Header
     */
    private static Header queryPageSize(Long pageSize) {
        return new BasicHeader(Constants.QUERY_BATCH_KEY_REQUEST, Constants.QUERY_BATCH_VALUE_REQUEST + pageSize);
    }

    private static Header assignmentRuleID(String ruleID) {
        if (StringUtil.isNotBlank(ruleID)) {
            return new BasicHeader(Constants.ASSIGNMENT_RULE_ID_REST_HEADER, ruleID);
        }
        // if Header is null will not be added by ClassicRequestBuilder documentations
        return null;
    }

    /**
     * Builds the ClassicHttpRequest to a new QUERY Job in Bulk V2
     *
     * @param queryString SOQL query string
     * @return ClassicHttpRequest to new QUERY Job. Ready for execution
     */
    public ClassicHttpRequest buildQueryJob(String queryString) {
        ObjectNode requestBody = new ObjectMapper().createObjectNode();
        requestBody.put(Constants.QUERY_BULKV2, queryString);
        requestBody.put(Constants.OPERATION_BULKV2, _operationProperties.getBulkOperation());
        requestBody.put(Constants.LINE_ENDING_BULKV2, _operationProperties.getLineEnding());
        requestBody.put(Constants.COLUMN_DELIMITER_BULKV2, _operationProperties.getColumnDelimiter());

        URI uri = _sfuriBuilder.buildQueryJob();
        return ClassicRequestBuilder.post(uri).addHeader(acceptJSON()).addHeader(contentJSON())
                                    .setEntity(new StringEntity(requestBody.toString())).build();
    }

    /**
     * Builds the ClassicHttpRequest to a new Job in Bulk V2
     *
     * @return ClassicHttpRequest to new Job. Ready for execution
     */
    public ClassicHttpRequest buildJob() {
        ObjectNode requestBody = new ObjectMapper().createObjectNode();
        // headers for creating a bulk job
        requestBody.put(Constants.CONTENT_TYPE_BULKV2, Constants.CONTENT_CSV_BULKV2);
        requestBody.put(Constants.LINE_ENDING_BULKV2, _operationProperties.getLineEnding());
        requestBody.put(Constants.OBJECT_SOBJECT, _operationProperties.getSObject());
        requestBody.put(Constants.OPERATION_BULKV2, _operationProperties.getBulkOperation());
        requestBody.put(Constants.COLUMN_DELIMITER_BULKV2, _operationProperties.getColumnDelimiter());
        requestBody.put(Constants.EXTERNAL_ID_FIELD_BULKV2, _operationProperties.getExternalIdFieldName());
        requestBody.put(Constants.ASSIGNMENT_RULE_ID_DESCRIPTOR, _operationProperties.getAssignmentRuleId());

        URI uri = _sfuriBuilder.buildJob();
        return ClassicRequestBuilder.post(uri).addHeader(acceptJSON()).addHeader(contentJSON())
                                    .setEntity(new StringEntity(requestBody.toString())).build();
    }

    /**
     * Builds the ClassicHttpRequest to a upload CSV data in Bulk V2 job
     *
     * @param jobID         Bulk Job Id
     * @param data          InputStream of batch data
     * @param contentLength length of batch data
     * @return ClassicHttpRequest to upload bulk data batch. Ready for execution
     */
    public ClassicHttpRequest uploadBatch(String jobID, InputStream data, long contentLength) {
        InputStreamEntity requestBody = new InputStreamEntity(data, contentLength, null);
        URI uri = _sfuriBuilder.uploadBatchBulk(jobID);
        return ClassicRequestBuilder.put(uri).addHeader(acceptJSON()).addHeader(contentCSV()).setEntity(requestBody)
                                    .build();
    }

    /**
     * Builds the ClassicHttpRequest to set upload state as UploadComplete so Salesforce starts processing the batch of
     * Bulk V2 job
     *
     * @param jobID Bulk Job Id
     * @return ClassicHttpRequest to mark finish bulk data batch. Ready for execution
     */
    public ClassicHttpRequest finishBatch(String jobID) {
        ObjectNode requestBody = new ObjectMapper().createObjectNode();
        requestBody.put(Constants.STATE_BULKV2, Constants.UPLOAD_COMPLETE_BULKV2);

        URI uri = _sfuriBuilder.finishBatchBulk(jobID);
        return ClassicRequestBuilder.patch(uri).addHeader(acceptJSON()).addHeader(contentJSON())
                                    .setEntity(requestBody.toString()).build();
    }

    /**
     * Builds the ClassicHttpRequest to get the job status of Bulk V2 job
     *
     * @param jobID Bulk Job Id
     * @return ClassicHttpRequest to get bulk job status. Ready for execution
     */
    public ClassicHttpRequest getJobStatus(String jobID) {
        URI uri = _sfuriBuilder.getBulkStatus(jobID);
        return ClassicRequestBuilder.get(uri).addHeader(acceptJSON()).build();
    }

    /**
     * Builds the ClassicHttpRequest to get the QUERY job status of Bulk V2 Query job
     *
     * @param jobID Bulk Job Id
     * @return ClassicHttpRequest to get QUERY bulk job status. Ready for execution
     */
    public ClassicHttpRequest getQueryJobStatus(String jobID) {
        URI uri = _sfuriBuilder.bulkQueryStatus(jobID);
        return ClassicRequestBuilder.get(uri).addHeader(acceptJSON()).build();
    }

    /**
     * Builds the ClassicHttpRequest to get the Successful Results of Bulk V2 job
     *
     * @param jobID Bulk Job Id
     * @return ClassicHttpRequest to get bulk job Successful Results. Ready for execution
     */
    public ClassicHttpRequest getSuccessResult(String jobID) {
        URI uri = _sfuriBuilder.bulkSuccessResult(jobID);
        return ClassicRequestBuilder.get(uri).addHeader(acceptCSV()).build();
    }

    /**
     * Builds the ClassicHttpRequest to get the Results of Query Bulk V2 job
     *
     * @param jobID    Bulk Job Id
     * @param locator  locator to the current Page for QUERY. Can be null
     * @param pageSize number of documents in the returned request. Can be null
     * @return ClassicHttpRequest to get bulk query result. Ready for execution
     */
    public ClassicHttpRequest getBulkQueryResult(String jobID, String locator, Long pageSize) {
        URI uri = _sfuriBuilder.bulkQueryResult(jobID, locator, pageSize);
        return ClassicRequestBuilder.get(uri).addHeader(acceptCSV()).build();
    }

    /**
     * Builds the ClassicHttpRequest to get the Failed Results of Bulk V2 job
     *
     * @param jobID Bulk Job Id
     * @return ClassicHttpRequest to get bulk job Failed Results. Ready for execution
     */
    public ClassicHttpRequest getFailedResult(String jobID) {
        URI uri = _sfuriBuilder.bulkFailedResult(jobID);
        return ClassicRequestBuilder.get(uri).addHeader(acceptCSV()).build();
    }

    /**
     * Builds the ClassicHttpRequest to list all SObjects
     *
     * @return ClassicHttpRequest to list all SObjects metadata. Ready for execution
     */
    public ClassicHttpRequest getSObjects() {
        URI uri = _sfuriBuilder.listSObjects();
        return ClassicRequestBuilder.get(uri).addHeader(acceptJSON()).build();
    }

    /**
     * Builds the ClassicHttpRequest to list all fields and metadata of a SObject
     *
     * @param sobjectName name of the target SObject to describe
     * @return ClassicHttpRequest to get metadata of a SObject. Ready for execution
     */
    public ClassicHttpRequest describeSObject(String sobjectName) {
        URI uri = _sfuriBuilder.describeSObject(sobjectName);
        return ClassicRequestBuilder.get(uri).addHeader(acceptJSON()).build();
    }

    /**
     * Builds the ClassicHttpRequest to a REST SOQL Query
     *
     * @param queryString SOQL Query String to be executed
     * @param pageSize    number of documents in the returned request. Can be null
     * @return ClassicHttpRequest to Query. Ready for execution
     */
    public ClassicHttpRequest querySOQL(String queryString, Long pageSize) {
        URI uri = _sfuriBuilder.querySOQL(queryString);
        ClassicRequestBuilder ret = ClassicRequestBuilder.get(uri).addHeader(acceptXML());
        if (pageSize != null) {
            ret.addHeader(queryPageSize(pageSize));
        }
        return ret.build();
    }

    /**
     * Builds the ClassicHttpRequest to a REST SOQL Query page
     *
     * @param nextPageUrl url to the target Query result Page. Can be null
     * @param pageSize    number of documents in the returned request. Can be null
     * @return ClassicHttpRequest to Query next page. Ready for execution
     */
    public ClassicHttpRequest queryNextPage(String nextPageUrl, Long pageSize) {
        URI uri = _sfuriBuilder.querySOQLNextPage(nextPageUrl);
        ClassicRequestBuilder ret = ClassicRequestBuilder.get(uri).addHeader(acceptXML());
        if (pageSize != null) {
            ret.addHeader(queryPageSize(pageSize));
        }
        return ret.build();
    }

    /**
     * Builds the ClassicHttpRequest to a REST SOQL QueryAll
     *
     * @param queryString SOQL Query String to be executed
     * @param pageSize    number of documents in the returned request. Can be null
     * @return ClassicHttpRequest to QueryAll. Ready for execution
     */
    public ClassicHttpRequest buildSOQLQueryAll(String queryString, Long pageSize) {
        URI uri = _sfuriBuilder.queryAllSOQL(queryString);
        return ClassicRequestBuilder.get(uri).addHeader(acceptXML()).addHeader(queryPageSize(pageSize)).build();
    }

    /**
     * Builds the ClassicHttpRequest to REST CREATE record.
     *
     * @param sobjectName   name of the target SObject
     * @param inputData     the body of the request, the content of the record
     * @param contentLength length of the content in bytes
     * @param ruleID        assignment rule ID value to be added in header
     * @return ClassicHttpRequest to create record. Ready for execution
     */
    public ClassicHttpRequest createRecord(String sobjectName, InputStream inputData, long contentLength,
                                           String ruleID) {
        InputStreamEntity requestBody = new InputStreamEntity(inputData, contentLength, ContentType.APPLICATION_XML);
        URI uri = _sfuriBuilder.createRecord(sobjectName);
        return ClassicRequestBuilder.post(uri).addHeader(contentXML()).addHeader(acceptXML())
                                    .addHeader(assignmentRuleID(ruleID)).setEntity(requestBody).build();
    }

    /**
     * Builds the ClassicHttpRequest to composite CREATE TREE.
     *
     * @param sobjectName   name of the target SObject
     * @param inputData     the body of the request, the content of the composite tree
     * @param contentLength length of the content in bytes
     * @param ruleID        assignment rule ID value to be added in header
     * @return ClassicHttpRequest to create record. Ready for execution
     */
    public ClassicHttpRequest createTree(String sobjectName, InputStream inputData, long contentLength, String ruleID) {
        InputStreamEntity requestBody = new InputStreamEntity(inputData, contentLength, ContentType.APPLICATION_XML);
        URI uri = _sfuriBuilder.createTree(sobjectName);
        return ClassicRequestBuilder.post(uri).addHeader(contentXML()).addHeader(acceptXML())
                                    .addHeader(assignmentRuleID(ruleID)).setEntity(requestBody).build();
    }

    /**
     * Builds the ClassicHttpRequest to REST DELETE record
     *
     * @param sobjectName name of the target SObject
     * @param recordId    the internal Id of the SObject record
     * @return ClassicHttpRequest to delete a record. Ready for execution
     */
    public ClassicHttpRequest deleteRecord(String sobjectName, String recordId) {
        URI uri = _sfuriBuilder.deleteRecord(sobjectName, recordId);
        return ClassicRequestBuilder.delete(uri).addHeader(acceptXML()).build();
    }

    /**
     * Builds the ClassicHttpRequest to REST UPDATE record
     *
     * @param sobjectName name of the target SObject
     * @param recordId    the internal Id of the SObject record
     * @param inputData   InputStream the request body contains SizeLimited the XML updated content
     * @param ruleID      assignment rule ID value to be added in header
     * @return ClassicHttpRequest to Update a record. Ready for execution
     */
    public ClassicHttpRequest updateRecord(String sobjectName, String recordId, InputStream inputData, String ruleID) {
        // Request body as InputStreamEntity around the inputData InputStream
        InputStreamEntity requestBody = new InputStreamEntity(inputData, ContentType.APPLICATION_XML);

        URI uri = _sfuriBuilder.updateRecord(sobjectName, recordId);
        return ClassicRequestBuilder.patch(uri).addHeader(contentXML()).addHeader(acceptXML())
                                    .addHeader(assignmentRuleID(ruleID)).setEntity(requestBody).build();

    }

    /**
     * Builds the ClassicHttpRequest to a REST UPSERT
     *
     * @param sobjectName     name of the target SObject
     * @param externalIdField name of the field used as External ID
     * @param externalIdValue value of the External ID
     * @param inputData       InputStream the request body contains the XML UPSERT content
     * @param ruleID          assignment rule ID value to be added in header
     * @return ClassicHttpRequest to Upsert a record. Ready for execution
     */
    public ClassicHttpRequest upsertRecord(String sobjectName, String externalIdField, String externalIdValue,
            InputStream inputData, String ruleID) {
        // Request body as InputStreamEntity around the inputData InputStream
        InputStreamEntity requestBody = new InputStreamEntity(inputData, ContentType.APPLICATION_XML);

        URI uri = _sfuriBuilder.upsertRecord(sobjectName, externalIdField, externalIdValue);

        ClassicRequestBuilder builder;

        if (StringUtil.isEmpty(externalIdValue)) {
            // insert new entity
            builder = ClassicRequestBuilder.post(uri);
        } else {
            // update existing entity
            builder = ClassicRequestBuilder.patch(uri);
        }

        return builder.addHeader(contentXML()).addHeader(acceptXML()).addHeader(assignmentRuleID(ruleID)).setEntity(
                requestBody).build();
    }

    /**
     * Builds the ClassicHttpRequest to a Composite request to Create sObjectCollection
     *
     * @param compositeBatch request body containing sObject Collection records format
     * @param ruleID         assignment rule ID value to be added in header
     * @return ClassicHttpRequest to create Composite records. Ready for execution
     */
    public ClassicHttpRequest createCompositeCollection(InputStream compositeBatch, String ruleID) {
        // Request body as InputStreamEntity around the inputData InputStream
        InputStreamEntity requestBody = new InputStreamEntity(compositeBatch, ContentType.APPLICATION_XML);

        URI uri = _sfuriBuilder.compositeCollection();
        return ClassicRequestBuilder.post(uri).addHeader(contentXML()).addHeader(acceptXML())
                                    .addHeader(assignmentRuleID(ruleID)).setEntity(requestBody).build();
    }

    /**
     * Builds the ClassicHttpRequest to a Composite request to Update sObjectCollection
     *
     * @param compositeBatch request body containing sObject Collection records format
     * @param ruleID         assignment rule ID value to be added in header
     * @return ClassicHttpRequest to update Composite records. Ready for execution
     */
    public ClassicHttpRequest updateCompositeCollection(InputStream compositeBatch, String ruleID) {
        // Request body as InputStreamEntity around the inputData InputStream
        InputStreamEntity requestBody = new InputStreamEntity(compositeBatch, ContentType.APPLICATION_XML);

        URI uri = _sfuriBuilder.compositeCollection();
        return ClassicRequestBuilder.patch(uri).addHeader(contentXML()).addHeader(acceptXML())
                                    .addHeader(assignmentRuleID(ruleID)).setEntity(requestBody).build();
    }

    /**
     * Builds the ClassicHttpRequest to a Composite request to Upsert sObjectCollection
     *
     * @param sobjectName     name of the target SObject
     * @param externalIdField name of the field used as External ID
     * @param compositeBatch  request body containing sObject Collection records format
     * @param ruleID          assignment rule ID value to be added in header
     * @return ClassicHttpRequest to upsert Composite records. Ready for execution
     */
    public ClassicHttpRequest upsertCompositeCollection(String sobjectName, String externalIdField,
                                                        InputStream compositeBatch, String ruleID) {
        // Request body as InputStreamEntity around the inputData InputStream
        InputStreamEntity requestBody = new InputStreamEntity(compositeBatch, ContentType.APPLICATION_XML);

        URI uri = _sfuriBuilder.compositeUpsertCollection(sobjectName, externalIdField);
        return ClassicRequestBuilder.patch(uri).addHeader(contentXML()).addHeader(acceptXML())
                                    .addHeader(assignmentRuleID(ruleID)).setEntity(requestBody).build();
    }

    public ClassicHttpRequest deleteCompositeCollection(String deleteParameters, Boolean allOrNone) {
        URI uri = _sfuriBuilder.compositeDeleteCollection(deleteParameters, allOrNone);
        return ClassicRequestBuilder.delete(uri).addHeader(contentXML()).addHeader(acceptXML()).build();
    }

    public ClassicHttpRequest updateReturnUpdatedRecord(String sobjectName, String recordId, Document record,
                                                        String ruleID) {
        Node oldRecordsRoot = record.getDocumentElement();

        Document newDoc = DOMUtil.newDocument();
        Element batch = newDoc.createElement("batch");
        Element batchRequests = newDoc.createElement("batchRequests");

        Element requestPatch = newDoc.createElement("request");
        Element methodPatch = newDoc.createElement("method");
        Element urlPatch = newDoc.createElement("url");
        Element richInputPatch = newDoc.createElement("richInput");

        methodPatch.setTextContent("PATCH");
        urlPatch.setTextContent(_sfuriBuilder.getUpdateReturnUpdatedRecordURI(sobjectName, recordId));
        richInputPatch.appendChild(newDoc.importNode(oldRecordsRoot, true));

        requestPatch.appendChild(methodPatch);
        requestPatch.appendChild(urlPatch);
        requestPatch.appendChild(richInputPatch);

        Element requestGET = newDoc.createElement("request");
        Element methodGET = newDoc.createElement("method");
        Element urlGET = newDoc.createElement("url");

        methodGET.setTextContent("GET");
        urlGET.setTextContent(_sfuriBuilder.getRecordURI(sobjectName, recordId));

        requestGET.appendChild(methodGET);
        requestGET.appendChild(urlGET);

        batchRequests.appendChild(requestPatch);
        batchRequests.appendChild(requestGET);

        batch.appendChild(batchRequests);

        newDoc.appendChild(batch);

        Payload tempPayload = PayloadUtil.toPayload(newDoc);
        InputStream inputData = SalesforcePayloadUtil.payloadToInputStream(tempPayload);

        // Request body as InputStreamEntity around the inputData InputStream
        InputStreamEntity requestBody = new InputStreamEntity(inputData, ContentType.APPLICATION_XML);

        URI uri = _sfuriBuilder.compositeBatch();

        return ClassicRequestBuilder.post(uri).addHeader(contentXML()).addHeader(acceptXML()).setEntity(requestBody)
                                    .addHeader(assignmentRuleID(ruleID)).build();
    }

    /**
     * Builds the ClassicHttpRequest to test the connection by requesting REST get URLs
     *
     * @return ClassicHttpRequest to test connection. Ready for execution
     */
    public ClassicHttpRequest testConnection() {
        URI uri = _sfuriBuilder.testConnection();
        return ClassicRequestBuilder.get(uri).addHeader(acceptJSON()).build();
    }
}
