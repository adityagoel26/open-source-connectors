// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.constant.Constants;

import org.apache.hc.core5.net.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Responsible for building URIs to Salesforce API
 */
public class SFURIBuilder {

    private final URI _baseURI;
    private final URI _authenticationURI;

    /**
     * Constructs a new SFURIBuilder with base and authentication URIs.
     *
     * @param baseURI           The base service URI for Salesforce REST endpoints
     * @param authenticationURI The authentication endpoint URI for Salesforce
     * @throws NullPointerException if either baseURI or authenticationURI is null
     */
    public SFURIBuilder(URI baseURI, URI authenticationURI) {
        _baseURI = Objects.requireNonNull(baseURI, "Service URL cannot be null.");
        _authenticationURI = Objects.requireNonNull(authenticationURI, "Authentication URL cannot be null.");
    }

    /**
     * Builds URI quietly given URIBuilder and catching URISyntaxException
     *
     * @param builder already build URI in URIBuilder
     * @param message error message in Exception cases
     * @return URI the target URI ready for execution
     */
    private static URI buildURI(URIBuilder builder, String message) {
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            throw new ConnectorException("Errors occurred while building " + message + " url. " + e.getMessage(), e);
        }
    }

    /**
     * Returns the authentication URI for SOAP login endpoint.
     *
     * @return URI The authentication endpoint URI for Salesforce SOAP login
     */
    public URI soapLogin() {
        return _authenticationURI;
    }

    /**
     * Builds URI to list all SObjects metadata
     *
     * @return uri to be used to list of all SObjects metadata
     */
    public URI listSObjects() {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(_baseURI.getPath() + Constants.SOBJECTS_URI);
        return buildURI(builder, "list all SObjects");
    }

    /**
     * Builds URI to metadata describe/list all fields of a SObject
     *
     * @param sobjectName target SObject name
     * @return uri to be used to describe the SObject
     */
    public URI describeSObject(String sobjectName) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.SOBJECTS_URI + "/" + sobjectName + Constants.DESCRIBE_URI);
        return buildURI(builder, "describe " + sobjectName);
    }

    /**
     * Builds URI to execute SOQL query
     *
     * @param queryString SOQL query string
     * @return uri to be used to execute SOQL query
     */
    public URI querySOQL(String queryString) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(_baseURI.getPath() + Constants.QUERY_URI).setParameter(
                Constants.QUERY_PARAMETER, queryString);
        return buildURI(builder, "execute SOQL");
    }

    /**
     * Builds URI to retrieve next page of SOQL query
     *
     * @param nextPageUrl the nextPageUrl
     * @return uri to be used to execute QUERY next page
     */
    public URI querySOQLNextPage(String nextPageUrl) {
        URIBuilder builder = new URIBuilder().setScheme(Constants.HTTPS).setHost(_baseURI.getAuthority()).setPath(
                nextPageUrl);
        return buildURI(builder, "execute QUERY next Page");
    }

    /**
     * Builds URI to execute SOQL queryAll
     *
     * @param queryString SOQL query string
     * @return uri to be used to execute SOQL queryAll
     */
    public URI queryAllSOQL(String queryString) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(_baseURI.getPath() + Constants.QUERY_ALL_URI)
                .setParameter(Constants.QUERY_PARAMETER, queryString);
        return buildURI(builder, "execute SOQL");
    }

    /**
     * Builds URI to be used to build job in Bulk API V2
     *
     * @return uri to execute build BULK V2 job
     */
    public URI buildJob() {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(_baseURI.getPath() + Constants.CREATE_JOB_URI);
        return buildURI(builder, "build Bulk V2 Job");
    }

    /**
     * Builds URI to be used to build QUERY job in Bulk API V2
     *
     * @return uri to execute build QUERY BULK V2 job
     */
    public URI buildQueryJob() {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(_baseURI.getPath() + Constants.CREATE_QUERY_JOB_URI);
        return buildURI(builder, "build Query Bulk V2 Job");
    }

    /**
     * Builds URI to be used to upload data in Bulk API V2
     *
     * @param jobID Bulk Job Id
     * @return uri to upload batch data in BULK V2 job
     */
    public URI uploadBatchBulk(String jobID) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_JOB_URI + "/" + jobID + Constants.BATCH_JOB_URI);
        return buildURI(builder, "upload Bulk batch data");
    }

    /**
     * Builds URI to be used to finish upload data in Bulk API V2
     *
     * @param jobID Bulk Job Id
     * @return uri to finish upload batch data in BULK V2 job
     */
    public URI finishBatchBulk(String jobID) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_JOB_URI + "/" + jobID);
        return buildURI(builder, "finish upload Bulk batch data");
    }

    /**
     * Builds URI to be used to get status of a job in Bulk API V2
     *
     * @param jobID Bulk Job Id
     * @return uri to get status in BULK V2 job
     */
    public URI getBulkStatus(String jobID) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_JOB_URI + "/" + jobID);
        return buildURI(builder, "get Bulk status");
    }

    /**
     * Builds URI to be used to get status of a QUERY job in Bulk API V2
     *
     * @param jobID Bulk Job Id
     * @return uri to get status in BULK QUERY V2 job
     */
    public URI bulkQueryStatus(String jobID) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_QUERY_JOB_URI + "/" + jobID);
        return buildURI(builder, "get Bulk QUERY status");
    }

    /**
     * Builds URI to be used to get successful results of a job in Bulk API V2
     *
     * @param jobID Bulk Job Id
     * @return uri to get successful results in BULK V2 job
     */
    public URI bulkSuccessResult(String jobID) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_JOB_URI + "/" + jobID + Constants.SUCCESS_JOB_URI);
        return buildURI(builder, "get Bulk Results");
    }

    /**
     * Builds URI to be used to get results of a QUERY job in Bulk API V2
     *
     * @param jobID    Bulk Job Id
     * @param pageSize maximum records returned in one request. Can be null
     * @param locator  locator works as query page url. Can be null
     * @return uri to get successful results in BULK V2 Query job
     */
    public URI bulkQueryResult(String jobID, String locator, Long pageSize) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_QUERY_JOB_URI + "/" + jobID + Constants.QUERY_RESULT_JOB_URI);
        if (locator != null && !"null".equals(locator)) {
            builder.addParameter(Constants.PAGE_ID_BULKV2, locator);
        }
        if (pageSize != null) {
            builder.addParameter(Constants.PAGE_SIZE_BULKV2, pageSize.toString());
        }
        return buildURI(builder, "get Query Bulk Results");
    }

    /**
     * Builds URI to be used to get failed results of a job in Bulk API V2
     *
     * @param jobID Bulk Job Id
     * @return uri to get failed results in BULK V2 job
     */
    public URI bulkFailedResult(String jobID) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.CREATE_JOB_URI + "/" + jobID + Constants.FAILED_JOB_URI);
        return buildURI(builder, "get Bulk Results");
    }

    /**
     * Builds URI to be used to post CREATE operation in REST API
     *
     * @param sobjectName target SObject name
     * @return uri to a create record request
     */
    public URI createRecord(String sobjectName) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.SOBJECTS_URI + "/" + sobjectName);
        return buildURI(builder, "CREATE record");
    }

    /**
     * Builds URI to be used to post CREATE TREE operation in Composite API
     *
     * @param sobjectName target SObject name
     * @return uri to a create record request
     */
    public URI createTree(String sobjectName) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.COMPOSITE_URI + "/tree" + "/" + sobjectName);
        return buildURI(builder, "CREATE TREE");
    }

    /**
     * Builds URI to be used to DELETE operation in REST API
     *
     * @param sobjectName target SObject name
     * @param recordId    Id of the deleted record
     * @return uri to a delete record request
     */
    public URI deleteRecord(String sobjectName, String recordId) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.SOBJECTS_URI + "/" + sobjectName + "/" + recordId);
        return buildURI(builder, "DELETE record");
    }

    /**
     * Builds URI to be used to UPDATE operation in REST API
     *
     * @param sobjectName target SObject name
     * @param recordId    Id of the deleted record
     * @return uri to a update record request
     */
    public URI updateRecord(String sobjectName, String recordId) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.SOBJECTS_URI + "/" + sobjectName + "/" + recordId);
        return buildURI(builder, "UPDATE record");
    }

    /**
     * Builds URI to be used to UPSERT operation in REST API
     *
     * @param sobjectName     target SObject name
     * @param externalIdField the field selected as External ID Field
     * @param externalIdValue the value of the External ID Field
     * @return uri to a upsert record request
     */
    public URI upsertRecord(String sobjectName, String externalIdField, String externalIdValue) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.SOBJECTS_URI + "/" + sobjectName + "/" + externalIdField + "/"
                        + externalIdValue);
        return buildURI(builder, "UPSERT record");
    }

    public URI compositeCollection() {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.COMPOSITE_URI + Constants.SOBJECTS_URI);
        return buildURI(builder, "Composite collection");
    }

    public URI compositeUpsertCollection(String sobjectName, String externalIdField) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.COMPOSITE_URI + Constants.SOBJECTS_URI + "/" + sobjectName + "/"
                        + externalIdField);
        return buildURI(builder, "Composite upsert collection");
    }

    public URI compositeDeleteCollection(String deleteParameters, Boolean allOrNone) {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(
                _baseURI.getPath() + Constants.COMPOSITE_URI + Constants.SOBJECTS_URI).addParameter("ids",
                deleteParameters);
        if (allOrNone) {
            builder.addParameter("allOrNone", "true");
        }
        return buildURI(builder, "Composite delete collection");
    }

    public URI testConnection() {
        return _baseURI;
    }

    public String getUpdateReturnUpdatedRecordURI(String sobjectName, String recordId) {
        URI updateURI = updateRecord(sobjectName, recordId);

        String url = updateURI.toASCIIString();
        int idx = url.indexOf("services/data/");
        return url.substring(idx + "services/data/".length());
    }

    public String getRecordURI(String sobjectName, String recordId) {
        return getUpdateReturnUpdatedRecordURI(sobjectName, recordId);
    }

    public URI compositeBatch() {
        URIBuilder builder = new URIBuilder(_baseURI).setPath(_baseURI.getPath() + Constants.COMPOSITE_URI + "/batch");
        return buildURI(builder, "Composite batch");
    }
}
