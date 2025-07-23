// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SFURIBuilderTest {

    @Test
    void soapLogin() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.soapLogin();
        assertEquals("https://login.salesforce.com/services/Soap/u/48.0", result.toString());
    }

    @Test
    void listSObjects() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.listSObjects();
        assertEquals("https://example.com/sobjects", result.toString());
    }

    @Test
    void describeSObject() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.describeSObject("theObject");
        assertEquals("https://example.com/sobjects/theObject/describe", result.toString());
    }

    @Test
    void querySOQL() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.querySOQL("theSQL");
        assertEquals("https://example.com/query?q=theSQL", result.toString());
    }

    @Test
    void querySOQLNextPage() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.querySOQLNextPage("the-next-page");
        assertEquals("https://example.com/the-next-page", result.toString());
    }

    @Test
    void queryAllSOQL() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.queryAllSOQL("theSQL");
        assertEquals("https://example.com/queryAll?q=theSQL", result.toString());
    }

    @Test
    void buildJob() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.buildJob();
        assertEquals("https://example.com/jobs/ingest", result.toString());
    }

    @Test
    void buildQueryJob() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.buildQueryJob();
        assertEquals("https://example.com/jobs/query", result.toString());
    }

    @Test
    void uploadBatchBulk() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.uploadBatchBulk("theJobID");
        assertEquals("https://example.com/jobs/ingest/theJobID/batches", result.toString());
    }

    @Test
    void finishBatchBulk() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.finishBatchBulk("theJobID");
        assertEquals("https://example.com/jobs/ingest/theJobID", result.toString());
    }

    @Test
    void getBulkStatus() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.getBulkStatus("theJobID");
        assertEquals("https://example.com/jobs/ingest/theJobID", result.toString());
    }

    @Test
    void bulkQueryStatus() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.bulkQueryStatus("theJobID");
        assertEquals("https://example.com/jobs/query/theJobID", result.toString());
    }

    @Test
    void bulkSuccessResult() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.bulkSuccessResult("theJobID");
        assertEquals("https://example.com/jobs/ingest/theJobID/successfulResults", result.toString());
    }

    @Test
    void bulkQueryResult() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.bulkQueryResult("theJobID", "theLocator", 42L);
        assertEquals("https://example.com/jobs/query/theJobID/results?locator=theLocator&maxRecords=42",
                result.toString());
    }

    @Test
    void createRecord() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.createRecord("theObjectName");
        assertEquals("https://example.com/sobjects/theObjectName", result.toString());
    }

    @Test
    void createTree() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.createTree("theObjectName");
        assertEquals("https://example.com/composite/tree/theObjectName", result.toString());
    }

    @Test
    void deleteRecord() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.deleteRecord("theObjectName", "theRecordID");
        assertEquals("https://example.com/sobjects/theObjectName/theRecordID", result.toString());
    }

    @Test
    void updateRecord() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.updateRecord("theObjectName", "theRecordID");
        assertEquals("https://example.com/sobjects/theObjectName/theRecordID", result.toString());
    }

    @Test
    void upsertRecordUpdate() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.upsertRecord("theObjectName", "theExternalField", "theExternalID");
        assertEquals("https://example.com/sobjects/theObjectName/theExternalField/theExternalID", result.toString());
    }

    @Test
    void upsertRecordInsert() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.upsertRecord("theObjectName", "theExternalField", "");
        assertEquals("https://example.com/sobjects/theObjectName/theExternalField/", result.toString());
    }

    @Test
    void compositeCollection() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.compositeCollection();
        assertEquals("https://example.com/composite/sobjects", result.toString());
    }

    @Test
    void compositeUpsertCollection() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.compositeUpsertCollection("theObjectName", "theExternalField");
        assertEquals("https://example.com/composite/sobjects/theObjectName/theExternalField", result.toString());
    }

    @Test
    void compositeDeleteCollectionWithAllOrNoneDisabled() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.compositeDeleteCollection("theDeleteParameters", false);
        assertEquals("https://example.com/composite/sobjects?ids=theDeleteParameters", result.toString());
    }

    @Test
    void compositeDeleteCollectionWithAllOrNoneEnabled() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.compositeDeleteCollection("theDeleteParameters", true);
        assertEquals("https://example.com/composite/sobjects?ids=theDeleteParameters&allOrNone=true",
                result.toString());
    }

    @Test
    void testConnection() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.testConnection();
        assertEquals("https://example.com", result.toString());
    }

    @Test
    void getUpdateReturnUpdatedRecordURI() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com/services/data/"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        String result = builder.getUpdateReturnUpdatedRecordURI("theObjectName", "theRecordID");
        assertEquals("/sobjects/theObjectName/theRecordID", result);
    }

    @Test
    void getRecordURI() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com/services/data/"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        String result = builder.getRecordURI("theObjectName", "theRecordID");
        assertEquals("/sobjects/theObjectName/theRecordID", result);
    }

    @Test
    void compositeBatch() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.compositeBatch();
        assertEquals("https://example.com/composite/batch", result.toString());
    }

    @Test
    void bulkFailedResult() throws URISyntaxException {
        SFURIBuilder builder = new SFURIBuilder(new URI("https://example.com"),
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        URI result = builder.bulkFailedResult("theJobID");
        assertEquals("https://example.com/jobs/ingest/theJobID/failedResults", result.toString());
    }
}
