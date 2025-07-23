// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.constant;

import org.apache.hc.core5.http.ContentType;

public class Constants {

    public static final int OK_GET_CODE = 200;
    public static final int OK_POST_CODE = 201;
    public static final int OK_NO_CONTENT = 204;
    public static final int SESSION_EXPIRES_CODE = 401;
    public static final int URI_LENGTH_LIMIT_EXCEEDED = 414;
    public static final int HEADER_LENGTH_LIMIT_EXCEEDED = 431;

    public static final int MAX_SOQL_REST = 16384;
    public static final int MAX_SOQL_BULK = 100000;

    public static final String SOAP_LOGIN_FILE = "/soap-login.xml";
    public static final String SOAP_LOGIN_USERNAME = "n1:username";
    public static final String SOAP_LOGIN_PASSWORD = "n1:password";

    public static final String QUERYABLE = "queryable";
    public static final String DELETABLE = "deletable";
    public static final String CREATEABLE = "createable";
    public static final String UPDATEABLE = "updateable";
    public static final String API_LIMIT = "apilimit";

    public static final String QUERY_ALL_DESCRIPTOR = "queryAll";
    public static final String LOG_SOQL_DESCRIPTOR = "logSoql";
    public static final String LIMIT_NUMBER_OF_DOCUMENTS_DESCRIPTOR = "limitNumberOfDocuments";
    public static final String NUMBER_OF_DOCUMENTS_DESCRIPTOR = "limit";
    public static final String PARENT_DEPTH_DESCRIPTOR = "parentsDepth";
    public static final String CHILDREN_DEPTH_DESCRIPTOR = "childrenDepth";
    public static final String BULK_CSV_CUSTOM_DESCRIPTOR = "csvBulkApiV2";
    public static final String CREATE_TREE_CUSTOM_DESCRIPTOR = "CREATE_TREE";
    public static final String QUERY_PAGE_SIZE_DESCRIPTOR = "pageSize";
    public static final String BULK_BATCH_SIZE_DESCRIPTOR = "batchSize";
    public static final String COMPOSITE_BATCH_COUNT_DESCRIPTOR = "batchCount";
    public static final String BULK_HEADER_DESCRIPTOR = "bulkHeader";
    public static final String REST_HEADERS_DESCRIPTOR = "restHeaders";
    public static final String ALL_OR_NONE_DESCRIPTOR = "allOrNone";
    public static final String RETURN_UPDATED_RECORD_DESCRIPTOR = "returnUpdatedRecord";

    public static final String CONTENT_TYPE_BULKV2 = "contentType";
    public static final String JSON = ContentType.APPLICATION_JSON.toString();
    public static final String XML = ContentType.APPLICATION_XML.toString();
    public static final String TEXT_XML = ContentType.TEXT_XML.toString();
    public static final String CSV = "text/csv";

    public static final String ASSIGNMENT_RULE_ID_DESCRIPTOR = "assignmentRuleId";
    public static final String COLUMN_DELIMITER_BULKV2 = "columnDelimiter";
    public static final String CONTENT_CSV_BULKV2 = "CSV";
    public static final String EXTERNAL_ID_FIELD_BULKV2 = "externalIdFieldName";
    public static final String LINE_ENDING_BULKV2 = "lineEnding";
    public static final String OBJECT_SOBJECT = "object";
    public static final String OPERATION_BULKV2 = "operation";
    public static final String STATE_BULKV2 = "state";
    public static final String UPLOAD_COMPLETE_BULKV2 = "UploadComplete";
    public static final String PAGE_ID_BULKV2 = "locator";
    public static final String PAGE_SIZE_BULKV2 = "maxRecords";
    public static final String EXTERNAL_ID_VALUE = "externalIdValue";

    public static final String OPERATION_BOOMI = "operationBoomi";

    public static final String INSERT_BULKV2 = "insert";
    public static final String UPDATE_BULKV2 = "update";
    public static final String UPSERT_BULKV2 = "upsert";
    public static final String DELETE_BULKV2 = "delete";
    public static final String QUERY_BULKV2 = "query";
    public static final String QUERY_ALL_BULKV2 = "queryAll";

    public static final String AUTHORIZATION_REQUEST = "Authorization";
    public static final String BEARER_REQUEST = "Bearer ";
    public static final String ACCEPT_REQUEST = "Accept";
    public static final String QUERY_BATCH_KEY_REQUEST = "Sforce-Query-Options";
    public static final String API_LIMIT_KEY_RESPONSE = "Sforce-Limit-Info";
    public static final String ASSIGNMENT_RULE_ID_REST_HEADER = "Sforce-Auto-Assign";
    public static final String QUERY_BATCH_VALUE_REQUEST = "batchSize=";

    public static final String CONTENT_TYPE_REQUEST = "Content-Type";
    public static final String SOAP_ACTION_REQUEST = "SOAPAction";
    public static final String LOGIN_ACTION_REQUEST = "login";

    public static final String HTTPS = "https";

    public static final String SOBJECTS_URI = "/sobjects";
    public static final String DESCRIBE_URI = "/describe";
    public static final String QUERY_URI = "/query";
    public static final String QUERY_PARAMETER = "q";
    public static final String QUERY_ALL_URI = "/queryAll";

    public static final String CREATE_JOB_URI = "/jobs/ingest";
    public static final String BATCH_JOB_URI = "/batches";
    public static final String SUCCESS_JOB_URI = "/successfulResults";
    public static final String FAILED_JOB_URI = "/failedResults";

    public static final String CREATE_QUERY_JOB_URI = "/jobs/query";
    public static final String QUERY_RESULT_JOB_URI = "/results";

    public static final String SALESFORCE_RECORDS = "records";
    public static final String SALESFORCE_NEXT_PAGE_URL = "nextRecordsUrl";
    public static final String COMPOSITE_RESULT = "result";
    public static final String COMPOSITE_URI = "/composite";

    private Constants() {
    }
}
