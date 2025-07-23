// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq;

/**
 * @author Rohan Jain
 */
public class GoogleBqConstants {

    public static final String DYNAMIC_OBJECT_ID = "DYNAMIC";
    public static final String NODE_TABLE_REFERENCE = "tableReference";
    public static final String GENERIC_TABLE_ID = "GENERIC";

    public static final String CUSTOM_OP_STREAMING = "STREAMING";
    public static final String CUSTOM_OP_RUN_JOB = "RUN_JOB";
    public static final String CUSTOM_OP_UPDATE = "UPDATE";
    public static final String CUSTOM_OP_QUERY_RESULTS = "QUERY_RESULTS";
    public static final String CUSTOM_OP_TEST = "TEST";
    public static final String CUSTOM_OP_UPSERT = "UPSERT";

    public static final String DATASET_TABLES_URL_SUFFIX = "datasets/%s/tables/%s";

    public static final String SECURITY_TYPE_CLIENT_CREDENTIALS = "CLIENT_CREDENTIALS";

    public static final String RESOURCE_TYPE_TABLE = "TABLE";
    public static final String RESOURCE_TYPE_VIEW = "VIEW";

    public static final String PROP_PROJECT_ID = "projectId";
    public static final String PROP_REQUEST_TIMEOUT = "requestTimeout";
    public static final String PROP_DATASET_ID = "datasetId";
    public static final String PROP_RESOURCE_TYPE = "resourceType";
    public static final String PROP_BATCH_COUNT = "batchCount";
    public static final String PROP_SKIP_INVALID_ROWS = "skipInvalidRows";
    public static final String PROP_IGNORE_UNKNOWN_VALUES = "ignoreUnknownValues";
    public static final String PROP_TEMPLATE_SUFFIX = "templateSuffix";
    public static final String PROP_IS_UPDATE = "update";
    public static final String PROP_TIMEOUT = "timeoutMs";
    public static final String PROP_MAX_RESULTS = "maxResults";
    public static final String PROP_GENERATE_INSERT_ID = "generateInsertId";

    public static final String FIELD_MODE_REPEATED = "REPEATED";

    public static final String FIELD_TYPE_BYTES = "BYTES";
    public static final String FIELD_TYPE_INTEGER = "INTEGER";
    public static final String FIELD_TYPE_IN64 = "IN64";
    public static final String FIELD_TYPE_FLOAT = "FLOAT";
    public static final String FIELD_TYPE_FLOAT64 = "FLOAT64";
    public static final String FIELD_TYPE_BOOLEAN = "BOOLEAN";
    public static final String FIELD_TYPE_BOOL = "BOOL";
    public static final String FIELD_TYPE_RECORD = "RECORD";
    public static final String FIELD_TYPE_STRUCT = "STRUCT";
    public static final String FIELD_TYPE_TIMESTAMP = "TIMESTAMP";
    public static final String FIELD_TYPE_DATE = "DATE";
    public static final String FIELD_TYPE_TIME = "TIME";
    public static final String FIELD_TYPE_DATETIME = "DATETIME";
    public static final String FIELD_TYPE_STRING = "STRING";

    public static final String BIG_QUERY_DISCOVERY_REST_URL =
            "https://www.googleapis.com/discovery/v1/apis/bigquery/v2/rest";

    public static final String ERROR_UNSUPPORTED_OPERATION_TYPE = " Operation type %s unsupported";
    public static final String ERROR_EMPTY_CUSTOM_OPERATION_TYPE =
            "This connector only supports custom operations. Custom operation required.";
    public static final String ERROR_STREAM_DATA =
            "Error occurred when streaming data. Location : %s. Reason : %s. Message : %s";
    public static final String ERROR_PARSE_JSON = "Unable to parse input document as a valid json document";

    private GoogleBqConstants() {
    }
}
