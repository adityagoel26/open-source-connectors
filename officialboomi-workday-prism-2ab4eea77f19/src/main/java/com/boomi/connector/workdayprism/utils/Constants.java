//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.ObjectType;

/**
 * Holds constants value that will be accessed from different classes.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class Constants {

    public static final String ENTITY_BUCKET = "bucket";
    public static final String ENTITY_DATASET = "dataset";
    public static final String ENTITY_STATIC_BUCKET = "static_bucket";

    public static final String PROPERTY_ENTITY_TYPE = "entity_type";

    public static final String TABLES_ENDPOINT = "datasets";
    public static final String DESCRIBE_TABLES_ENDPOINT = TABLES_ENDPOINT +"/%s/describe";
    public static final String BUCKETS_ENDPOINT = "wBuckets";
    public static final String COMPLETE_BUCKET_ENDPOINT = BUCKETS_ENDPOINT +"/%s/complete";
    public static final String UPLOAD_FILE_ENDPOINT = "/service/" + BUCKETS_ENDPOINT + "/%s/files";

    public static final String AUTHORIZATION_BEARER_PATTERN = "Bearer %s";
    public static final String AUTHORIZATION_BASIC_PATTERN = "Basic %s";

    public static final String COMPLETE_BUCKET_CUSTOM_TYPE_ID = "complete_bucket";
    
    public static final String IMPORT_CUSTOM_TYPE_ID = "import";


    public static final String FIELD_RANDOM_BUCKET_NAME = "random_bucket_name";
    public static final String FIELD_WPA_COLUMN = "WPA_";
    public static final String FIELD_ENCLOSING = "enclosing";
    public static final String FIELD_DELIMITER = "delimiter";
    public static final String FIELD_HEADER_LINES = "header_lines";
    public static final String FIELD_ENCLOSING_VALUE = "\"";
    public static final String FIELD_DELIMITER_VALUE = ",";
    public static final int FIELD_HEADER_LINES_VALUE = 1;
    public static final String FIELD_BUCKET_ID = "id";
    public static final String FIELD_FILENAME = "filename";
    public static final String FIELD_ENCODING = "encoding";
    public static final String FIELD_MAX_FILE_SIZE = "max_file_size";

    public static final String FIELD_STATE = "state";
    public static final String FIELD_DESCRIPTOR = "descriptor";

    static final String PREFIX_SCHEMA_FIELD_TYPE = "Schema_Field_Type=";

    public static final String FIELD_NAME = "name";
    static final String FIELD_DISPLAY_NAME = "displayName";
    static final String FIELD_OPERATION = "operation";
    static final String FIELD_OPERATION_TYPE_REPLACE = "Operation_Type=Replace";
    static final String FIELD_TARGETDATASET = "targetDataset";
    static final String FIELD_ID = "id";
    static final String FIELD_ESCRIPTOR = "descriptor";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_DESCRIPTION = "description";
    public static final String FIELD_FIELDS = "fields";
    static final String FIELD_DEFAULT_VALUE = "defaultValue";
    static final String FIELD_PARSE_FORMAT = "parseFormat";
    static final String FIELD_ORDINAL = "ordinal";
    static final String FIELD_PRECISION = "precision";
    static final String FIELD_SCALE = "scale";
    public static final String FIELD_FILE = "file";

    public static final String ID_DYNAMIC_DATASET = "DYNAMIC_DATASET";
    private static final String LABEL_BUCKET = "Bucket";
    private static final String LABEL_DATASET = "Dataset";
    private static final String LABEL_DYNAMIC_DATASET = "Dynamic Dataset";

    public static final ObjectType OBJECT_TYPE_BUCKET = new ObjectType().withId(ENTITY_BUCKET).withLabel(LABEL_BUCKET);
    public static final ObjectType OBJECT_TYPE_DATASET = new ObjectType().withId(ENTITY_DATASET)
            .withLabel(LABEL_DATASET);
    public static final ObjectType OBJECT_TYPE_DYNAMIC_DATASET = new ObjectType().withId(ID_DYNAMIC_DATASET)
            .withLabel(LABEL_DYNAMIC_DATASET);

    public static final String ERROR_WRONG_INPUT_PROFILE = "the input profile is not valid";
	public static final String CREATED_MOMENT = "createdMoment";

    private Constants(){
    }
}
