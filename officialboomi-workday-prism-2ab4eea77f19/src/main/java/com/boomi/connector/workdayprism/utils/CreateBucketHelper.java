//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.UUID;

/**
 * Delegate class in charge of handling the creation of a new Json payload for Create Bucket operation request.
 * This request is not simple in Workday Prism and the connectors tries to simplify it by defining some of its
 * values as operation fields.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class CreateBucketHelper {

    private static final String ERROR_BLANK_BUCKET_NAME = "the bucket name cannot be blank. Either set the bucket "
            + "name in the profile, or clear the Automatically generate bucket name check box.";

    private static final String VALUE_SCHEMA_VERSION = "Schema_Version=1.0";
    private static final String VALUE_ENCODING = "Encoding=UTF-8";
    private static final String VALUE_SCHEMA_TYPE = "Schema_File_Type=Delimited";

    private static final String FIELD_TARGET_DATASET = "targetDataset";
    private static final String FIELD_SCHEMA = "schema";
    private static final String FIELD_SCHEMA_VERSION = "schemaVersion";
    private static final String FIELD_PARSE_OPTIONS = "parseOptions";
    private static final String FIELD_CHARSET = "charset";
    private static final String FIELD_ENCLOSED_BY = "fieldsEnclosedBy";
    private static final String FIELD_DELIMITED_BY = "fieldsDelimitedBy";
    private static final String FIELD_HEADER_LINES = "headerLinesToIgnore";

    private static final String NAME_PREFIX = "b_";
    private static final String NAME_REPLACE_CHARS = "-";

    private CreateBucketHelper() {
    }

    /**
     * Orchestrates the creation of a new create bucket payload based on the input document and the configuration
     * defined for the Operation.
     *
     * @return a {@link JsonNode} instance with a valid payload to create a bucket
     */
    public static JsonNode buildBucketPayload(PropertyMap properties, String objectTypeId, JsonNode input,
            JsonNode fields) {
        ObjectNode payload = JSONUtil.newObjectNode();
        payload.put(Constants.FIELD_NAME, getName(properties, input, objectTypeId));
        payload.set(Constants.FIELD_OPERATION, getOperation(input));    
        payload.set(FIELD_TARGET_DATASET, getDataset(input, objectTypeId)); 

        ObjectNode schema = getBaseSchema();
        schema.set(FIELD_PARSE_OPTIONS, getSchemaParseOptions(input)); 
        schema.set(Constants.FIELD_FIELDS, getSchemaFields(input, fields));
        payload.set(FIELD_SCHEMA, schema);

        return payload;
    }
    
    /** Helper method to  fetch the name of the bucket from input or generate a random bucket name if it is empty
     * @param properties
     * @param input
     * @param objectTypeId
     * @return String
     */
    private static String getName(PropertyMap properties, JsonNode input, String objectTypeId) {
        String bucketName = input.path(Constants.FIELD_NAME).asText();
        if (StringUtil.isBlank(bucketName)) {
        	Boolean isRandomBucketNameSelected = properties.getBooleanProperty(Constants.FIELD_RANDOM_BUCKET_NAME, false);
            if (Boolean.TRUE.equals(isRandomBucketNameSelected)) {
                bucketName = getRandomBucketName(objectTypeId);
            }
            else {
                throw new ConnectorException(ERROR_BLANK_BUCKET_NAME);
            }
        }
        return bucketName;
    }

    /** Generate a random string using Java randomUUID() method in UUID class, to be used as a bucket name
     * @param id
     * @return String
     */
    private static String getRandomBucketName(String id) {
        return NAME_PREFIX + id + UUID.randomUUID().toString().replace(NAME_REPLACE_CHARS, StringUtil.EMPTY_STRING);
    }

    /** Helper method to append Operation_Type in the create bucket request payload
     * @param input
     * @return ObjectNode
     */
    private static ObjectNode getOperation(JsonNode input) {
        JsonNode operation = input.path(Constants.FIELD_OPERATION);
        if(operation.path(Constants.FIELD_ID).asText().isEmpty()|| operation.path(Constants.FIELD_ID).asText()==null) {
        	return JSONUtil.newObjectNode().put(Constants.FIELD_ID, Constants.FIELD_OPERATION_TYPE_REPLACE);
        }
        return JSONUtil.newObjectNode().put(Constants.FIELD_ID, operation.path(Constants.FIELD_ID).asText());
    }
    
    /** Helper method to append target table details in the create bucket request payload
     * @param input
     * @param objectTypeId
     * @return ObjectNode
     */
    private static ObjectNode getDataset(JsonNode input, String objectTypeId) {
        ObjectNode dataset = JSONUtil.newObjectNode();
        String datasetId = Constants.ID_DYNAMIC_DATASET.equals(objectTypeId) ? input.path(FIELD_TARGET_DATASET).path(Constants.FIELD_ID)
                .asText() : objectTypeId;
        dataset.put(Constants.FIELD_ID, datasetId);
        return dataset;
    }

    /** Creates a new empty schema to be populated with values in the request payload for create bucket 
     * @return ObjectNode
     */
    private static ObjectNode getBaseSchema() {
        ObjectNode schema = JSONUtil.newObjectNode();
        ObjectNode version = schema.putObject(FIELD_SCHEMA_VERSION);
        version.put(Constants.FIELD_ID, VALUE_SCHEMA_VERSION);
        return schema;
    }
    
    /** The method buildSchemaIfInputIsNull() creates a schema in the case of a static bucket from existing tables creation with no input JSON
     * @return an instance of ObjectNode
     */
    public static ObjectNode buildSchemaIfInputIsNull() {
        ObjectNode schema = JSONUtil.newObjectNode();
        schema.put(Constants.FIELD_NAME, "");
        ObjectNode targetDataset = schema.putObject(Constants.FIELD_TARGETDATASET);
        targetDataset.put(Constants.FIELD_ID, "");
        ObjectNode operation = schema.putObject(Constants.FIELD_OPERATION);
        operation.put(Constants.FIELD_ID, Constants.FIELD_OPERATION_TYPE_REPLACE);
        return schema;
    }

    /** Create "schemaPrseOptions" object to be put inside in the request payload for create bucket 
     * @param input
     * @return JsonNode
     */
    private static JsonNode getSchemaParseOptions(JsonNode input) {

        String enclosing=null; 
        String delimiter=null;
        int headerLines=0;
        String enclosingDefault = Constants.FIELD_ENCLOSING_VALUE;
        String delimiterDefault = Constants.FIELD_DELIMITER_VALUE;
        int headerLinesDefault = Constants.FIELD_HEADER_LINES_VALUE;

        JsonNode inputParseOptions = input.findPath(FIELD_PARSE_OPTIONS);
        enclosing = getStringField(inputParseOptions, FIELD_ENCLOSED_BY, enclosingDefault);
        delimiter = getStringField(inputParseOptions, FIELD_DELIMITED_BY, delimiterDefault);
        headerLines = getIntField(inputParseOptions, FIELD_HEADER_LINES, headerLinesDefault);
        
        ObjectNode parseOptions = JSONUtil.newObjectNode();
        ObjectNode charset = parseOptions.putObject(FIELD_CHARSET);
        charset.put(Constants.FIELD_ID, VALUE_ENCODING);
        parseOptions.put(FIELD_ENCLOSED_BY, enclosing);
        parseOptions.put(FIELD_DELIMITED_BY, delimiter);
        parseOptions.put(FIELD_HEADER_LINES, headerLines);
        ObjectNode type = parseOptions.putObject(Constants.FIELD_TYPE); 
        type.put(Constants.FIELD_ID, VALUE_SCHEMA_TYPE);

        return parseOptions;
    }

    private static String getStringField(JsonNode src, String key, String defaultValue){
        JsonNode field = src.findPath(key);
        return field.isMissingNode()? defaultValue : field.asText();
    }

    private static int getIntField(JsonNode src, String key, int defaultValue){
        JsonNode field = src.findPath(key);
        return field.isMissingNode()? defaultValue : field.asInt();
    }

    private static ArrayNode getSchemaFields(JsonNode input, JsonNode storedFields) {
        // We don't have JSONUtil.newArrayNode(), but it would be equivalent to this:
        ArrayNode fields = new ArrayNode(JsonNodeFactory.instance);

        // if the fields from the cookie are present, it just include that
        if (storedFields != null && storedFields.path(Constants.FIELD_FIELDS).isArray()) {
            fields.addAll((ArrayNode) storedFields.path(Constants.FIELD_FIELDS));
            return fields;
        }

        // otherwise it needs to process the input
        JsonNode srcFields = input.findPath(Constants.FIELD_FIELDS);
        if (!srcFields.isArray()) {
            throw new ConnectorException(Constants.ERROR_WRONG_INPUT_PROFILE);
        }
        completeFields(fields, (ArrayNode) srcFields);
        return fields;
    }

    /** Helper method to put values and form the final payload
     * @param newFields
     * @param srcFields
     */
    private static void completeFields(ArrayNode newFields, ArrayNode srcFields) {
        for (int i = 0; i < srcFields.size(); i++) {
            ObjectNode field = newFields.addObject();
            JsonNode srcField = srcFields.get(i);

            putStringIfPresent(field, Constants.FIELD_NAME, srcField);
            putStringIfPresent(field, Constants.FIELD_DESCRIPTION, srcField);
            putStringIfPresent(field, Constants.FIELD_DEFAULT_VALUE, srcField);
            putStringIfPresent(field, Constants.FIELD_PARSE_FORMAT, srcField);
            putIntegerIfPresent(field, Constants.FIELD_ORDINAL, srcField);
            putIntegerIfPresent(field, Constants.FIELD_PRECISION, srcField);
            putIntegerIfPresent(field, Constants.FIELD_SCALE, srcField);

            ObjectNode type = field.putObject(Constants.FIELD_TYPE);
            type.put(Constants.FIELD_ID,srcField.path(Constants.FIELD_TYPE).path(Constants.FIELD_ID).asText());
            type.put(Constants.FIELD_DESCRIPTOR,srcField.path(Constants.FIELD_TYPE).path(Constants.FIELD_DESCRIPTOR).asText());
           
        }
    }

    private static void putStringIfPresent(ObjectNode dest, String name, JsonNode source) {
        JsonNode field = source.path(name);
        if (!field.isMissingNode()) {
            dest.put(name, field.asText());
        }
    }

    private static void putIntegerIfPresent(ObjectNode dest, String name, JsonNode source) {
        JsonNode field = source.path(name);
        if (!field.isMissingNode()) {
            dest.put(name, field.asInt());
        }
    }

    /** This method extractFieldArrayString() helps in extracting the field schema from existing tables
     * @param schema an instance of JsonNode
     * @return a String representation of the extracted Schema
     */
    public static String extractFieldArrayString(JsonNode schema) {
		ObjectNode validSchema=JSONUtil.newObjectNode();
		ArrayNode fieldArrayNode=(ArrayNode)schema.findPath(Constants.FIELD_FIELDS);
		JsonNodeFactory factory = JsonNodeFactory.instance;
		ArrayNode newArrayNode = new ArrayNode(factory);
		
		for(int i=0; i<fieldArrayNode.size(); i++) {
			if (fieldArrayNode.get(i).hasNonNull(Constants.FIELD_NAME) &&
					!(fieldArrayNode.get(i).get(Constants.FIELD_NAME).asText().contains(Constants.FIELD_WPA_COLUMN))) {
				newArrayNode.add(fieldArrayNode.get(i));
		    }
		}
		validSchema.set(Constants.FIELD_FIELDS,newArrayNode);
		return validSchema.toString();
	}

}
