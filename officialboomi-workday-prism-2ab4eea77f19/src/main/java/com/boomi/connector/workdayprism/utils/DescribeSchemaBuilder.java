//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.workdayprism.responses.DescribeTableResponse;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Delegate class to encapsulate the process of a {@link DescribeDatasetResponse} and form a new {@link JsonNode} to be
 * stored as the INPUT role cookie of a Create Bucket operation.
 *
 * It's worth to mention that the a Dataset schema concept in Workday Prism is somehow disjointed as it is not defined
 * when the Dataset is created because it will take it from the last Bucket used to upload data. As a consequence of
 * this, the Describe Dataset API might not return return a schema at all and in that case the user will need to provide
 * it as an operation input.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class DescribeSchemaBuilder {

    private static final String ERROR_NO_SCHEMA = "the dataset does not contains a schema";

    private DescribeSchemaBuilder() {
    }

    /**
     * Returns a Json representation of the the schema fields as they will be expected by the Create Bucket endpoint. A
     * small processing is needed because the response from the Describe Dataset API does not have the exact structure
     * expected as an input.
     *
     * @param response
     *         a {@link DescribeDatasetResponse} instance.
     * @return an {@link JsonNode} holding the current schema fields.
     */
    public static JsonNode getSchema(DescribeTableResponse response) {
        if (response == null || response.getData() == null) {
            throw new ConnectorException(ERROR_NO_SCHEMA);
        }
        return buildJsonSchema(response.getData());
    }

    private static JsonNode buildJsonSchema(DescribeTableResponse.Data source) {
        ObjectNode schema = JSONUtil.newObjectNode();
        if (CollectionUtil.isEmpty(source.getFields())) {
            throw new ConnectorException(Constants.ERROR_WRONG_INPUT_PROFILE);
        }
        ArrayNode fieldArray = schema.putArray(Constants.FIELD_FIELDS);
        for (DescribeTableResponse.Field field : source.getFields()) {
            fieldArray.add(getJsonField(field));
        }
        return schema;
    }
    
    private static ObjectNode getJsonField(DescribeTableResponse.Field source) {
        ObjectNode dest = JSONUtil.newObjectNode();
        putStringIfPresent(dest, Constants.FIELD_NAME, source.getName());
        putStringIfPresent(dest, Constants.FIELD_DESCRIPTION, source.getDescription());
        putStringIfPresent(dest, Constants.FIELD_DEFAULT_VALUE, source.getDefaultValue());
        putStringIfPresent(dest, Constants.FIELD_PARSE_FORMAT, source.getParseFormat()); 

        putIntegerIfPresent(dest, Constants.FIELD_ORDINAL, source.getOrdinal());
        putIntegerIfPresent(dest, Constants.FIELD_PRECISION, source.getPrecision());
        putIntegerIfPresent(dest, Constants.FIELD_SCALE, source.getScale());

        ObjectNode type = dest.putObject(Constants.FIELD_TYPE);
        putStringIfPresent(type, Constants.FIELD_ID, source.getType().getId());
        putStringIfPresent(type, Constants.FIELD_DESCRIPTOR, Constants.PREFIX_SCHEMA_FIELD_TYPE + source.getType().getDescriptor());

        return dest;
    }

    private static void putStringIfPresent(ObjectNode dest, String name, String value) {
        if (StringUtil.isNotBlank(value)) {
            dest.put(name, value);
        }
    }

    private static void putIntegerIfPresent(ObjectNode dest, String name, Integer value) {
        if (value != null) {
            dest.put(name, value);
        }
    }

}
