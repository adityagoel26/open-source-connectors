// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.mongodb.util;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.DOUBLE_QUOTE;

import java.io.IOException;

import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.boomi.connector.api.ConnectorException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Utility class to create JSON Schema
 *
 */
public class JsonSchemaUtil {
    
    /**
     * Instantiates a new JsonSchemaUtil.
     */
    private JsonSchemaUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Creates the JSON Schema.
     *
     * @param doc
     *            the doc
     * @return the string
     */
    public static String createJsonSchema(Document doc) {
        try {
            JsonWriterSettings jsonWriterSettings;
            String json;

            if (doc == null) {
                throw new ConnectorException("JSON document is null!");
            }
            jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.STRICT).build();
            json = doc.toJson(jsonWriterSettings);
            JsonParser parser = getJsonParser(json);

            return JsonSchemaUtil.fetchJsonSchema(parser, null);
        } catch (IOException exception) {
            throw new ConnectorException("Failed to create JSON Schema: " + exception.getMessage(), exception);
        }
    }

    /**
     * Creates a json schema of the provided json parser.
     *
     * @param jsonParser the json parser
     * @param schema     the schema
     * @return the string
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static String fetchJsonSchema(JsonParser jsonParser, StringBuilder schema) throws IOException {
        JsonToken currentToken = jsonParser.nextToken();
        int fieldCounter = 0;
        while (currentToken != null) {
            if (null == schema) {
                schema = new StringBuilder("{\"type\": \"object\", \"properties\": {");
            }
            if (JsonToken.FIELD_NAME == currentToken) {
                fieldCounter++;
                if (fieldCounter > 1) {
                    schema.append(",");
                }
                processFieldToken(schema, jsonParser, null);
            }
            currentToken = jsonParser.nextToken();
        }
        schema.append("}}");
        return schema.toString();
    }
    
    /**
     * Parses the input json string.
     *
     * @param json the json
     * @return the json parser
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static JsonParser getJsonParser(String json) throws IOException {
        return getJsonfactory().createParser(json);
    }
    
    /**
     * Gets the jsonfactory.
     *
     * @return the jsonfactory
     */
    private static JsonFactory getJsonfactory() {
        return new JsonFactory();
    }

    /**
     * Process the object token.
     *
     * @param schema
     *            the schema
     * @param jsonParser
     *            the json parser
     * @param arrayItemSchema
     *            the array item schema
     * @return the string buffer
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static StringBuilder processObjectToken(StringBuilder schema, JsonParser jsonParser,
            StringBuilder arrayItemSchema) throws IOException {
        JsonToken currentToken = jsonParser.nextToken();
        int fieldCounter = 0;
        while (currentToken != JsonToken.END_OBJECT) {
            if (JsonToken.FIELD_NAME == currentToken) {
                fieldCounter++;
                if (fieldCounter > 1) {
                    updateSchema(",", schema, arrayItemSchema);
                }
                processFieldToken(schema, jsonParser, arrayItemSchema);
            }
            currentToken = jsonParser.nextToken();
        }
        updateSchema("}", schema, arrayItemSchema);
        return schema;
    }

    /**
     * Prepares the json schema for the provided fields.
     *
     * @param valueToken      the value token
     * @param schema          the schema
     * @param jsonParser      the json parser
     * @param arrayItemSchema the array item schema
     * @return the string buffer
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static StringBuilder processValueToken(JsonToken valueToken, StringBuilder schema, JsonParser jsonParser,
            StringBuilder arrayItemSchema) throws IOException {
        switch (valueToken) {
            case START_ARRAY:
                updateSchema("array\"", schema, arrayItemSchema);
                fetchJsonSchemaForArrayField(schema, jsonParser, arrayItemSchema);
                break;
            case START_OBJECT:
                updateSchema("object\", \"properties\": {", schema, arrayItemSchema);
                processObjectToken(schema, jsonParser, arrayItemSchema);
                break;
            case VALUE_FALSE:
            case VALUE_TRUE:
                updateSchema("boolean\" ", schema, arrayItemSchema);
                break;
            case VALUE_NULL:
                updateSchema("null\" ", schema, arrayItemSchema);
                break;
            case VALUE_NUMBER_FLOAT:
                updateSchema("number\" ", schema, arrayItemSchema);
                break;
            case VALUE_NUMBER_INT:
                updateSchema("integer\" ", schema, arrayItemSchema);
                break;
            case VALUE_STRING:
                updateSchema("string\" ", schema, arrayItemSchema);
                break;
            default:
                break;
        }
        return schema;
    }

    /**
     * Updates the json schema.
     *
     * @param schemaDetails   the schema details
     * @param schema          the schema
     * @param arrayItemSchema the array item schema
     */
    private static void updateSchema(String schemaDetails, StringBuilder schema, StringBuilder arrayItemSchema) {
        schema.append(schemaDetails);
        if (null != arrayItemSchema) {
            arrayItemSchema.append(schemaDetails);
        }
    }

    /**
     * Extract json schema for array field.
     *
     * @param schema                the schema
     * @param jsonParser            the json parser
     * @param parentArrayItemSchema the parent array item schema
     * @return the string buffer
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static StringBuilder fetchJsonSchemaForArrayField(StringBuilder schema, JsonParser jsonParser,
            StringBuilder parentArrayItemSchema) throws IOException {
        int arrayItemsCounter;
        JsonToken arrayItemToken = jsonParser.nextToken();

        if (arrayItemToken == JsonToken.END_ARRAY) {
            return schema;
        }
        updateSchema(", \"items\": [", schema, parentArrayItemSchema);
        arrayItemsCounter = 0;
        
        while (JsonToken.END_ARRAY != arrayItemToken) {
            StringBuilder arrayItemSchema = new StringBuilder();
            if (arrayItemsCounter >= 1) {
                updateSchema(",", schema, arrayItemSchema);
            }
            updateSchema("{ \"type\": \"", schema, arrayItemSchema);
            arrayItemsCounter++;
            processValueToken(arrayItemToken, schema, jsonParser, arrayItemSchema);
            updateSchema("}", schema, arrayItemSchema);
            if (null != parentArrayItemSchema) {
                parentArrayItemSchema.append(arrayItemSchema);
            }
            arrayItemToken = jsonParser.nextToken();
        }
        updateSchema("]", schema, parentArrayItemSchema);

        return schema;
    }

    /**
     * Process field token of the provided field.
     *
     * @param schema          the schema
     * @param jsonParser      the json parser
     * @param arrayItemSchema the array item schema
     * @return the string buffer
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private static StringBuilder processFieldToken(StringBuilder schema, JsonParser jsonParser, StringBuilder arrayItemSchema)
            throws IOException {
        String fieldName = jsonParser.getCurrentName();
        JsonToken valueToken = jsonParser.nextToken();
        updateSchema(new StringBuffer(DOUBLE_QUOTE).append(fieldName).append("\": { \"type\": \"").toString(), schema,
                arrayItemSchema);
        processValueToken(valueToken, schema, jsonParser, arrayItemSchema);
        updateSchema("}", schema, arrayItemSchema);

        return schema;
    }

}
