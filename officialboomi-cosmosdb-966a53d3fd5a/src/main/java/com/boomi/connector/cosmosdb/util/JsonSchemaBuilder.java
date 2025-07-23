//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.util;

import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.COMMA;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.CURLY_BRACE_END;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.DOUBLE_QUOTE;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.HTTP_GET;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_ARRAY;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_BOOLEAN;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_INTEGER;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_NULL;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_NUMBER;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_PROPERTIES;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_START;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_STRING;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_TYPE;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_TYPE_TOKEN;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SCHEMA_UNHANDLED_TOKEN;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.SQUARE_BRACKET_END;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.cosmosdb.bean.GetOperationRequest;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class JsonSchemaBuilder {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(JsonSchemaBuilder.class.getName());

	/**
	 * This method is used for extracting the JSON Schema from the Input Stream coming from Cosmos DB
	 * @param jsonStream
	 * @param schema
	 * @return String
	 * @throws IOException
	 */
	public static String extractJsonSchema(InputStream jsonStream, StringBuilder schema) throws IOException {

		try (JsonParser jsonParser = getJsonParser(jsonStream);) {
			JsonToken currentToken = jsonParser.nextToken();
			int fieldCounter = 0;
			while (currentToken != null) {
				if (null == schema) {
					schema = new StringBuilder(SCHEMA_START);
				}
				if (JsonToken.FIELD_NAME == currentToken) {
					fieldCounter++;
					if (fieldCounter > 1) {
						schema.append(COMMA);
					}
					processFieldToken(schema, jsonParser, null);
				}
				currentToken = jsonParser.nextToken();
			}
			schema.append(CURLY_BRACE_END).append(CURLY_BRACE_END);
			return schema.toString();
		}
	}
	
	/**
	 * This method is used for extracting the JSON Schema from the Input Stream coming from Cosmos DB
	 * @param jsonStream
	 * @param schema
	 * @return String
	 * @throws IOException
	 */
	public static String extractJsonSchemaForUnstructuredData(InputStream jsonStream, StringBuilder schema)
			throws IOException {

		try (JsonParser jsonParser = getJsonParser(jsonStream);) {
			JsonToken currentToken = jsonParser.nextToken();
			int fieldCounter = 0;
			while (jsonParser.nextToken() != null) {
				String fieldName = jsonParser.getCurrentName();
				if (fieldName != null && fieldName.equals("Documents")) {
					currentToken = jsonParser.nextToken();
					break;
				}
			}
			while (currentToken != null) {
					if (null == schema) {
						schema = new StringBuilder(SCHEMA_START);
					}
					if (JsonToken.FIELD_NAME == currentToken) {
						fieldCounter++;
						if (fieldCounter > 1) {
							schema.append(COMMA);
						}
						processFieldToken(schema, jsonParser, null);
					}
					currentToken = jsonParser.nextToken();
			}
			schema.append(CURLY_BRACE_END).append(CURLY_BRACE_END);
			return schema.toString();
		}

	}

	/**
	 * This method is to process an Field Token
	 * @param schema
	 * @param jsonParser
	 * @param arrayItemSchema
	 * @return StringBuilder
	 * @throws IOException
	 */
	private static StringBuilder processFieldToken(StringBuilder schema, JsonParser jsonParser,
			StringBuilder arrayItemSchema) throws IOException {
		String fieldName = jsonParser.getCurrentName();
		JsonToken valueToken = jsonParser.nextToken();
		updateSchema(new StringBuilder(DOUBLE_QUOTE).append(fieldName)
				.append(SCHEMA_TYPE_TOKEN).toString(), schema, arrayItemSchema);
		processValueToken(valueToken, schema, jsonParser, arrayItemSchema);
		updateSchema(CURLY_BRACE_END, schema, arrayItemSchema);
		return schema;
	}

	/**
	 * This method is to process an Object Field
	 * @param schema
	 * @param jsonParser
	 * @param arrayItemSchema
	 * @return StringBuilder
	 * @throws IOException
	 */
	private static StringBuilder processObjectToken(StringBuilder schema, JsonParser jsonParser,
			StringBuilder arrayItemSchema) throws IOException {
		JsonToken currentToken = jsonParser.nextToken();
		int fieldCounter = 0;
		while (currentToken != JsonToken.END_OBJECT) {
			if (JsonToken.FIELD_NAME == currentToken) {
				fieldCounter++;
				if (fieldCounter > 1) {
					updateSchema(COMMA, schema, arrayItemSchema);
				}
				processFieldToken(schema, jsonParser, arrayItemSchema);
			}
			currentToken = jsonParser.nextToken();
		}
		updateSchema(CURLY_BRACE_END, schema, arrayItemSchema);
		return schema;
	}

	/**
	 * Processing the Value token in the JSON as per the field type.
	 * 
	 * @param valueToken, schema, jsonParser, arrayItemSchema
	 * @return StringBuilder
	 * @throws CosmosDBConnectorException
	 */
	public static StringBuilder processValueToken(JsonToken valueToken, StringBuilder schema, JsonParser jsonParser,
			StringBuilder arrayItemSchema) throws IOException {
		switch (valueToken) {
		case START_ARRAY:
			updateSchema(SCHEMA_ARRAY, schema, arrayItemSchema);
			extractJsonSchemaForArrayField(schema, jsonParser, arrayItemSchema);
			break;
		case START_OBJECT:
			updateSchema(SCHEMA_PROPERTIES, schema, arrayItemSchema);
			processObjectToken(schema, jsonParser, arrayItemSchema);
			break;
		case VALUE_FALSE:
		case VALUE_TRUE:
			updateSchema(SCHEMA_BOOLEAN, schema, arrayItemSchema);
			break;
		case VALUE_NULL:
			updateSchema(SCHEMA_NULL, schema, arrayItemSchema);
			break;
		case VALUE_NUMBER_FLOAT:
			updateSchema(SCHEMA_NUMBER, schema, arrayItemSchema);
			break;
		case VALUE_NUMBER_INT:
			updateSchema(SCHEMA_INTEGER, schema, arrayItemSchema);
			break;
		case VALUE_STRING:
			updateSchema(SCHEMA_STRING, schema, arrayItemSchema);
			break;
		default:
			logger.log(Level.WARNING, SCHEMA_UNHANDLED_TOKEN, valueToken);
			break;
		}
		return schema;
	}

	/**
	 * Updating the schema for array types
	 * @param schemaDetails
	 * @param schema
	 * @param arrayItemSchema
	 */
	private static void updateSchema(String schemaDetails, StringBuilder schema, StringBuilder arrayItemSchema) {
		schema.append(schemaDetails);
		if (null != arrayItemSchema) {
			arrayItemSchema.append(schemaDetails);
		}
	}

	/**
	 * Schema generation in case of field of array type
	 * @param schema
	 * @param jsonParser
	 * @param parentArrayItemSchema
	 * @return
	 * @throws IOException
	 */
	private static StringBuilder extractJsonSchemaForArrayField(StringBuilder schema, JsonParser jsonParser,
			StringBuilder parentArrayItemSchema) throws IOException {
		JsonToken arrayItemToken = jsonParser.nextToken();
		int arrayItemsCounter = 0;
		while (JsonToken.END_ARRAY != arrayItemToken) {
			StringBuilder arrayItemSchema = new StringBuilder();
			if (arrayItemsCounter >= 1) {
				updateSchema(COMMA, schema, arrayItemSchema);
			}
			updateSchema(SCHEMA_TYPE, schema, arrayItemSchema);
			arrayItemsCounter++;
			processValueToken(arrayItemToken, schema, jsonParser, arrayItemSchema);
			updateSchema(CURLY_BRACE_END, schema, arrayItemSchema);
			if (null != parentArrayItemSchema) {
				parentArrayItemSchema.append(arrayItemSchema);
			}
			arrayItemToken = jsonParser.nextToken();
		}
		updateSchema(SQUARE_BRACKET_END, schema, parentArrayItemSchema);
		return schema;
	}

	/**
	 * Getting Json Parser Object from the Json Factory instance
	 * @param responseStream
	 * @return
	 * @throws IOException
	 */
	private static JsonParser getJsonParser(InputStream responseStream) throws IOException {
		return getJsonfactory().createParser(responseStream);
	}

	/**
	 * Get JsonFactory Instance
	 * @return
	 */
	private static JsonFactory getJsonfactory() {
		return new JsonFactory();
	}

	/**
	 * Creates the JSON schema for request profile
	 * 
	 * @param objectTypeId
	 * @return JSON String
	 * @throws CosmosDBConnectorException
	 */
	public static String getInputJsonSchema(String objectTypeId) throws CosmosDBConnectorException {
		String json = null;
		try {
			ObjectMapper objectMapper = new ObjectMapper().disable(
		            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
			SchemaFactoryWrapper wrapper = new SchemaFactoryWrapper();
			if (objectTypeId.equalsIgnoreCase(HTTP_GET)) {
				objectMapper.acceptJsonFormatVisitor(GetOperationRequest.class, wrapper);
			}
			JsonSchema schema = wrapper.finalSchema();
			json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
		} catch (Exception e) {
			throw new CosmosDBConnectorException(e.getMessage());
		}
		return json;
	}

	private JsonSchemaBuilder() {

	}
}
