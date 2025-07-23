//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.cosmosdb.bean.CreateOperationRequest;
import com.boomi.connector.cosmosdb.bean.DeleteOperationRequest;
import com.boomi.connector.cosmosdb.bean.GetOperationRequest;
import com.boomi.connector.cosmosdb.bean.UpdateOperationRequest;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class DocumentUtil {
	
	private static final Logger logger = Logger.getLogger(DocumentUtil.class.getName());

	/**
	 * Converts given Input stream to List of containers in Cosmos DB.
	 *
	 * @param responseStream
	 * @return the List of String
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static List<String> getCollectionFromStream(InputStream responseStream) throws IOException {
		JsonFactory f = new MappingJsonFactory();
		List<String> collectionList = null;
		try (JsonParser jp = f.createParser(responseStream);) {
			collectionList = new ArrayList<>();
			while (jp != null && jp.nextToken() != null) {
				String fieldName = jp.getCurrentName();
				if (("id").equals(fieldName)) {
					JsonToken nextToken = jp.nextToken();
					if (!nextToken.equals(JsonToken.VALUE_NULL)) {
						collectionList.add(jp.readValueAs(String.class));
					}
				}
			}
		}
		return collectionList;
	}

	/**
	 * Converts given Request input stream to GetOperationRequest object
	 *
	 * @param objectData
	 * @return the GetOperationRequest
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static GetOperationRequest getRequestData(ObjectData objectData) throws IOException {

		try (InputStream requestStream = objectData.getData()) {
			ObjectMapper objectMapper = new ObjectMapper().disable(
		            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
			return objectMapper.readValue(requestStream, GetOperationRequest.class);
		}
	}

	/**
	 * Converts given Request input stream to UpdateOperationRequest object
	 *
	 * @param objectData
	 * @param partitionKey
	 * @return the GetOperationRequest
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static UpdateOperationRequest getUpdateRequestData(ObjectData objectData, String partitionKey)
			throws IOException {

		UpdateOperationRequest request = new UpdateOperationRequest();
		String[] partitionKeyPaths = partitionKey.replace("\"", "").split("/");
		request.setId(getPrimaryId(objectData));
		try (JsonParser jp = new JsonFactory().createParser(objectData.getData())) {
			request.setPartitionKey(getPartitionKeyValue(jp, partitionKeyPaths, 1));
		}
		return request;
	}
	
	/**
	 * Method which will take request and builds CreateOperationRequest out of partition key
	 * @param objectData
	 * @param partitionKey
	 * @return CreateOperationRequest
	 * @throws IOException
	 */
	public static CreateOperationRequest getCreateRequestData(ObjectData objectData, String partitionKey)
			throws IOException {

		CreateOperationRequest request = new CreateOperationRequest();
		String[] partitionKeyPaths = partitionKey.replace("\"", "").split("/");
		request.setRequestId(getPrimaryId(objectData));
		try (JsonParser jp = new JsonFactory().createParser(objectData.getData())) {
			request.setPartitionValue(getPartitionKeyValue(jp, partitionKeyPaths, 1));
		}
		return request;
	}
	
	/**
	 * This method returns the value from the ID field.
	 * @param objectData
	 * @return
	 * @throws IOException
	 */
	private static String getPrimaryId(ObjectData objectData) throws IOException {
		try (JsonParser jp = new JsonFactory().createParser(objectData.getData())) {
			while (jp.nextToken() != null) {
				String fieldName = jp.getCurrentName();
				if (fieldName != null && fieldName.equals("id")) {
					jp.nextToken();
					return jp.getText();
				}
			}
		}
		return null;
	}

	/**
	 * this method returns the partition key value after parsing the json data.
	 * @param jp
	 * @param paths
	 * @param i
	 * @return String
	 * @throws IOException
	 */
	private static String getPartitionKeyValue(JsonParser jp, String[] paths, int i) throws IOException {
		
		while (jp.nextToken() != null) {
			String fieldName = jp.getCurrentName();
			if (fieldName != null && fieldName.equals(paths[i])) {
				jp.nextToken();
				if (i == paths.length - 1) {
					return jp.getText();
				} else {
					return getPartitionKeyValue(jp, paths, i + 1);
				}
			}
		}
		return null;
	}

	/**
	 * Converts given Input stream to string.
	 *
	 * @param inputStream
	 * @param charset 
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static String inputStreamToString(InputStream inputStream, Charset charset) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		if (charset == null) {
			charset = StandardCharsets.UTF_8;
		}
		String line = null;
		BufferedReader bufferedreader = new BufferedReader(new InputStreamReader(inputStream, charset));
		while ((line = bufferedreader.readLine()) != null) {
			stringBuilder.append(line);
		}
		return stringBuilder.toString();
	}

	private DocumentUtil() {

	}

	/**
	 * Get Partition Key field for the Collection selected
	 *
	 * @param responInputStream
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static String getPartitionKeyField(InputStream responInputStream) throws IOException {
		try {
			ObjectMapper objectMapper = new ObjectMapper().disable(
		            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
			JsonNode jsonNode = objectMapper.readTree(responInputStream);
			ArrayNode paths = (ArrayNode) jsonNode.get("partitionKey").get("paths");
			if (paths != null) {
				return String.valueOf(paths.get(0));
			}
		} finally {
			IOUtil.closeQuietly(responInputStream);
		}
		return null;
	}
	
	/**
	 * Get Partition Key field for the Collection selected
	 *
	 * @param responInputStream
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static String getPartitionKeyRange(InputStream responInputStream) throws IOException {
		try {
			ObjectMapper objectMapper = new ObjectMapper().disable(
		            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
			JsonNode jsonNode = objectMapper.readTree(responInputStream);
			ArrayNode paths = (ArrayNode) jsonNode.get("PartitionKeyRanges");
			if (paths != null) {
				return jsonNode.get("_rid").toString().replaceAll("\"", "")+","+paths.get(0).get("id").toString().replaceAll("\"", "");
			}
		} finally {
			IOUtil.closeQuietly(responInputStream);
		}
		return null;
	}

	/**
	 * Gets the json schema for a given bean class
	 *
	 * @param definitionClass
	 * @return the json schema
	 * @throws JsonProcessingException the json processing exception
	 */
	@SuppressWarnings("rawtypes")
	public static String getJsonSchema(Class definitionClass) throws JsonProcessingException {
		ObjectMapper mapper =  new ObjectMapper().disable(
	            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
		JavaType javaType = mapper.getTypeFactory().constructType(definitionClass);
		JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
		JsonSchema schema = schemaGen.generateSchema(javaType);
		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
	}

	public static DeleteOperationRequest getUpdateRequestData(ObjectData request) throws IOException {
		ObjectMapper mapper =  new ObjectMapper().disable(
	            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
		return mapper.readValue(request.getData(), DeleteOperationRequest.class);
	}
	
	/**
	 * Prepare the Final Query for POST api by appending all Parameters
	 * @param filterParameters
	 * @return Query String
	 */
	public static String getRequestData(List<Entry<String, String>> filterParameters) {
		String projectionQuery = "";
		String filterQuery = "";
		String sortingQuery = "";
		for (Entry<String, String> requestFilter : filterParameters) {
			switch (requestFilter.getKey()) {
			case "$projection":
				projectionQuery = requestFilter.getValue();
				break;
			case "$filter":
				filterQuery = requestFilter.getValue();
				break;
			case "$sort":
				sortingQuery = requestFilter.getValue();
				break;
			default:
				break;
			}
		}
		return CosmosDbConstants.QUERY_PREFIX+projectionQuery + filterQuery + sortingQuery +CosmosDbConstants.QUERY_SUFFIX;
	}
}
