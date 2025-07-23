//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics.utils;

import static com.boomi.connector.liveoptics.utils.LiveOpticsConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class JsonSchemaBuilder {

	/**
	 * To build the Json Schema
	 * 
	 * @param surl The swagger URL of type String
	 * @return String The json schema
	 * @throws IOException
	 */
	public static String buildJsonSchema(String surl) throws IOException {

		JsonNode lNode = getSwaggerDocument(surl);
		JsonNode lProj = lNode.get(SCHEMA_DEFINITIONS).get(SCHEMA_PROJECTS);
		return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(addRefs(lProj, lNode));
	}

	/*
	 * This method adds reference definitions to build proper schema
	 */
	private static JsonNode addRefs(JsonNode lNode, JsonNode lParent) {
		Iterator<JsonNode> lIter = lNode.get(SCHEMA_PROPERTIES).iterator();
		while (lIter.hasNext()) {
			JsonNode parameterNode = lIter.next();
			if (((TextNode) parameterNode.get(SCHEMA_TYPE)).textValue().equals(SCHEMA_ARRAY)) {
				if (parameterNode.get(SCHEMA_ITEMS).get(SCHEMA_REF) != null) {
					JsonNode lRef = parameterNode.get(SCHEMA_ITEMS).get(SCHEMA_REF);
					JsonNode lDef = lParent.get(SCHEMA_DEFINITIONS).get(getDefinitionKey(((TextNode) lRef).textValue()));
					((ObjectNode) parameterNode.get(SCHEMA_ITEMS)).remove(SCHEMA_REF);
					((ObjectNode) parameterNode.get(SCHEMA_ITEMS)).setAll((ObjectNode) lDef);
					addRefs(lDef, lParent);
				}
			}
		}
		return lNode;
	}

	private static String getDefinitionKey(String ref) {
		ref = ref.substring(ref.lastIndexOf("/") + 1);
		return ref;
	}

	/*
	 * This method is used for calling the Swagger APi and getting the Swagger
	 * Document for identifying the Response Profiles
	 */
	private static JsonNode getSwaggerDocument(String surl) throws IOException {

		URL url = null;
		HttpURLConnection urlConnection = null;
		JsonNode lJsonParentDoc = null;
		try {
			url = new URL(surl);
			urlConnection = (HttpURLConnection) url.openConnection();
			urlConnection.setRequestMethod("GET");
			urlConnection.setRequestProperty("Content-Type", "application/json");
			urlConnection.setRequestProperty("Accept", "application/json");

			try (InputStream httpResponse = urlConnection.getInputStream();) {
				lJsonParentDoc = new ObjectMapper().readTree(httpResponse);
			}
		} catch (Exception e) {
			throw new ConnectException("Exception while getting the Swagger Document.");
		} finally {
			if (null != urlConnection) {
				urlConnection.disconnect();
			}
		}
		return lJsonParentDoc;
	}

}
