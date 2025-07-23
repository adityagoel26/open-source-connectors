// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;

import net.snowflake.client.jdbc.internal.net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.internal.net.minidev.json.parser.JSONParser;
import net.snowflake.client.jdbc.internal.net.minidev.json.parser.ParseException;

import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

public class JSONHandler {
	private static final Logger LOG = LogUtil.getLogger(JSONHandler.class);
	private static final String DOUBLE_QUOTES = "\"";
	private static final String SINGLE_QUOTES = "'";

	private JSONHandler() {
		// Prevent initialization
	}

	/**
	 * Reads input ObjectData stream and construct a JSON Object
	 * the JSON object is expected to have at most one row of database table
	 * @param requestData stream request input in ObjectData Object
	 * @return JSON object containing the input data of requestData stream
	 */
	private static JSONObject readJSON(InputStream requestData) {
		LOG.entering(JSONHandler.class.getCanonicalName(), "readJSON()");
		JSONParser parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
		InputStreamReader reader = null;
		JSONObject result = new JSONObject();
		try {
			reader = new InputStreamReader(requestData, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ConnectorException("Unexpected encoding", e);
		}
		try {
			if (reader.ready()) {
				result = (JSONObject) parser.parse(reader);
			}
		} catch (ParseException e) {
			throw new ConnectorException("Error parsing JSON Object", e);
		} catch (IOException e) {
			throw new ConnectorException("I/O error occurs", e);
		}finally {
			IOUtil.closeQuietly(reader);
			IOUtil.closeQuietly(requestData);
		}
		return result;
	}

	/**
	 * Reads input String and construct a JSON Object
	 * 
	 * @param Data input in ObjectData Object
	 * @return JSON object containing the input data
	 */
	private static JSONObject readJSON(String Data) {
		LOG.entering(JSONHandler.class.getCanonicalName(), "readJSON()");
		JSONObject resultantJSONObject = null;
		try {
			// parse JSON filter object
			JSONParser parser = new JSONParser(JSONParser.MODE_PERMISSIVE);
			resultantJSONObject = (JSONObject) parser.parse(Data);
		} catch (ParseException e) {
			throw new ConnectorException("Error parsing JSON Object", e);
		}
		return resultantJSONObject;
	}
	
	/**
	 * converts from JSON to SortedMap so that data is sorted
	 * 
	 * @param jsonObject
	 * @return SortedMap containing the same data as the JSON object
	 */
	public static SortedMap<String, String> jsonToSortedMap(JSONObject jsonObject) {
		SortedMap<String, String> result = new TreeMap<String, String>();
		while(jsonObject.keySet().isEmpty() == false) {
			String keyStr = jsonObject.keySet().iterator().next(); 
			result.put(keyStr, jsonObject.getAsString(keyStr));
			jsonObject.remove(keyStr);
		}
		return result;
	}
	
	public static SortedMap<String, String> readSortedMap(InputStream inputData){
		return jsonToSortedMap(readJSON(inputData));
	}
	
	public static SortedMap<String, String> readSortedMap(String Data){
		return jsonToSortedMap(readJSON(Data));
	}
	
	public static SortedMap<String, String> readDocPropertiesOrInputStream(ObjectData requestData){
		requestData.getLogger().info("Started processing input document: " + requestData.getUniqueId());
		SortedMap<String, String> sortedMap = readSortedMap(requestData.getData());
		Map<String, String> map = requestData.getUserDefinedProperties();
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()) {
			String key = keys.next();
			sortedMap.put(key, map.get(key));
		}
		return sortedMap;
		
	}
	
	/**
	 * Checks whether the input String is valid JSON format.
	 * 
	 * @param test input String
	 * @return Boolean
	 */
	public static boolean isJSONValid(String test) {
    	JSONParser parser = new JSONParser(JSONParser.MODE_STRICTEST);
    	test = test.replace(SINGLE_QUOTES, DOUBLE_QUOTES);
    	try {
			parser.parse(test);
		} catch (ParseException e) {
			return false;
		}
    return true;
}
}
