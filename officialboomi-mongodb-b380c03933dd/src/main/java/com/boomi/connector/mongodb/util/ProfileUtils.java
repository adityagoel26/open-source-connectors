// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.util;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.FORWARD_SLASH;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.JSON_SCHEMA_FIELD_ITEMS;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.JSON_SCHEMA_FIELD_PROPERTIES;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.JSON_SCHEMA_FIELD_TYPE;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ProfileUtils {

	/** The mongo extended json fieldnames. */
	public static final List<String> MONGO_EXTENDED_JSON_FIELDNAMES = ProfileUtils.extendedJsonSchema();

	/** The jsonfactory. */
	private JsonFactory jsonfactory = null;

	/** The response profile. */
	private String profile = null;

	/**
	 * Constructor initializes profile.
	 * 
	 * @param profile
	 */
	public ProfileUtils(String profile) {
		this.profile = profile;
	}

	/**
	 * This method returns the profile.
	 * 
	 * @return profile
	 */
	public String getProfile() {
		return profile;
	}

	/**
	 * Extended json schema.
	 *
	 * @return the list
	 */
	private static List<String> extendedJsonSchema() {

		ArrayList<String> list = new ArrayList<>();
		list.add("$date");
		list.add("$oid");
		list.add("$numberDecimal");
		list.add("$numberLong");
		list.add("$numberInt");
		return list;
	}

	/**
	 * Gets the data type of the field name provided on the platform.
	 *
	 * @param jsonParser the json parser
	 * @return the type
	 * @throws IOException             Signals that an I/O exception has occurred.
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private String getType(JsonParser jsonParser) throws IOException {
		JsonToken currentToken = jsonParser.nextToken();
		String type = null;
		String currentFieldName = jsonParser.getCurrentName();
		while (!(JsonToken.VALUE_STRING.equals(currentToken) && JSON_SCHEMA_FIELD_TYPE.equals(currentFieldName))) {
			currentToken = jsonParser.nextToken();
			currentFieldName = jsonParser.getCurrentName();
		}
		type = jsonParser.getText();
		
		return type;
	}

	/**
	 * Gets the data type of the filters provided in the platform.
	 *
	 * @param field the field
	 * @return the type
	 * @throws IOException             Signals that an I/O exception has occurred.
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	public String getType(String field) throws IOException, MongoDBConnectException {
		String[] fieldNames = field.split(FORWARD_SLASH);
		String type = null;
		if (fieldNames.length > 1) {
			type = getTypeForMongoExtJsonType(fieldNames[fieldNames.length - 1]);
		}
		if (StringUtil.isBlank(type)) {
			try (JsonParser jsonParser = getJsonfactory().createParser(getProfile());) {
				initializeParser(jsonParser);
				type = getFieldType(jsonParser, fieldNames, field);
			}
		}
		return type;
	}

	/**
	 * Gets the field type of the selected field on the platform.
	 *
	 * @param jsonParser        the json parser
	 * @param fieldNames        the field names
	 * @param absoluteFieldName the absolute field name
	 * @return the field type
	 * @throws IOException             Signals that an I/O exception has occurred.
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	@SuppressWarnings({"java:S3776"})
	private String getFieldType(JsonParser jsonParser, String[] fieldNames, String absoluteFieldName)
			throws IOException, MongoDBConnectException {
		JsonToken currentToken = jsonParser.nextToken();
		String type = null;
		boolean fieldFound = false;
		String currentField = null;
		boolean traversedTillEndToken = false;
		int fieldIndex = 0;
		List<JsonToken> listOfArrayTokens = new ArrayList<>();
		for (; fieldIndex < fieldNames.length; fieldIndex++) {
			String fieldName = fieldNames[fieldIndex];
			fieldFound = false;
			boolean isArrayitem = false;
			boolean isArrayItemFound = false;
			while (currentToken != null) {
				traversedTillEndToken = false;
				if (JsonToken.FIELD_NAME.equals(currentToken) && fieldName.equals(jsonParser.getCurrentName())) {
					fieldFound = true;
					if (fieldIndex < fieldNames.length - 1) {
						traverseFieldSchema(jsonParser, fieldNames[fieldIndex + 1], listOfArrayTokens);
					}
					isArrayitem = isArrayItem(listOfArrayTokens);
					if (isArrayitem) {
						if (fieldIndex == fieldNames.length - 2) {
							isArrayItemFound = true;
						} else {
							fieldIndex++;
							for (; isArrayitem && (fieldIndex < fieldNames.length - 1); fieldIndex++) {
								traverseFieldSchema(jsonParser, fieldNames[fieldIndex + 1], listOfArrayTokens);
								isArrayitem = isArrayItem(listOfArrayTokens);
							}
							if (fieldIndex == fieldNames.length - 1) {
								fieldFound = getIsArrayItem(jsonParser, fieldNames, fieldFound, fieldIndex,
										isArrayitem);
							} else if (!isArrayitem) {
								fieldIndex++;
							}
						}
					}
					break;
				} else {
					if (JsonToken.FIELD_NAME.equals(currentToken)) {
						currentField = jsonParser.getCurrentName();
					} else if (JsonToken.START_OBJECT.equals(currentToken)) {
						traversedTillEndToken = true;
						traverseTillEndTokenIncluded(jsonParser, currentField, JsonToken.END_OBJECT);
					} else if (JsonToken.START_ARRAY.equals(currentToken)) {
						traversedTillEndToken = true;
						traverseTillEndTokenIncluded(jsonParser, currentField, JsonToken.END_ARRAY);
					}
					currentToken = jsonParser.getCurrentToken();
					if (!traversedTillEndToken || JsonToken.END_OBJECT.equals(currentToken)) {
						currentToken = jsonParser.nextToken();
					}
				}
			}
			if (isArrayItemFound || !StringUtil.isBlank(type)) {
				break;
			} else if (!fieldFound) {
				currentToken = jsonParser.nextToken();
			}
		}
		if (fieldFound) {
			type = getType(jsonParser);
		} else {
			throw new MongoDBConnectException(
					new StringBuffer("Error in traversing schema for fetching type for field - ")
							.append(absoluteFieldName).toString());
		}
		return type;
	}

	private boolean getIsArrayItem(JsonParser jsonParser, String[] fieldNames, boolean fieldFound, int fieldIndex,
			boolean isArrayitem) throws IOException {
		if (isArrayitem) {
			fieldFound = isArrayitem;
		} else {
			traverseTillEndToken(jsonParser, fieldNames[fieldIndex], JsonToken.FIELD_NAME,
					false);
		}
		return fieldFound;
	}

	/**
	 * Checks if is array item.
	 *
	 * @param listOfArrayTokens the list of array tokens
	 * @return true, if is array item
	 */
	private boolean isArrayItem(List<JsonToken> listOfArrayTokens) {
		boolean isArrayItem = !listOfArrayTokens.isEmpty()
				&& JsonToken.START_ARRAY.equals(listOfArrayTokens.get(listOfArrayTokens.size() - 1));
		listOfArrayTokens.clear();
		return isArrayItem;
	}

	/**
	 * Traverse the json schema for all the fields in the response profile.
	 *
	 * @param jsonParser        the json parser
	 * @param childFieldName    the child field name
	 * @param listOfArrayTokens the list of array tokens
	 * @return the json parser
	 * @throws IOException             Signals that an I/O exception has occurred.
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private JsonParser traverseFieldSchema(JsonParser jsonParser, String childFieldName,
			List<JsonToken> listOfArrayTokens) throws IOException, MongoDBConnectException {
		JsonToken currentToken = jsonParser.nextToken();
		boolean isArrayItemFound = false;
		while (null != currentToken) {
			String currentFieldName = jsonParser.getCurrentName();
			if (JsonToken.FIELD_NAME.equals(currentToken) && (JSON_SCHEMA_FIELD_PROPERTIES.equals(currentFieldName)
					|| JSON_SCHEMA_FIELD_ITEMS.equals(currentFieldName))) {
				currentToken = jsonParser.nextToken();
				if (JsonToken.START_OBJECT.equals(currentToken)) {
					currentToken = jsonParser.nextToken();
					isArrayItemFound = true;
				} else if (JsonToken.START_ARRAY.equals(currentToken)) {
					isArrayItemFound = getStartArrayEqualsCurrentToken(jsonParser, childFieldName, listOfArrayTokens,
							currentToken, isArrayItemFound);
				}
				if (isArrayItemFound) {
					break;
				}
			}
			currentToken = jsonParser.nextToken();
		}
		if (null == currentToken) {
			throw new MongoDBConnectException("Invalid schema object to fetch type");
		}
		return jsonParser;
	}

	private boolean getStartArrayEqualsCurrentToken(JsonParser jsonParser, String childFieldName,
			List<JsonToken> listOfArrayTokens, JsonToken currentToken, boolean isArrayItemFound)
			throws MongoDBConnectException, IOException {
		Deque<JsonToken> arrayItemTypeStack = new ArrayDeque<>();
		listOfArrayTokens.add(currentToken);
		int arrayItemIndex = 0;
		int fielditemIndex = getArrayItemIndex(childFieldName);
		JsonToken nextToken = jsonParser.nextToken();
		String arrayItemFieldName = null;
		while (!isArrayItemFound && null != nextToken) {
			nextToken = jsonParser.nextToken();
			arrayItemFieldName = jsonParser.getCurrentName();
			if (JsonToken.FIELD_NAME.equals(nextToken)
					&& JSON_SCHEMA_FIELD_TYPE.equals(arrayItemFieldName)) {
				if (arrayItemIndex == fielditemIndex) {
					isArrayItemFound = true;
					break;
				}
			} else if (JsonToken.START_ARRAY.equals(nextToken)
					|| JsonToken.START_OBJECT.equals(nextToken)) {
				arrayItemTypeStack.push(nextToken);
			} else if (!arrayItemTypeStack.isEmpty()) {
				arrayItemIndex = getItemTypeIsNotEmpty(arrayItemTypeStack, arrayItemIndex, nextToken);
			}
		}
		return isArrayItemFound;
	}

	private int getItemTypeIsNotEmpty(Deque<JsonToken> arrayItemTypeStack, int arrayItemIndex, JsonToken nextToken) {
		JsonToken lastItemType = arrayItemTypeStack.peek();
		if ((JsonToken.END_ARRAY.equals(nextToken) && JsonToken.START_ARRAY.equals(lastItemType))
				|| (JsonToken.END_OBJECT.equals(nextToken)
						&& JsonToken.START_OBJECT.equals(lastItemType))) {
			arrayItemTypeStack.pop();
			if (arrayItemTypeStack.isEmpty()) {
				arrayItemIndex++;
			}
		}
		return arrayItemIndex;
	}

	/**
	 * Gets the array item index.
	 *
	 * @param childFieldName the child field name
	 * @return the array item index
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private int getArrayItemIndex(String childFieldName) throws MongoDBConnectException {
		int fielditemIndex = -1;
		try {
			fielditemIndex = Integer.parseInt(childFieldName);
		} catch (NumberFormatException ex) {
			throw new MongoDBConnectException("Invalid schema object in array to fetch type");
		}
		return fielditemIndex;
	}

	/**
	 * Initialize parser.
	 *
	 * @param jsonParser the json parser
	 * @return the json parser
	 * @throws IOException             Signals that an I/O exception has occurred.
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private JsonParser initializeParser(JsonParser jsonParser) throws IOException, MongoDBConnectException {
		String currentFieldName = null;
		JsonToken currentToken = null;
		while (!JSON_SCHEMA_FIELD_PROPERTIES.equals(currentFieldName)) {
			currentFieldName = jsonParser.getCurrentName();
			currentToken = jsonParser.nextToken();
		}
		if (!JsonToken.START_OBJECT.equals(currentToken)) {
			throw new MongoDBConnectException("Error while fetching type for field");
		}
		return jsonParser;
	}

	/**
	 * Gets the jsonfactory.
	 *
	 * @return the jsonfactory
	 */
	public JsonFactory getJsonfactory() {
		if (null == jsonfactory) {
			jsonfactory = new JsonFactory();
		}
		return jsonfactory;
	}

	/**
	 * Traverse till end token included.
	 *
	 * @param jsonParser the json parser
	 * @param fieldName  the field name
	 * @param endToken   the end token
	 * @return Type casted object based on the field type from schema.
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	private JsonParser traverseTillEndTokenIncluded(JsonParser jsonParser, String fieldName, JsonToken endToken)
			throws IOException {
		JsonToken currentToken = null;
		currentToken = jsonParser.nextToken();
		if (fieldName != null) {
			while (!fieldName.equals(jsonParser.getCurrentName()) && !endToken.equals(currentToken)) {
				currentToken = jsonParser.nextToken();
			}
		}
		jsonParser.nextToken();
		return jsonParser;
	}

	/**
	 * Traverse till end token.
	 *
	 * @param jsonParser         the json parser
	 * @param fieldName          the field name
	 * @param endToken           the end token
	 * @param isEndTokenIncluded the is end token included
	 * @return the json parser
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private JsonParser traverseTillEndToken(JsonParser jsonParser, String fieldName, JsonToken endToken,
			boolean isEndTokenIncluded) throws IOException {
		JsonToken currentToken = null;
		currentToken = jsonParser.nextToken();
		while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
			if (!fieldName.equals(jsonParser.getCurrentName()) || !endToken.equals(currentToken)) {
				currentToken = jsonParser.nextToken();
			}
		}
		if (isEndTokenIncluded) {
			jsonParser.nextToken();
		}

		return jsonParser;
	}

	/**
	 * {@linkextendedJsonSchema()} contains the list of special data types in the
	 * MongoDB.This method matches the provided the data types of the provided
	 * fieldnames with the list.
	 *
	 * @param fieldName the field name
	 * @return the type for mongo ext json type
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	private String getTypeForMongoExtJsonType(String fieldName) throws MongoDBConnectException {
		String type = null;
		if (MONGO_EXTENDED_JSON_FIELDNAMES.contains(fieldName)) {
			switch (fieldName) {
			case "$date":
				type = "date";
				break;
			case "$oid":
				type = "string";
				break;
			case "$numberInt":
				type = "number";
				break;
			case "$numberDecimal":
				type = "decimal128";
				break;
			case "$numberLong":
				type = "long";
				break;
			default:
				throw new MongoDBConnectException(
						new StringBuffer("invalid Mongo extended JSON type provided as field name-").append(fieldName)
								.toString());
			}
		}
		return type;
	}
}
