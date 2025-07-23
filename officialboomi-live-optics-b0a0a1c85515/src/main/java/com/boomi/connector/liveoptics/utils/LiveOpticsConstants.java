//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics.utils;

/**
 * @author Sudeshna Bhattacharjee
 *
 * ${tags}
 */
public class LiveOpticsConstants {
	public static final String DEFAULT_URL = "https://papi.liveoptics.com";
	public static final String URL_PROPERTY = "url";
	public static final String POST_METHOD = "POST";
	public static final String CONTENT_TYPE_HEADER = "Content-Type";
	public static final String JSON_CONTENT_TYPE = "application/json";
	public static final String LOGIN_API = "/papi/session/login";
	public static final String METADATA_RESOURCE = "/swagger/docs/v1";
	public static final String JSON_DRAFT4_DEFINITION = "\n\"$schema\": \"http://json-schema.org/draft-04/schema#\",";
	public static final String US_ASCII = "US-ASCII";
	public static final String HMAC_SHA512 = "HmacSHA512";
	public static final String GET_API = "/papi/projects/getdetail";
	public static final String ID = "Project";
	public static final String DATE_FORMAT = "yyyyMMddHHmmss";
	public static final String LOGIN_ID = "LoginId";
	public static final String LOGIN_TOKEN = "LoginToken";
	public static final String INCLUDE_ENTITIES = "includeEntities";
	public static final String SHARED_SECRET = "sharedSecret";
	public static final String LOGIN_SECRET = "loginSecret";
	public static final String LOGIN_ID_1 = "loginID";
	public static final String CLOSE_API = "/papi/session/close";
	public static final String BAD_REQUEST_CODE = "400";
	
	public static final String SCHEMA_DEFINITIONS = "definitions";
	public static final String SCHEMA_PROJECTS = "Project";
	public static final String SCHEMA_PROPERTIES = "properties";
	public static final String SCHEMA_ITEMS = "items";
	public static final String SCHEMA_TYPE = "type";
	public static final String SCHEMA_REF = "$ref";
	public static final String SCHEMA_ARRAY = "array";
	public static final String SCHEMA_OBJECT = "object";
	public static final String SCHEMA_DATA = "Data";
}
