// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap.util;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConstants {
	
	private SAPConstants() {
		super();
	}
	
	public static final String HOST = "host";
	public static final String APIKEY = "apikey";
	public static final String BUSINESS_HUB_USERNAME = "businessHubUserName";
	public static final String BUSINESS_HUB_PASS = "businessHubPassword";
	public static final String SAP_USER = "sapUser";
	public static final String SAP_PASS = "sapPassword";
	
	public static final String URL = "url";
	public static final String URL_ERROR_MSG = "Please Provide Url before importing the profile";
	public static final String NO_SUPPORTED_API_MSG = "No supported API's for the selected Operation";
	public static final String GUID = "guid'";
	public static final String DATETIME = "datetime";
	public static final int HTTPSTATUS_OK = 200;
	public static final int HTTPSTATUS_CREATED = 201;
	public static final int HTTPSTATUS_ACCEPTED = 202;
	public static final int HTTPSTATUS_UNAUTHORIZED = 401;
	public static final String SELECT = "$select";
	public static final String ENUM_LABEL = "enum";
	public static final String FILTER = "$filter";
	public static final String ORDERBY = "$orderby";
	public static final String RESULTS = "results";
	public static final String PREPEND_UNDERSCORE = "_";
	public static final String GZIP = "gzip";
	public static final String CONNECTION_LEAK_MSG = "Possible Connection Leak.";
	public static final String APPLICATION_JSON = "application/json";
	public static final String ACCEPT = "Accept";
	public static final String CONTENT_TYPE = "Content-Type";
	public static final String HTTPS = "https";
	public static final String AUTHORIZATION = "Authorization";
	public static final String INPUT_JSON_ERROR = "Error while parsing provided input json, Please provide the valid Json.";
	public static final String INVALID_PATH_PARAMS = "Please provide valid/required path parameters.";

}
