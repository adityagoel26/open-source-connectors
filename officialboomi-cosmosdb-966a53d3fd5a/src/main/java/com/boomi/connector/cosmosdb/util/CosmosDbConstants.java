//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.util;

import com.boomi.util.ByteUnit;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDbConstants {
	
	private CosmosDbConstants() {
		
	}
	
	/** The Constant ID. */
	public static final String ID = "ID";
	
	/** The Constant ID_LOWERCASE */
	public static final String ID_LOWERCASE = "id";
	
	/** The Constant PART_KEY. */
	public static final String PART_KEY = "Partition Key";
	
	/** The Constant DELETE_ID. */
	public static final String DELETE_ID = "id";
	
	/** The Constant DELETE_PART_KEY. */
	public static final String DELETE_PART_KEY = "partitionKey";
	
	/** The Constant HOST_URL. */
	public static final String HOST_URL = "hostUrl";
	
	/** The Constant MASTER_KEY. */
	public static final String MASTER_KEY = "masterKey";
	
	/** The Constant DB_NAME. */
	public static final String DB_NAME = "databaseName";
	
	/** The Constant AUTHORIZATION. */
	public static final String AUTHORIZATION = "Authorization";
	
	/** The Constant AUTH_TOKEN. */
	public static final String AUTH_TOKEN = "auth-token";
	
	/** The Constant X_MS_VERSION. */
	public static final String X_MS_VERSION = "x-ms-version";
	
	/** The Constant X_MS_CONTINUATION. */
	public static final String X_MS_CONTINUATION = "x-ms-continuation";
	
	public static final String HEADER_PARTITION_KEY = "x-ms-documentdb-partitionkey";
		
	/** The Constant ACTUAL_MS_VERSION. */
	public static final String ACTUAL_MS_VERSION = "2017-02-22";
	
	/** The Constant PARTITION_KEY_HEADER. */
	public static final String PARTITION_KEY_HEADER = "x-ms-documentdb-partitionkey";
	
	/** The Constant X_MS_DATE. */
	public static final String X_MS_DATE ="x-ms-date";
	
	/** The Constant X_MS_DATE_FORMAT. */
	public static final String X_MS_DATE_FORMAT ="EEE, dd MMM yyyy HH:mm:ss zzz";
	
	/** The Constant RFC_TIME. */
	public static final String RFC_TIME ="RFC1123time";
	
	/** The Constant HTTP_GET. */
	public static final String HTTP_GET ="GET";
	
	/** The Constant HTTP_GET. */
	public static final String HTTP_PUT ="PUT";
	
	/** The Constant HTTP_POST. */
	public static final String HTTP_POST ="POST";
	
	/** The Constant HTTP_DELETE. */
	public static final String HTTP_DELETE ="DELETE";
	
	/** The Constant COLL. */
	public static final String COLL ="/colls";
	
	/** The Constant DOCS. */
	public static final String DOCS ="/docs/";
	
	/** The Constant DOCS. */
	public static final String CREATE_DOCS = "/docs";
	
	public static final String X_MS_DOCUMENT = "x-ms-documentdb-is-upsert";
	
	public static final String TRUE = "true";
	
	public static final String FALSE = "false";
	
	public static final String UNKNOWN_FAILURE ="Unknown Failure";
	
	/** The Constant DB. */
	public static final String DB = "dbs/";
	
	/** The Constant COLLS. */
	public static final String COLLS = "/colls/";

	/** The Constant APPLICATION_JSON. */
	public static final String APPLICATION_JSON = "application/json";
	
	
	/** The Constant ACCEPT. */
	public static final String ACCEPT = "Accept";
	
	public static final String DOUBLE_QUOTE = "\"";
	
	/** The Constant OBJECTID. */
	public static final String OBJECTID = "objectId";
	
	/** The Constant CUSTOM_ACTION_TYPE. */
	public static final String CUSTOM_ACTION_TYPE = "customActionType";
	
	/** The Constant COLLECTION. */
	public static final String COLLECTION = "collection";
	
	/** The Constant DATASTRUCTURE. */
	public static final String DATASTRUCTURE = "structureData";
	
	/** The Constant PARTITION_KEY_VALUE. */
	public static final String PARTITION_KEY_VALUE = "partitionKey";
	
	/** The Constant STATUS_CODE_SUCCESS. */
	public static final String STATUS_CODE_SUCCESS = "200";
	
	/** The Constant STATUS_MESSAGE_SUCCESS. */
	public static final String STATUS_MESSAGE_SUCCESS = "processed successfully";
	
	/** The Constant CONNECTION_ERROR_MSG. */
	public static final String CONNECTION_ERROR_MSG = "Unable to connect to CosmosDB with connectionString-";
	
	/** The Constant PARTITION_KEY_HEADER_START. */
	public static final String PARTITION_KEY_HEADER_START = "[\"";
	
	/** The Constant PARTITION_KEY_HEADER_END. */
	public static final String PARTITION_KEY_HEADER_END = "\"] ";
	
	/** The Constant CURLY_BRACE_END. */
	public static final String CURLY_BRACE_END = "}";
	
	/** The Constant SQUARE_BRACKET_END. */
	public static final String SQUARE_BRACKET_END = "]";
	
	/** The Constant SQUARE_BRACKET_START. */
	public static final String SQUARE_BRACKET_START = "[";
	
	/** The Constant COMMA. */
	public static final String COMMA = ",";
	
	/** The Constant FOR_DATABASE. */
	public static final String FOR_DATABASE = " and Database: ";
	
	/** The Constant PROFILE_ERROR_MSG_ONE. */
	public static final String PROFILE_ERROR_MSG_ONE = "Invalid Profile config for ";
	
	/** The Constant PROFILE_ERROR_MSG_TWO. */
	public static final String PROFILE_ERROR_MSG_TWO = " Config provided isStructuredData=";

	/** The Constant PROFILE_ERROR_MSG_THREE. */
	public static final String PROFILE_ERROR_MSG_THREE = " and objectId=";
	
	/** The Constant PROFILE_ERROR_MSG_FOUR. */
	public static final String PROFILE_ERROR_MSG_FOUR = " for operation=";
	
	/** The Constant SCHEMA_ARRAY. */
	public static final String SCHEMA_ARRAY = "array\", \"items\": [";
	
	/** The Constant SCHEMA_PROPERTIES. */
	public static final String SCHEMA_PROPERTIES = "object\", \"properties\": {";
	
	/** The Constant SCHEMA_BOOLEAN. */
	public static final String SCHEMA_BOOLEAN = "boolean\" ";
	
	/** The Constant SCHEMA_NULL. */
	public static final String SCHEMA_NULL = "null\" ";
	
	/** The Constant SCHEMA_NUMBER. */
	public static final String SCHEMA_NUMBER = "number\" ";
	
	/** The Constant SCHEMA_INTEGER. */
	public static final String SCHEMA_INTEGER = "integer\" ";
	
	/** The Constant SCHEMA_STRING. */
	public static final String SCHEMA_STRING = "string\" ";
	
	/** The Constant SCHEMA_TYPE. */
	public static final String SCHEMA_TYPE = "{ \"type\": \"";
	
	/** The Constant SCHEMA_START. */
	public static final String SCHEMA_START = "{\"type\": \"object\", \"properties\": {";
	
	/** The Constant SCHEMA_TYPE_TOKEN. */
	public static final String SCHEMA_TYPE_TOKEN = "\": { \"type\": \"";
	
	/** The Constant SCHEMA_UNHANDLED_TOKEN. */
	public static final String SCHEMA_UNHANDLED_TOKEN = "Unhandled token type: {}";
	
	/** The Constant CONNECTION_SUCCESS_MSG. */
	public static final String CONNECTION_SUCCESS_MSG = "Connection is successfull";
	
	/** The Constant SIGNATURE_ALGORITHM. */
	public static final String SIGNATURE_ALGORITHM =  "HmacSHA256";
	
	/** The Constant UTF. */
	public static final String UTF = "UTF-8";
	
	/** The Constant SIGNATURE_PARAMS. */
	public static final String SIGNATURE_PARAMS = "type=master&ver=1.0&sig=";
	
	/** The Constant LINE_BREAK. */
	public static final String LINE_BREAK = "\n";
	
	/** The Constant LINE_BREAK. */
	public static final String GMT = "GMT";
	
	//Update Operation 
	
	/** The Constant QUERY_MAXRETRY. */
	public static final String QUERY_MAXRETRY = "maxRetryCount";
	
	/** The Constant DEFAULTMAXRETRY. */
	public static final Integer DEFAULTMAXRETRY = 3;

	public static final String JSON_PARSING_ERROR_MSG = "Could not parse request data, invalid JSON";
	
	public static final long MAX_SIZE = ByteUnit.MB.getByteUnitSize();
	
	public static final String JSON_SCHEMA_FIELD_TYPE = "type";
	
	public static final String JSON_SCHEMA_FIELD_PROPERTIES = "properties";
	
	public static final String JSON_SCHEMA_FIELD_ITEMS = "items";
	
	/** The Constant ASCENDING_ORDER. */
	public static final String ASCENDING_ORDER = "asc";
	
	/** The Constant DESCENDING_ORDER. */
	public static final String DESCENDING_ORDER = "desc";
	
	/** The Constant APPLICATION_JSON_QUERY. */
	public static final String APPLICATION_JSON_QUERY = "application/query+json";
	
	/** The Constant QUERY_PREFIX. */
	public static final String QUERY_PREFIX = "{ \"query\": \"";
	
	/** The Constant QUERY_SUFFIX. */
	public static final String QUERY_SUFFIX = "\",\"parameters\": [] } ";
	
	/** The Constant REGEX_CSV_FORMAT. */
	public static final String REGEX_CSV_FORMAT = "\\s*,\\s*";
	
	/** The Constant CONTENT_TYPE. */
	public static final String CONTENT_TYPE = "Content-Type";
	
	/** The Constant QUERY_ROOT. */
	public static final String QUERY_ROOT = "root.";

	/** The Constant BLANK_SPACE. */
	public static final String BLANK_SPACE = " ";
	
	/** The Constant UPSERT. */
	public static final String UPSERT = "UPSERT";
	
	/**
	 * Enum representing the supported query operations (these names match the ids
	 * specified in the connector descriptor). Each operation has a prefix field
	 * which is used when sending the query filter to the service. 
	 * used eq, ge, le.
	 */
	public enum QueryOp {
		EQUALS("="), NOT_EQUALS("<>"), GREATER_THAN(">"), LESS_THAN("<"), GREATER_THAN_OR_EQUALS(">="),
		LESS_THAN_OR_EQUALS("<="), IN_LIST("in");
		
		private final String prefix;

		private QueryOp(String prefixs) {
			prefix= prefixs;
		}

		public String getPrefix() {
			return prefix;
		}
	}
}
