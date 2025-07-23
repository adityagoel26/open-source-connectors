// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.mongodb.constants;

import com.boomi.util.ByteUnit;

/**
 * Constants used in the connector.
 *
 */
public class MongoDBConstants {
	
	/**
	 * Instantiates a new mongo DB constants.
	 */
	private MongoDBConstants() {
	    throw new IllegalStateException("Utility class");
	  }
	
	/** The Constant BOOLEAN_TRUE. */
	public static final String BOOLEAN_TRUE = Boolean.TRUE.toString();
	
	/** The Constant BOOLEAN_FALSE. */
	public static final String BOOLEAN_FALSE = Boolean.FALSE.toString();
	
	/** The Constant NULL_STRING. */
	public static final String NULL_STRING = "null";
	
	/** The Constant DOUBLE_QUOTE. */
	public static final String DOUBLE_QUOTE = "\"";
	
	/** The Constant SCHEMA_FIELD_TYPE. */
	public static final String SCHEMA_FIELD_TYPE = "type";
	
	/** The Constant KDC. */
	public static final String KDC = "kdc";
	
	/** The Constant REALM. */
	public static final String REALM = "realm";
	
	/** The Constant KRB5PATH. */
	public static final String KRB_PATH = "krb5Path";
	
	/** The Constant GSSPATH. */
	public static final String JAAS_PATH = "gssPath";
	
	/** The Constant MONGO_CONNECTION_URL_SCHEME. */
	public static final String MONGO_CONNECTION_URL_SCHEME = "mongodb://";
	
	/** The Constant ASCENDING_ORDER. */
	public static final String ASCENDING_ORDER = "asc";
	
	/** The Constant DESCENDING_ORDER. */
	public static final String DESCENDING_ORDER = "desc";
	
	/** The Constant MONGO_CLIENT_APP_NAME. */
	public static final String MONGO_CLIENT_APP_NAME = "BoomiMongoDBConnector";
	
	/** The Constant HOSTNAME. */
	public static final String HOSTNAME = "hostname";
	
	/** The Constant PORT. */
	public static final String PORT = "port";
	
	/** The Constant ALIAS. */
	public static final String ALIAS = "aliasName";
	
	/** The Constant DATABASE. */
	public static final String DATABASE = "database";
	
	/** The Constant REGEX_CSV_FORMAT. */
	public static final String REGEX_CSV_FORMAT = "\\s*,\\s*";
	
	/** The Constant SQUARE_BRACKET_OPEN. */
	public static final String SQUARE_BRACKET_OPEN = "[";
	
	/** The Constant SQUARE_BRACKET_CLOSE. */
	public static final String SQUARE_BRACKET_CLOSE = "]";
	
	/** The Constant REPLICA_SET_MEMBERS. */
	public static final String REPLICA_SET_MEMBERS = "replicaSetMembers";
	
	/** The Constant AUTHENTICATION_TYPE. */
	public static final String AUTHENTICATION_TYPE = "authtype";
	
	/** The Constant USER_NAME. */
	public static final String USER_NAME = "username";
	
	/** The Constant PASSWORD. */
	public static final String CONSTANT_MONGOPD = "password";
	
	/** The Constant SERVICE_PRINCIPAL. */
	public static final String SERVICE_PRINCIPAL = "servicePrincipal";
	
	/** The Constant FIELD. */
	public static final String FIELD = "for field-";
	
	/** The Constant USESSL. */
	public static final String USESSL = "useSSL";
	
	/** The Constant USER_CERTIFICATE. */
	public static final String USER_CERTIFICATE = "userCert";
	
	public static final String ID_FIELD_NAME = "_id";
	
	/** The Constant TRUST_STORE. */
	public static final String TRUST_STORE = "caCert";
	
	/** The Constant COLLECTION_NAME. */
	public static final String COLLECTION_NAME = "collectionName"; 
	
	/** The Constant AUTHDATABASE. */
	public static final String AUTHDATABASE = "authDatabase";
	
	/** The Constant CONNECTIONSTRING. */
	public static final String CONNECTIONSTRING = "connectionString";
	
	/** The Constant COLLECTION_SCHEMA. */
	public static final String COLLECTION_SCHEMA = "collectionSchema";
	
	/** The Constant AUTH_TYPE_NONE. */
	public static final String AUTH_TYPE_NONE = "None";
	
	/** The Constant SCRAM_SHA_1. */
	public static final String SCRAM_SHA_1 = "SCRAMSHA1";
	
	/** The Constant SCRAM_SHA_256. */
	public static final String SCRAM_SHA_256 = "SCRAMSHA256";
	
	/** The Constant X509. */
	public static final String X509 = "X.509";
	
	/** The Constant LDAP. */
	public static final String LDAP = "LDAP";
	
	/** The Constant KERBEROS. */
	public static final String KERBEROS = "KERBEROS";
	
	/** The Constant QUERY_PROJECTION. */
	public static final String QUERY_PROJECTION = "projection";
	
	/** The Constant QUERY_MAXRETRY. */
	public static final String QUERY_MAXRETRY = "maxRetryCount";
	
	/** The Constant IS_UPSERT_OPERATION. */
	public static final String IS_UPSERT_OPERATION = "isUpsertOperation";
	
	/** The Constant QUERY_BATCHSIZE. */
	public static final String QUERY_BATCHSIZE = "batchSize";
	
	/** The Constant INCLUDE_SIZE_EXCEEDED_PAYLOAD. */
	public static final String INCLUDE_SIZE_EXCEEDED_PAYLOAD = "includeSizeExceededPayload";
	
	/** The Constant STATUS_CODE_SUCCESS. */
	public static final String STATUS_CODE_SUCCESS = "200";
	
	/** The Constant DOCUMENT. */
	public static final String DOCUMENT = "document";

	/** The Constant INVALID_OBJECTID_ERROR. */
	public static final String INVALID_OBJECTID_ERROR = "Record ID is not valid ObjectId MongoDB type";
	
	/** The Constant STATUS_MESSAGE_SUCCESS. */
	public static final String STATUS_MESSAGE_SUCCESS = "processed successfully";
	
	/** The Constant STATUS_CODE_FAILURE. */
	public static final String STATUS_CODE_FAILURE = "500";
	
	/** The Constant OBJRESPONSE. */
	public static final String OBJRESPONSE = "_response";
	
	/** The Constant OBJREQUEST. */
	public static final String OBJREQUEST = "_request";
	
	/** The Constant OBJECTID. */
	public static final String OBJECTID = "objectId";
	
	/** The Constant DATATYPE. */
	public static final String DATATYPE = "datatype";
	
	/** The Constant COLLECTION. */
	public static final String COLLECTION = "collection";
	
	/** The Constant DATASTRUCTURE. */
	public static final String DATASTRUCTURE = "structureData";
	
	/** The Constant BOOMI_FILTER_FIELD. */
	public static final String BOOMI_FILTER_FIELD = "boomiSdkBrowseFilter2016";
	
	/** The Constant NEW_COLL_LABEL. */
	public static final String NEW_COLL_LABEL = "(new)";
	
	/** The Constant TIMEMASK. */
	public static final String TIMEMASK = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	
	/** The Constant DEFAULTBATCHSIZE. */
	public static final long DEFAULTBATCHSIZE = 1L;
	
	/** The Constant TOTAL_TIME_ALL_BATCHES. */
	public static final String TOTAL_TIME_ALL_BATCHES = "totalTimeForAllBatches";
	
	/** The Constant MAX_TIME_FOR_BATCH. */
	public static final String MAX_TIME_FOR_BATCH = "maxTimeForBatch";
	
	/** The Constant MIN_TIME_FOR_BATCH. */
	public static final String MIN_TIME_FOR_BATCH = "minTimeForBatch";
	
	/** The Constant TIME_UNIT_MILLISEC. */
	public static final String TIME_UNIT_MILLISEC = "ms";
	
	/** The Constant HIDDEN. */
	public static final String HIDDEN = "<HIDDEN>";
	
	/** The Constant SINGLE_SPACE. */
	public static final String SINGLE_SPACE = " ";
	
	/** The Constant COMMA. */
	public static final String COMMA = ",";
	
	/** The Constant COLON. */
	public static final String COLON = ":";
	
	/** The Constant AMPERSAND. */
	public static final String AMPERSAND = "&";
	
	/** The Constant AT. */
	public static final String AT = "@";
	
	/** The Constant FORWARD_SLASH. */
	public static final String FORWARD_SLASH = "/";
	
	public static final String DOUBLE_FORWARD_SLASH = "//";
	
	/** The Constant DOT. */
	public static final String DOT = ".";
	
	/** The Constant QUERY_FILTER. */
	public static final String QUERY_FILTER = "queryFilter";
	
	/** The Constant SORT_SPEC. */
	public static final String SORT_SPEC = "sortSpec";
	
	/** The Constant JSON_SCHEMA_FIELD_TYPE. */
	public static final String JSON_SCHEMA_FIELD_TYPE = "type";
	
	/** The Constant JSON_SCHEMA_FIELD_PROPERTIES. */
	public static final String JSON_SCHEMA_FIELD_PROPERTIES = "properties";
	
	/** The Constant JSON_SCHEMA_FIELD_ITEMS. */
	public static final String JSON_SCHEMA_FIELD_ITEMS = "items";
	
	/** The Constant QUESTION_MARK. */
	public static final String QUESTION_MARK = "?";
	
	/** The Constant OBJECT. */
	public static final String OBJECT = "object";
	
	/** The Constant STRING. */
	public static final String STRING = "string";
	
	/** The Constant MONGOSRV. */
	public static final String MONGO_SRV = "mongosrv";
	
	/** The Constant CONNECTION_STRING. */
	public static final String CONNECTION_STRING = "connstring";
	
	/** The Constant COOKIE. */
	public static final String COOKIE="lkstrey";

	/** The configuration key for specifying the maximum document size for MongoDB operations. */
	public static final String MAX_DOCUMENT_SIZE_PROPERTY_KEY ="com.boomi.connector.mongodb.maxDocumentSize";

	/** The default maximum document size, set to 1 megabyte. */
	public static final long DEFAULT_MAX_DOCUMENT_SIZE = ByteUnit.MB.getByteUnitSize();
}
