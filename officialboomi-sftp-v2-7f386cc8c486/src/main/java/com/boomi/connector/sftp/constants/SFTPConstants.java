//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.constants;

import com.boomi.util.ObjectUtil;
import com.boomi.util.SystemUtil;

import java.util.concurrent.TimeUnit;

/**
 * The Class SFTPConstants.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SFTPConstants {

	private SFTPConstants() {
		//Hide implicit constructor
	}

	/** The Constant PROPERTY_REMOTE_DIRECTORY. */
	public static final String PROPERTY_REMOTE_DIRECTORY = "directory";

	/** The Constant PROPERTY_MOVETO_DIRECTORY. */
	public static final String PROPERTY_MOVETO_DIRECTORY = "movetoDirectory";

	/** The Constant PROPERTY_FILENAME. */
	public static final String PROPERTY_FILENAME = "fileName";
	
	/** The Constant PROPERTY_FILE_CONTENT. */
	public static final String PROPERTY_FILE_CONTENT = "fileContent";

	/** The Constant PROPERTY_PORT. */
	public static final String PROPERTY_PORT = "port";

	/** The Constant FILESIZE. */
	public static final String FILESIZE = "fileSize";

	/** The Constant IS_DIRECTORY. */
	public static final String IS_DIRECTORY = "isDirectory";

	/** The Constant MODIFIED_DATE. */
	public static final String MODIFIED_DATE = "modifiedDate";

	/** The Constant REMOTE_DIRECTORY. */
	public static final String REMOTE_DIRECTORY = "remoteDirectory";
	
	public static final String FILE_NAME = "fileName";

	/** The Constant PROPERTY_HOST. */
	public static final String PROPERTY_HOST = "host";
	
	/** The Constant PROXY_HOST. */
	public static final String PROXY_HOST = "proxyHost";
	
	public static final String PROXY_PORT = "proxyPort";
	
	/** The Constant PROXY_USERNAME. */
	public static final String PROXY_USERNAME = "proxyuserName";
	
	/** The Constant PROXY_PKEY. */
	public static final String PROXY_PKEY = "proxyPassword";
	
	/** The Constant PROXY_ENABLED. */
	public static final String PROXY_ENABLED = "proxyEnable";
	
	/** The Constant PROXY_TYPE. */
	public static final String PROXY_TYPE = "proxyType";

	/** The Constant PROPERTY_USERNAME. */
	public static final String PROPERTY_USERNAME = "username";

	/** The Constant PROPERTY_PKEY. */
	public static final String PROPERTY_PKEY = "password";

	/** The Constant AUTHORIZATION_TYPE. */
	public static final String AUTHORIZATION_TYPE = "authType";

	/** The Constant INCLUDE_ALL. */
	public static final String INCLUDE_ALL = "includeAll";

	/** The Constant APPEND. */
	public static final String APPEND = "append";

	/** The Constant CREATE_DIR. */
	public static final String CREATE_DIR = "createDir";
	
	public static final String ASCENDING_ORDER = "asc";
	
	public static final String DESCENDING_ORDER = "desc";

	/** The Constant USERNAME_AND_PASSWORD. */
	public static final String USERNAME_AND_PASSWORD = "Username and Password";

	/** The Constant USING_PUBLIC_KEY. */
	public static final String USING_PUBLIC_KEY = "Using public Key";
	
	public static final String POLLING_INTERVAL = "pollingInterval";

	/** The Constant OP_REGEX. */
	public static final String OP_REGEX = "REGEX";

	/** The Constant OP_WILDCARD. */
	public static final String OP_WILDCARD = "WILDCARD";

	/** The Constant UNKNOWN_EXPRESSION. */
	public static final String UNKNOWN_EXPRESSION = "Unknown expression type: ";

	/** The Constant EXACTLY_ONE_ARGUEMENT_REQUIRED. */
	public static final String EXACTLY_ONE_ARGUEMENT_REQUIRED = "There should be exactly one argument. ''{0}'' given.";

	/** The Constant PATTERN_UNSUPPORTED_FOR_FILENAMES. */
	public static final String PATTERN_UNSUPPORTED_FOR_FILENAMES = "Pattern type operators (regex and wildcard) are currently only supported for filenames";

	/** The Constant UNKNOWN_PROPERTY. */
	public static final String UNKNOWN_PROPERTY = "Unknown property: ";

	/** The Constant QUOTE. */
	public static final String QUOTE = "\"";

	/** The Constant INVALID_BOOLEAN_VALUE. */
	public static final String INVALID_BOOLEAN_VALUE = "\" is not a valid boolean value";

	/** The Constant UNABLE_TO_PARSE_DATE. */
	public static final String UNABLE_TO_PARSE_DATE = "Unable to parse date: \"";

	/** The Constant REGEX. */
	public static final String REGEX = "regex";
	
	public static final String ENABLE_POOLING= "enablePooling";

	/** The Constant GLOB. */
	public static final String GLOB = "glob";

	/** The Constant MATCHING_WITH_PATTERN. */
	public static final String MATCHING_WITH_PATTERN = "Matching \"%s\" with pattern \"%s\" (op = %s); result = %b";

	/** The Constant COMPARISON_YIELDS. */
	public static final String COMPARISON_YIELDS = "Comparison %s %s %s yields %b";

	/** The Constant UNKNOWN_GROUPING_OPERATOR. */
	public static final String UNKNOWN_GROUPING_OPERATOR = "Unknown grouping operator: ";

	/** The Constant PATH_TYPE. */
	public static final String PATH_TYPE = "FilePath";

	/** The Constant COMPARABLE_TYPE. */
	public static final String COMPARABLE_TYPE = "Comparable";

	/** The Constant BOOLEAN_TYPE. */
	public static final String BOOLEAN_TYPE = "Boolean";

	/** The Constant OBJECT_TYPE_FILE. */
	public static final String OBJECT_TYPE_FILE = "File";

	/** The Constant SIMPLE_FILE_META_SCHEMA_PATH. */
	public static final String SIMPLE_FILE_META_SCHEMA_PATH = "/schemas/simple-file-metadata.schema.json";

	/** The Constant EXTENDED_FILE_META_SCHEMA_PATH. */
	public static final String EXTENDED_FILE_META_SCHEMA_PATH = "/schemas/extended-file-metadata.schema.json";

	/** The Constant ERROR_SCHEMA_LOAD_FORMAT. */
	public static final String ERROR_SCHEMA_LOAD_FORMAT = "the '%s' schema could not be loaded.";

	/** The Constant PROPERTY_INCLUDE_METADATA. */
	public static final String PROPERTY_INCLUDE_METADATA = "includeAllMetadata";

	/** The Constant SFTP. */
	public static final String SFTP = "sftp";
	
	/** The Constant CONNECTION_PARAM_CONN_TIMEOUT. */
	public static final String CONNECTION_PARAM_CONN_TIMEOUT = "connectionTimeout";
	
	/** The Constant CONNECTION_PARAM_READ_TIMEOUT. */
	public static final String CONNECTION_PARAM_READ_TIMEOUT = "readTimeout";

	/** The Constant DEFAULT_TIMEOUT_IN_MS. */
	private static final long DEFAULT_TIMEOUT_IN_MS = TimeUnit.MINUTES.toMillis(2L);

	/** The Constant DEFAULT_READ_TIMEOUT. */
	public static final long DEFAULT_READ_TIMEOUT = (Long) ObjectUtil
			.defaultIfNull((Object) SystemUtil.DEFAULT_NETWORK_READ_TIMEOUT, (Object) DEFAULT_TIMEOUT_IN_MS);

	/** The Constant DEFAULT_CONNECTION_TIMEOUT. */
	public static final long DEFAULT_CONNECTION_TIMEOUT = (Long) ObjectUtil
			.defaultIfNull((Object) SystemUtil.DEFAULT_NETWORK_CONNECT_TIMEOUT, (Object) DEFAULT_TIMEOUT_IN_MS);

	/** The Constant ERROR_FAILED_RENAME. */
	public static final String ERROR_FAILED_RENAME = "Could not rename path ''{0}'' to ''{1}''";

	/** The Constant ERROR_FAILED_FILE_RETRIEVAL. */
	public static final String ERROR_FAILED_FILE_RETRIEVAL = "An error occurred while retrieving the file at path ''{0}''";

	/** The Constant ERROR_FAILED_DIRECTORY_CREATE. */
	public static final String ERROR_FAILED_DIRECTORY_CREATE = "An error occurred while creating the ''{0}'' directory.";

	/** The Constant ERROR_FAILED_LISTING_FILENAMES. */
	public static final String ERROR_FAILED_LISTING_FILENAMES = "An error occured while listing the files in directory";

	/** The Constant ERROR_FAILED_FILE_REMOVAL. */
	public static final String ERROR_FAILED_FILE_REMOVAL = "An error occurred while removing the file at path ''{0}''";

	/** The Constant ERROR_FAILED_RETRIEVING_DIRECTORY. */
	public static final String ERROR_FAILED_RETRIEVING_DIRECTORY = "An error occured while listing the files in directory";

	/** The Constant ERROR_FAILED_CHANGING_CURRENT_DIRECTORY. */
	public static final String ERROR_FAILED_CHANGING_CURRENT_DIRECTORY = "An error occured while changing the current directory to ''{0}'' ";

	/** The Constant ERROR_FAILED_FILE_UPLOAD. */
	public static final String ERROR_FAILED_FILE_UPLOAD = "An error occurred while creating the file at path ''{0}''";

	/** The Constant ERROR_FAILED_CONNECTION_TO_HOST. */
	public static final String ERROR_FAILED_CONNECTION_TO_HOST = "Sftp connection to host failed";

	/** The Constant ERROR_FAILED_CREATING_SESSION. */
	public static final String ERROR_FAILED_CREATING_SESSION = "An error occurred while creating session";

	/** The Constant ERROR_FAILED_CONNECTION_TO_PORT. */
	public static final String ERROR_FAILED_CONNECTION_TO_PORT = "An error occurred while connecting to port";

	/** The Constant ERROR_FAILED_SFTP_LOGIN. */
	public static final String ERROR_FAILED_SFTP_LOGIN = "SFTP login failed";

	/** The Constant ERROR_FAILED_SFTP_SERVER_CONNECTION. */
	public static final String ERROR_FAILED_SFTP_SERVER_CONNECTION = "Error connecting to SFTP server";

	/** The Constant ERROR_FAILED_OPENING_SFTP_CHANNEL. */
	public static final String ERROR_FAILED_OPENING_SFTP_CHANNEL = "Error opening SFTP channel: Read timeout";

	/** The Constant ERROR_FAILED_CLOSING_CONNECTION. */
	public static final String ERROR_FAILED_CLOSING_CONNECTION = "Connection was not closed properly.";

	/** The Constant ERROR_FAILED_GETING_FILE_METADATA. */
	public static final String ERROR_FAILED_GETING_FILE_METADATA = "Error getting file metadata at path ''{0}''";

	/** The Constant ERROR_FAILED_GETING_DIRECTORY_CONTENTS. */
	public static final String ERROR_FAILED_GETING_DIRECTORY_CONTENTS = "Error getting directory contents at path ''{0}''";

	/** The Constant ERROR_FAILED_CHECKING_FILE_EXISTENCE. */
	public static final String ERROR_FAILED_CHECKING_FILE_EXISTENCE = "Error checking file existence at path ''{0}''";

	/** The Constant LEGACY_ALGO_LIST. */
	public static final String LEGACY_ALGO_LIST = "diffie-hellman-group1-sha1,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1";

	/** The Constant KEX. */
	public static final String KEX = "kex";

	/** The Constant PROP_STRICT_HOST_KEY_CHECKING. */
	public static final String PROP_STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";

	/** The Constant SHKC_NO. */
	public static final String SHKC_NO = "no";

	/** The Constant AUTH_SEQUENCE_FULL. */
	public static final String AUTH_SEQUENCE_FULL = "publickey,password,keyboard-interactive";

	/** The Constant PREFERRED_AUTHENTICATIONS. */
	public static final String PREFERRED_AUTHENTICATIONS = "PreferredAuthentications";

	/** The Constant KEY_COMP_S2C_ALG. */
	public static final String KEY_COMP_S2C_ALG = "compression.s2c";

	/** The Constant KEY_COMP_C2S_ALG. */
	public static final String KEY_COMP_C2S_ALG = "compression.c2s";

	/** The Constant DH_GROUP_EXCHANGE_SHA1. */
	public static final String DH_GROUP_EXCHANGE_SHA1 = "diffie-hellman-group-exchange-sha1";

	/** The Constant DH_GROUP_EXCHANGE_SHA256. */
	public static final String DH_GROUP_EXCHANGE_SHA256 = "diffie-hellman-group-exchange-sha256";

	/** The Constant CLASS_DHGEX1024. */
	public static final String CLASS_DHGEX1024 = "com.boomi.connector.sftp.DHGEX1024";

	/** The Constant CLASS_DHGEX256_1024. */
	public static final String CLASS_DHGEX256_1024 = "com.boomi.connector.sftp.DHGEX256_1024";

	/** The Constant COMP_ALGS. */
	public static final String COMP_ALGS = "zlib,none";

	/** The Constant SIGNATURE_DSS_KEY. */
	public static final String SIGNATURE_DSS_KEY = "signature.dss";

	/** The Constant SIGNATURE_RSA_KEY. */
	public static final String SIGNATURE_RSA_KEY = "signature.rsa";

	/** The Constant CAUSE. */
	public static final String CAUSE = " Cause: ";

	/** The Constant UNABLE_TO_PARSE_SSH_KEY. */
	public static final String UNABLE_TO_PARSE_SSH_KEY = "Failed to load or parse SSH Key.  ";

	/** The Constant UTF_8. */
	public static final String UTF_8 = "UTF-8";

	/** The Constant UNABLE_TO_PPROCESS_KNOWN_HOST_KEY. */
	public static final String UNABLE_TO_PPROCESS_KNOWN_HOST_KEY = "Unable to process SSH Known Hosts Entry. Exception message is: ";

	/** The Constant DISCONNECTING_FROM_SFTP_SERVER. */
	public static final String DISCONNECTING_FROM_SFTP_SERVER = "Disconnecting from SFTP server.";

	/** The Constant ERROR_DISCONNECTING_CHANNEL. */
	public static final String ERROR_DISCONNECTING_CHANNEL = "Errors occurred disconnecting channel, will ignore.";

	/** The Constant ERROR_DISCONNECTING_SESSION. */
	public static final String ERROR_DISCONNECTING_SESSION = "Errors occurred disconnecting session, will ignore.";

	/** The Constant FILE_NOT_FOUND. */
	public static final String FILE_NOT_FOUND = "File Not Found";

	/** The Constant FILE_NOT_FOUND_EXCEPTION. */
	public static final String FILE_NOT_FOUND_EXCEPTION = "FileNotFoundException";

	/** The Constant ERROR_GETTING_HOME_DIR. */
	public static final String ERROR_GETTING_HOME_DIR = "Error fetching home directory";

	/** The Constant YES. */
	public static final String YES = "yes";

	/** The Constant ERROR_MISSING_INPUT_FILENAME. */
	public static final String ERROR_MISSING_INPUT_FILENAME = "the 'File Name' input document property is required, and must contain a non-blank value. This property contains the name of the file to upload.";

	/** The Constant ERROR_MISSING_DIRECTORY. */
	public static final String ERROR_MISSING_DIRECTORY = "A directory must be specified in the connection settings or as a document property for this operation";

	/** The Constant ERROR_QUERYING_FILES. */
	public static final String ERROR_QUERYING_FILES = "Unexpected error while querying files";

	/** The Constant LIMIT_MUST_BE_POSITIVE. */
	public static final String LIMIT_MUST_BE_POSITIVE = "Limit must be positive or -1 (given: %d)";

	/** The Constant UNEXPECTED_ERROR_OCCURED. */
	public static final String UNEXPECTED_ERROR_OCCURED = "An unexpected error occurred. File: \"%s\"";

	/** The Constant COUNT. */
	public static final String COUNT = "count";

	/** The Constant ERROR_RESUMING_UPLOAD. */
	public static final String ERROR_RESUMING_UPLOAD = "Error resuming upload";

	/** The Constant ERROR_CREATING_JSON_DEFINITION. */
	public static final String ERROR_CREATING_JSON_DEFINITION = "Error creating JSON definition";

	/** The Constant UNKNOWN_DEFINITION_ROLE. */
	public static final String UNKNOWN_DEFINITION_ROLE = "Unknown definition role: ";

	/** The Constant ERROR_REMOTE_DIRECTORY_NOT_FOUND. */
	public static final String ERROR_REMOTE_DIRECTORY_NOT_FOUND = "Remote directory does not exist";

	/** The Constant PROPERTY_DELETE_AFTER. */
	public static final String PROPERTY_DELETE_AFTER = "deleteAfter";

	/** The Constant PROPERTY_FAIL_DELETE_AFTER. */
	public static final String PROPERTY_FAIL_DELETE_AFTER = "failDeleteAfter";

	/** The Constant ERROR_MISSING_INPUT_FILENAME_ID. */
	public static final String ERROR_MISSING_INPUT_FILENAME_ID = "ID must contain a non-blank value. Id is the name of the file to download.";

	/** The Constant ERROR_UNABLE_TO_DELETE_FILE. */
	public static final String ERROR_UNABLE_TO_DELETE_FILE = "Unable to delete file";

	/** The Constant PROPERTY_STAGING_DIRECTORY. */
	public static final String PROPERTY_STAGING_DIRECTORY = "stagingDirectory";

	/** The Constant PROPERTY_TEMP_EXTENSION. */
	public static final String PROPERTY_TEMP_EXTENSION = "tempExtension";

	/** The Constant PROPERTY_TEMP_FILE_NAME. */
	public static final String PROPERTY_TEMP_FILE_NAME = "tempFileName";

	/** The Constant PROPERTY_TARGET_FILE_NAME. */
	public static final String PROPERTY_TARGET_FILE_NAME = "targetFileName";

	/** The Constant OPERATION_PROP_ACTION_IF_FILE_EXISTS. */
	public static final String OPERATION_PROP_ACTION_IF_FILE_EXISTS = "actionIfFileExists";

	/** The Constant ERROR_FILE_ALREADY_EXISTS_FORMAT. */
	public static final String ERROR_FILE_ALREADY_EXISTS_FORMAT = "the ''{0}'' file already exists in the remote directory on the SFTP server.";

	/** The Constant ERROR_INVALID_COOKIE. */
	public static final String ERROR_INVALID_COOKIE = "the cookie containing the browsing metadata for Create is not set, or does not contain a JSON value.";

	/** The Constant EXTENSION_SEPARATOR. */
	public static final String EXTENSION_SEPARATOR = ".";

	/** The Constant FILE_SEPARATOR. */
	public static final String FILE_SEPARATOR = "/";

	/** The Constant ASTERISK. */
	public static final String ASTERISK = "*";

	/** The Constant ERROR_STAGING_DIR_NOT_FOUND. */
	public static final String ERROR_STAGING_DIR_NOT_FOUND = "Staging directory does not exist";

	/** The Constant FILE_CREATED. */
	public static final String FILE_CREATED = "Created the file successfully";

	/** The Constant ERROR_DELETING_FILE. */
	public static final String ERROR_DELETING_FILE = "There was an unexpected error deleting \"%s\"";
	
	public static final String FILE_DELETED = "The file is deleled successfully!!";
	
	public static final String FILE_DOESNOT_EXISTS = "The file does not exists!!";

	/** The Constant INVALID_QUERY_TYPE. */
	public static final String INVALID_QUERY_TYPE = "Invalid Query type: ";

	/** The Constant ERROR_SEARCHING. */
	public static final String ERROR_SEARCHING = "An unexpected error occurred while searching";

	/** The Constant SUCCESS_MESSAGE. */
	public static final String SUCCESS_MESSAGE = "Success";

	/** The Constant FAILURE_CODE. */
	public static final String FAILURE_CODE = "-1";

	/** The Constant SUCCESS_CODE. */
	public static final String SUCCESS_CODE = "0";

	/** The Constant FILE_NOT_FOUND_CODE. */
	public static final String FILE_NOT_FOUND_CODE = "1";

	/** The Constant ERROR_LIST. */
	public static final String ERROR_LIST = "errors";

	/** The Constant MESSAGE. */
	public static final String MESSAGE = "message";

	/** The Constant CODE. */
	public static final String CODE = "code";

	/** The Constant ERROR_OCCURED_IN_FILES. */
	public static final String ERROR_OCCURED_IN_FILES = "An error occurred in %d files";

	/** The Constant ERROR_IN_ERR_DOC. */
	public static final String ERROR_IN_ERR_DOC = "Error occurred while finishing error document";

	/** The Constant KEY_PSWRD. */
	public static final String KEY_PSWRD = "keyPswrd";

	/** The Constant KEY_PATH. */
	public static final String KEY_PATH = "keyPath";

	/** The Constant HOST_ENTRY. */
	public static final String HOST_ENTRY = "hostEntry";

	/** The Constant IS_MAX_EXCHANGE. */
	public static final String IS_MAX_EXCHANGE = "isMaxExchange";

	/** The Constant INVALID_PATH. */
	public static final String INVALID_PATH = "Path must be non-null";

	/** The Constant ERROR_PARSING_DATE_FROM_FILESYSTEM. */
	public static final String ERROR_PARSING_DATE_FROM_FILESYSTEM = "Unable to parse date returned from file system";

	/** The Constant ERROR_INVALID_TIMESTAMP_INPUT. */
	public static final String ERROR_INVALID_TIMESTAMP_INPUT = "The specified timestamp is invalid and must be provided in this format ({0}).";

	/** The Constant UNABLE_TO_DELETE_FILE. */
	public static final String UNABLE_TO_DELETE_FILE = "Unable to delete the file";

	/** The Constant PRIVATE_KEY_CONTENT. */
	public static final String PRIVATE_KEY_CONTENT = "prvkeyContent";

	/** The Constant PUBLIC_KEY_CONTENT. */
	public static final String PUBLIC_KEY_CONTENT = "pubkeyContent";

	/** The Constant KEY_PAIR_NAME. */
	public static final String KEY_PAIR_NAME = "keyPairName";

	/** The Constant USE_KEY_CONTENT. */
	public static final String USE_KEY_CONTENT = "useKeyContent";

	/** The Constant PARAMETER_BASE. */
	public static final String PARAMETER_BASE = "$BASE";

	/** The Constant PARAMETER_EXTENSION. */
	public static final String PARAMETER_EXTENSION = "$EXTENSION";

	/** The Constant PARAMETER_UUID. */
	public static final String PARAMETER_UUID = "$UUID";

	/** The Constant PARAMETER_DATE. */
	public static final String PARAMETER_DATE = "$DATE";

	/** The Constant PARAMETER_TIME. */
	public static final String PARAMETER_TIME = "$TIME";

	/** The Constant DEFAULT_TIME_FOMRAT. */
	public static final String DEFAULT_TIME_FOMRAT = "HHmmss.SSS";

	/** The Constant DEFAULT_DATE_FORMAT. */
	public static final String DEFAULT_DATE_FORMAT = "yyyyMMdd";

	/** The Constant FILE_NOT_FOUND_MESSAGE. */
	public static final String FILE_NOT_FOUND_MESSAGE = "The entered directory for the field 'Client SSH Key File Path' is invalid.";

	/** The Constant INVALID_PRIVATEKEY. */
	public static final String INVALID_PRIVATEKEY = "invalid privatekey";

	/** The Constant INVALID_PRIVATEKEY_ERROR_MESSAGE. */
	public static final String INVALID_PRIVATEKEY_ERROR_MESSAGE = "An error occurred while creating a session. Cause: Invalid Private Key.";

	/** The Constant ACCESS_DENIED. */
	public static final String ACCESS_DENIED = "access denied";

	/** The Constant ACCESS_DENIED_ERROR_MESSAGE. */
	public static final String ACCESS_DENIED_ERROR_MESSAGE = "SFTP login failed Cause: access denied. Caused by: Invalid public key or server hostname";

	/** The Constant AUTH_FAIL. */
	public static final String AUTH_FAIL = "Auth fail";

	/** The Constant AUTH_FAIL_ERROR_MESSAGE. */
	public static final String AUTH_FAIL_ERROR_MESSAGE = "SFTP login failed Cause: access denied, Incorrect user credentials.";

	/** The Constant USER_AUTH_FAIL. */
	public static final String USER_AUTH_FAIL = "USERAUTH fail";

	/** The Constant USER_AUTH_FAIL_MESSAGE. */
	public static final String USER_AUTH_FAIL_MESSAGE = "SFTP login failed Cause: access denied. Caused by: Incorrect key file password or hostname.";

	/** The Constant PORT. */
	public static final String PORT = "port";

	/** The Constant PORT_ERROR_MESSAGE. */
	public static final String PORT_ERROR_MESSAGE = "An error occurred while connecting to the host with entered port. Cause: Connection Refused.";

	/** The Constant UNKNOWN_HOST. */
	public static final String UNKNOWN_HOST = "UnknownHostException";

	/** The Constant UNKNOWN_HOST_ERROR_MESSAGE. */
	public static final String UNKNOWN_HOST_ERROR_MESSAGE = "SFTP connection to host failed. Cause: Unknown Host.";

	/** The Constant HOSTNAME_BLANK. */
	public static final String HOSTNAME_BLANK = "The field 'Host' is blank.";

	/** The Constant USER_BLANK. */
	public static final String USER_BLANK = "The field 'User Name' is blank.";

	/** The Constant PASS_BLANK. */
	public static final String PASS_BLANK = "The field 'Password' is blank.";

	/** The Constant PORT_BLANK. */
	public static final String PORT_BLANK = "The field 'Port' is blank.";

	/** The Constant KEY_FILE_PATH. */
	public static final String KEY_FILE_PATH = "The field 'Client SSH Key File Path' is blank.";

	/** The Constant PRIVATE_KEY_BLANK. */
	public static final String PRIVATE_KEY_BLANK = "The field 'Private Key Content' is blank.";

	/** The Constant PUBLIC_KEY_BLANK. */
	public static final String PUBLIC_KEY_BLANK = "The field 'Public Key Content' is blank.";

	/** The Constant KEY_PAIR_NAME_BLANK. */
	public static final String KEY_PAIR_NAME_BLANK = "The field 'Key Pair Name' is blank.";



}
