/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.util;

/**
 * The constants that are used in the connector API
 * 
 * @author a.kumar.samantaray
 *
 */
public class AWSEventBridgeConstant {
	private AWSEventBridgeConstant() {
	}

	public static final String SCHEME = "AWS4";
	public static final String ALGORITHM = "HMAC-SHA256";
	public static final String TERMINATOR = "aws4_request";

	public static final String ISO8601BASICFORMAT = "yyyyMMdd'T'HHmmss'Z'";
	public static final String DATESTRINGFORMAT = "yyyyMMdd";
	public static final String UTC = "UTC";
	public static final String ALGO = "HmacSHA256";
	public static final String XMZDATE = "X-Amz-Date";
	public static final String HOST = "Host";
	public static final String SERVICE = "events";
	public static final String BASEURL = "https://%s.%s.amazonaws.com";
	public static final String ACCESSKEY = "accessKey";
	public static final String SECRETEKEY = "awsSecretKey";
	public static final String CUSTOMEREGION = "customAwsRegion";
	public static final String REGION = "awsRegion";
	public static final String ACOUNTID = "awsAccountID";
	public static final String CUSTOMEREGIONVALUE = "Custom-Region";

	public static final String CONTENTSHA256 = "X-Amz-Content-Sha256";
	public static final String HTTPMETHOD = "POST";
	public static final String AUTHORIZATION = "Authorization";
	public static final String XAMZTARGET = "X-Amz-Target";
	public static final String PUTEVENTS = "Events";
	public static final String AWSPUTEVENT = "AWSEvents.PutEvents";
	public static final String UTF8 = "UTF-8";
	public static final String CONTENTTYPE = "Content-Type";
	public static final String AWSREQID = "x-amzn-requestid";
	public static final String USERAGENT = "user-agent";
	public static final String CONTENTTYPEVALUE = "application/x-amz-json-1.1";
	public static final String HOSTVALUE = "events.us-east-2.amazonaws.com";
	public static final String USERAGENTVALUE = "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0";
	public static final String SHA256 = "SHA-256";

	/*
	 * EXCEPTION CONSTANTS
	 */	
	public static final String FAILEDRESPONESCHEMAERROR = "Failed to build response schema";
	public static final String FAILEDRQUESTSCHEMA = "Failed to build request schema";
	public static final String ERRORHASH = "Unable to compute hash while signing request: ";
	public static final String SIGNERRORMSG = "Unable to calculate a request signature: ";
	public static final String HTTPCALLERROR = "Request failed executing HTTPURLConnection: ";
	public static final String CREATEHTTPCONERROR = "HTTP create connection Error : ";
	public static final String UTFENCODINGERROR = "UTF-8 encoding is not supported.";

	/* SYMBOLS */
	public static final String FWDSLASH = "/";
	public static final String HYPHEN = "-";
	public static final String SPACE = " ";
	public static final String COMMASPACE = ", ";
	public static final String COLON = ":";
	public static final String UNDERSCORE = "_";
	public static final String AWSCREDENTIALSTRING = "Credential=";
	public static final String AWSSIGNEDEADERS = "SignedHeaders=";
	public static final String AWSSIGNATURE = "Signature=";
	public static final String BACKSLASHN = "\n";
	public static final String EMPTY = "";
	public static final Object SEMECOLON = ";";
	public static final String DOUBLEBACKSLASHSPLUS = "\\s+";
	public static final String EVENTCREATIONFAILED = "Event Creation Failed for mentioned Event : ";

	public static final String EVENTCREATIONPASSED = "Event Creation Passed for mentioned Event : ";
	public static final String ERRORINPUTMSGCONVERTION = "Error occured while converting the input request message.";
	public static final String EMPTYCUSTOMREGION = "Custom-Region value is empty.";
	public static final String CREATE = "CREATE";
	public static final String ENTRIES = "Entries";
	public static final String DETAIL = "Detail";

}
