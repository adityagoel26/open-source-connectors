/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.auth;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.util.AWSEventBridgeConstant;
import com.boomi.connector.util.AWSEventBridgeUtil;

/**
 * The AWSEventBridgeAWS4Signer abstract class is a helper class to create AWSV4
 * signer which includes creating CanonicalRequest, StringToSign, Signing Key
 * 
 * @author a.kumar.samantaray
 */
public abstract class AWSEventBridgeAWS4Signer {
	protected URL endpointUrl;
	protected String httpMethod;
	protected String serviceName;
	protected String regionName;

	protected final SimpleDateFormat dateTimeFormat;
	protected final SimpleDateFormat dateStampFormat;

	/**
	 * Create a new AWS AWSV4 signer.
	 * 
	 * @param endpointUrl The service endpoint, including the path to any resource.
	 * @param httpMethod  The HTTP verb for the request, e.g. POST.
	 * @param serviceName The signing name of the service, e.g. 'events'.
	 * @param regionName  The system name of the AWS region associated with the
	 *                    endpoint, e.g. us-east-2
	 */
	public AWSEventBridgeAWS4Signer(URL endpointUrl, String httpMethod, String serviceName, String regionName) {
		this.endpointUrl = endpointUrl;
		this.httpMethod = httpMethod;
		this.serviceName = serviceName;
		this.regionName = regionName;

		dateTimeFormat = new SimpleDateFormat(AWSEventBridgeConstant.ISO8601BASICFORMAT);
		dateTimeFormat.setTimeZone(new SimpleTimeZone(0, AWSEventBridgeConstant.UTC));
		dateStampFormat = new SimpleDateFormat(AWSEventBridgeConstant.DATESTRINGFORMAT);
		dateStampFormat.setTimeZone(new SimpleTimeZone(0, AWSEventBridgeConstant.UTC));
	}

	/**
	 * Returns the canonical collection of header names that will be included in the
	 * signature. For AWS4, all header names must be included in the process in
	 * sorted canonicalized order.
	 * 
	 * @param headers
	 * @return Canonicalize Header Names String
	 */
	protected static String getCanonicalizeHeaderNames(Map<String, String> headers) {
		List<String> sortedHeaders = new ArrayList<>();
		sortedHeaders.addAll(headers.keySet());
		Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);

		StringBuilder buffer = new StringBuilder();
		for (String header : sortedHeaders) {
			if (buffer.length() > 0)
				buffer.append(AWSEventBridgeConstant.SEMECOLON);
			buffer.append(header.toLowerCase());
		}

		return buffer.toString();
	}

	/**
	 * Computes the canonical headers with values for the request. For AWS4, all
	 * headers must be included in the signing process.
	 * 
	 * @param headers
	 * @return Canonicalized Header String
	 * 
	 */
	protected static String getCanonicalizedHeaderString(Map<String, String> headers) {
		if (headers == null || headers.isEmpty()) {
			return AWSEventBridgeConstant.EMPTY;
		}

		List<String> sortedHeaders = new ArrayList<>();
		sortedHeaders.addAll(headers.keySet());
		Collections.sort(sortedHeaders, String.CASE_INSENSITIVE_ORDER);
		StringBuilder buffer = new StringBuilder();
		for (String key : sortedHeaders) {
			buffer.append(key.toLowerCase().replaceAll(AWSEventBridgeConstant.DOUBLEBACKSLASHSPLUS,
					AWSEventBridgeConstant.SPACE) + AWSEventBridgeConstant.COLON
					+ headers.get(key).replaceAll(AWSEventBridgeConstant.DOUBLEBACKSLASHSPLUS,
							AWSEventBridgeConstant.SPACE));
			buffer.append(AWSEventBridgeConstant.BACKSLASHN);
		}

		return buffer.toString();
	}

	/**
	 * Returns the canonical request string to go into the signer process; this
	 * consists of several canonical sub-parts.
	 * 
	 * @param endpoint
	 * @param httpMethod
	 * @param queryParameters
	 * @param canonicalizedHeaderNames
	 * @param canonicalizedHeaders
	 * @param bodyHash
	 * @return Canonical Request String
	 */
	protected static String getCanonicalRequest(URL endpoint, String httpMethod, String canonicalizedHeaderNames,
			String canonicalizedHeaders, String bodyHash) {
		return httpMethod + AWSEventBridgeConstant.BACKSLASHN + getCanonicalizedResourcePath(endpoint)
				+ AWSEventBridgeConstant.BACKSLASHN + AWSEventBridgeConstant.BACKSLASHN + canonicalizedHeaders
				+ AWSEventBridgeConstant.BACKSLASHN + canonicalizedHeaderNames + AWSEventBridgeConstant.BACKSLASHN
				+ bodyHash;

	}

	/**
	 * Returns the canonicalized resource path for the service endpoint.
	 * 
	 * @param endpoint
	 * @return Canonicalized Resource Path String
	 */
	private static String getCanonicalizedResourcePath(URL endpoint) {
		if (endpoint == null) {
			return AWSEventBridgeConstant.FWDSLASH;
		}
		String path = endpoint.getPath();
		if (path == null || path.isEmpty()) {
			return AWSEventBridgeConstant.FWDSLASH;
		}

		String encodedPath = AWSEventBridgeUtil.urlEncode(path, true);
		if (encodedPath.startsWith(AWSEventBridgeConstant.FWDSLASH)) {
			return encodedPath;
		} else {
			return AWSEventBridgeConstant.FWDSLASH.concat(encodedPath);
		}
	}

	
	/**
	 * Creates the String to Sign using the passed parameters.
	 * 
	 * @param scheme
	 * @param algorithm
	 * @param dateTime
	 * @param scope
	 * @param canonicalRequest
	 * @return String to SignIn String
	 * 
	 */
	protected static String getStringToSign(String scheme, String algorithm, String dateTime, String scope,
			String canonicalRequest) {
		return scheme + AWSEventBridgeConstant.HYPHEN + algorithm + AWSEventBridgeConstant.BACKSLASHN + dateTime
				+ AWSEventBridgeConstant.BACKSLASHN + scope + AWSEventBridgeConstant.BACKSLASHN
				+ AWSEventBridgeUtil.toHex(hash(canonicalRequest));

	}

	/**
	 * Hashes the string contents (assumed to be UTF-8) using the SHA-256 algorithm.
	 * 
	 * @param text
	 * @return Hashed byte array
	 * 
	 */
	public static byte[] hash(String text) {
		try {
			MessageDigest md = MessageDigest.getInstance(AWSEventBridgeConstant.SHA256);
			md.update(text.getBytes(StandardCharsets.UTF_8));
			return md.digest();
		} catch (Exception e) {
			throw new ConnectorException(AWSEventBridgeConstant.ERRORHASH + e.getMessage(), e);
		}
	}

	/**
	 * Creates the signed byte array using the input data, key and algorithm
	 * 
	 * @param stringData
	 * @param key
	 * @param algorithm
	 * @return Hashed byte array
	 */
	protected static byte[] sign(String stringData, byte[] key, String algorithm) {
		try {
			byte[] data = stringData.getBytes(StandardCharsets.UTF_8);
			Mac mac = Mac.getInstance(algorithm);
			mac.init(new SecretKeySpec(key, algorithm));
			return mac.doFinal(data);
		} catch (Exception e) {
			throw new ConnectorException(AWSEventBridgeConstant.SIGNERRORMSG + e.getMessage(), e);
		}
	}
}
