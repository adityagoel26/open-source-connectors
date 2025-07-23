/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.auth;

import java.net.URL;
import java.util.Date;
import java.util.Map;

import com.boomi.connector.util.AWSEventBridgeConstant;
import com.boomi.connector.util.AWSEventBridgeUtil;

/**
 * This class helps to create the AWS4 signer demonstrating how to sign requests
 * to Amazon Event Bridge using an 'Authorization' header.
 * 
 * @author a.kumar.samantaray
 */
public class AWSEventBridgeAWS4SignerImpl extends AWSEventBridgeAWS4Signer {

	public AWSEventBridgeAWS4SignerImpl(URL endpointUrl, String httpMethod, String serviceName, String regionName) {
		super(endpointUrl, httpMethod, serviceName, regionName);
	}

	/**
	 * Computes an AWS4 signature for a request as an 'Authorization' header. It
	 * takes all the headers fields including Host,'X-Amz-Date',SHA256 Hashed value
	 * of Body content as 'X-Amz-Content-SHA256', Access key and Secrete key.
	 * 
	 * @param headers
	 * @param queryParameters
	 * @param bodyHash
	 * @param awsAccessKey
	 * @param awsSecretKey
	 * @return The computed authorization string for the request. This value needs
	 *         to be set as the header 'Authorization' on the subsequent HTTP
	 *         request.
	 */
	public String computeSignature(Map<String, String> headers, String bodyHash, String awsAccessKey,
			String awsSecretKey) {
		Date now = new Date();
		String dateTimeStamp = dateTimeFormat.format(now);
		headers.put(AWSEventBridgeConstant.XMZDATE, dateTimeStamp);
		String hostHeader = endpointUrl.getHost();
		StringBuilder hostHeaderFinal = new StringBuilder();
		hostHeaderFinal.append(hostHeader);
		int port = endpointUrl.getPort();
		if (port > -1) {
			hostHeaderFinal.append(AWSEventBridgeConstant.COLON + Integer.toString(port));
		}
		headers.put(AWSEventBridgeConstant.HOST, hostHeaderFinal.toString());
		String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
		String canonicalizedHeaders = getCanonicalizedHeaderString(headers);
		String canonicalRequest = getCanonicalRequest(endpointUrl, httpMethod, canonicalizedHeaderNames,
				canonicalizedHeaders, bodyHash);
		String dateStamp = dateStampFormat.format(now);
		String scope = dateStamp + AWSEventBridgeConstant.FWDSLASH + regionName + AWSEventBridgeConstant.FWDSLASH
				+ serviceName + AWSEventBridgeConstant.FWDSLASH + AWSEventBridgeConstant.TERMINATOR;
		String stringToSign = getStringToSign(AWSEventBridgeConstant.SCHEME, AWSEventBridgeConstant.ALGORITHM,
				dateTimeStamp, scope, canonicalRequest);
		byte[] kSecret = (AWSEventBridgeConstant.SCHEME + awsSecretKey).getBytes();
		byte[] kDate = sign(dateStamp, kSecret, AWSEventBridgeConstant.ALGO);
		byte[] kRegion = sign(regionName, kDate, AWSEventBridgeConstant.ALGO);
		byte[] kService = sign(serviceName, kRegion, AWSEventBridgeConstant.ALGO);
		byte[] kSigning = sign(AWSEventBridgeConstant.TERMINATOR, kService, AWSEventBridgeConstant.ALGO);
		byte[] signature = sign(stringToSign, kSigning, AWSEventBridgeConstant.ALGO);
		String credentialsAuthorizationHeader = AWSEventBridgeConstant.AWSCREDENTIALSTRING + awsAccessKey
				+ AWSEventBridgeConstant.FWDSLASH + scope;
		String signedHeadersAuthorizationHeader = AWSEventBridgeConstant.AWSSIGNEDEADERS + canonicalizedHeaderNames;
		String signatureAuthorizationHeader = AWSEventBridgeConstant.AWSSIGNATURE + AWSEventBridgeUtil.toHex(signature);
		return AWSEventBridgeConstant.SCHEME + AWSEventBridgeConstant.HYPHEN + AWSEventBridgeConstant.ALGORITHM
				+ AWSEventBridgeConstant.SPACE + credentialsAuthorizationHeader + AWSEventBridgeConstant.COMMASPACE
				+ signedHeadersAuthorizationHeader + AWSEventBridgeConstant.COMMASPACE + signatureAuthorizationHeader;

	}
}
