/*
*  Copyright (c) 2020 Boomi, Inc.
*/

package com.boomi.connector.eventbridge;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Map;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.util.AWSEventBridgeConstant;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.StringUtil;

/**
 * AWSEventBridgeConnection class helps get/set the user provided values from
 * AWSEventBridge Connector Connection.
 * 
 * @author a.kumar.samantaray
 *
 */
public class AWSEventBridgeConnection extends BaseConnection {

	private String awsAccessKey;
	private String awsSecretKey;
	private String customRegion;
	private String region;
	private String accountId;

	public String getAwsAccessKey() {
		return awsAccessKey;
	}

	public String getAwsSecretKey() {
		return awsSecretKey;
	}

	public String getCustomRegion() {
		return customRegion;
	}

	public String getRegion() {
		return region;
	}

	/**
	 * Constructor which set the credential and AWS properties fields from the
	 * Browser Context.
	 * 
	 * @param context
	 * 
	 */
	public AWSEventBridgeConnection(BrowseContext context) {
		super(context);
		PropertyMap connProps = this.getContext().getConnectionProperties();
		awsAccessKey = connProps.getProperty(AWSEventBridgeConstant.ACCESSKEY);
		awsSecretKey = connProps.getProperty(AWSEventBridgeConstant.SECRETEKEY);
		region = connProps.getProperty(AWSEventBridgeConstant.REGION);
		customRegion = connProps.getProperty(AWSEventBridgeConstant.CUSTOMEREGION);
		region = !StringUtil.isEmpty(customRegion)
				&& StringUtil.equalsIgnoreCase(region, AWSEventBridgeConstant.CUSTOMEREGIONVALUE) ? customRegion
						: region;
		accountId = connProps.getProperty(AWSEventBridgeConstant.ACOUNTID);
	}

	/**
	 * It returns the formatted URL
	 * 
	 * @return {@link String}
	 * 
	 */
	public String getBaseUrl() {
		return String.format(AWSEventBridgeConstant.BASEURL, AWSEventBridgeConstant.SERVICE, region);
	}

	public String getAccountId() {
		return accountId;
	}

	/**
	 * This method takes the URL, http method and Mapped collection of headers as
	 * parameter, it returns the HttpURLConnection object.
	 * 
	 * @param connection
	 * @param httpMethod
	 * @param headers
	 * @return HttpURLConnection
	 */
	public HttpURLConnection createHttpConnection(HttpURLConnection connection, Map<String, String> headers) {

		try {
			connection.setRequestMethod(AWSEventBridgeConstant.HTTPMETHOD);
			if (headers != null) {
				for (Map.Entry<String, String> entry : headers.entrySet()) {
					connection.setRequestProperty(entry.getKey(), entry.getValue());
				}
			}
			connection.setUseCaches(false);
			connection.setDoInput(true);
			connection.setDoOutput(true);
			return connection;
		} catch (Exception e) {
			throw new ConnectorException(AWSEventBridgeConstant.CREATEHTTPCONERROR + e.getMessage(), e);
		}
	}

	/**
	 * This method takes the connection object and request body and returns the
	 * result input stream.
	 * 
	 * @param connection
	 * @param requestBody
	 * @return InputStream
	 */
	public InputStream invokeHttpRequest(HttpURLConnection connection, String body) {
		if (body != null) {
			try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
				wr.writeBytes(body);
				wr.flush();
				return connection.getInputStream();
			} catch (Exception e) {
				throw new ConnectorException(AWSEventBridgeConstant.HTTPCALLERROR + e.getMessage(), e);
			}
		}
		return null;
	}

}