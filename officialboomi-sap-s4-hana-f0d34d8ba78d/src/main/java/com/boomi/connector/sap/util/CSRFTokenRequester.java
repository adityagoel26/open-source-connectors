// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.sap.SAPConnectorConnection;
import com.boomi.util.IOUtil;

/**
 * @author kishore.pulluru
 *
 */
public class CSRFTokenRequester {
	private static final String COULD_NOT_FETCH_A_X_CSRF_TOKEN = "Could not fetch an X-CSRF-TOKEN";

	private static final String X_CSRF_TOKEN_HEADER = "x-csrf-token";
	private static final String ETAG = "etag";
	private static final String SET_COOKIE_HEADER = "set-cookie";
	private static final String COOKIE = "Cookie";
	private static final String IF_MATCH = "If-Match";
	private static final String FETCH = "fetch";
	private static final Header TOKEN_HEADER = new BasicHeader(X_CSRF_TOKEN_HEADER, FETCH);
	private static final Header ACCEPT_HEADER = new BasicHeader(SAPConstants.ACCEPT, SAPConstants.APPLICATION_JSON);
	private static final String DEFAULT_GET_URL = "my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_AddressEmailAddress";

	private final SAPConnectorConnection con;

	CSRFTokenRequester(SAPConnectorConnection con) {
		this.con = con;
	}

	public static CSRFTokenRequester getInstance(SAPConnectorConnection connection) {
		return new CSRFTokenRequester(connection);
	}

	/**
	 * This method will get the csrf and etag headers.
	 * 
	 * @param url
	 * @return csrf headers
	 */
	public Header[] getCsrfTokenHeaders(String... url) {

		CloseableHttpResponse response = null;
		CloseableHttpClient httpclient = null;
		HttpGet httpget = null;
		try {
			if (url.length > 0 && url[0] != null) {
				httpget = new HttpGet(getRequestUri(url[0]));
			} else {
				httpget = new HttpGet(getRequestUri());
			}

			httpget.setHeader(ACCEPT_HEADER);
			httpget.setHeader(TOKEN_HEADER);
			httpget.setHeader(getAuthHeader());

			httpclient = HttpClients.createDefault();
			response = httpclient.execute(httpget);
			validateResponse(response);
			return extractHeader(response);
		} catch (IOException e) {
			throw new ConnectorException(COULD_NOT_FETCH_A_X_CSRF_TOKEN, e);
		} finally {
			IOUtil.closeQuietly(response, httpclient);
		}

	}

	/**
	 * This method will build and return the URI to get the csrf token headers.
	 * 
	 * @param url
	 * @return URI
	 */
	private URI getRequestUri(String url) {
		URI uri = null;
		try {
			uri = new URIBuilder().setScheme(SAPConstants.HTTPS).setHost(url).build();
		} catch (URISyntaxException e) {
			throw new ConnectorException("Exception while building URI : " + e.getMessage());
		}
		return uri;
	}

	/**
	 * This method will build and return the URI to get the csrf token headers with
	 * default URL.
	 * 
	 * @return URI
	 */
	public URI getRequestUri() {
		URI uri = null;
		try {
			uri = new URIBuilder().setScheme(SAPConstants.HTTPS).setHost(DEFAULT_GET_URL).build();
		} catch (URISyntaxException e) {
			throw new ConnectorException("Exception while building URI : " + e.getMessage());
		}
		return uri;
	}

	/**
	 * This method will extract the csrf headers from the API response.
	 * 
	 * @param response
	 * @return Headers
	 */
	private Header[] extractHeader(CloseableHttpResponse response) {
		List<Header> headers = new ArrayList<>();
		Header csrfHeader = response.getLastHeader(X_CSRF_TOKEN_HEADER);
		Header etagHeader = response.getLastHeader(ETAG);
		if (csrfHeader != null) {
			headers.add(csrfHeader);
		}

		if (etagHeader != null) {
			headers.add(new BasicHeader(IF_MATCH, etagHeader.getValue()));
		}

		for (Header cookieHeader : response.getHeaders(SET_COOKIE_HEADER)) {
			headers.add(new BasicHeader(COOKIE, cookieHeader.getValue()));
		}

		return headers.toArray(new Header[headers.size()]);
	}

	/**
	 * This method will build the basic authorization token.
	 * @return AuthHeader
	 */
	private Header getAuthHeader() {
		return new BasicHeader(SAPConstants.AUTHORIZATION, SAPUtils.getBasicAuthToken(con));
	}

	/**
	 * This method will validate the response weather required headers are present or not.
	 * @param response
	 */
	private static void validateResponse(CloseableHttpResponse response) {

		if ((response == null) || (response.getStatusLine() == null)) {
			throw new ConnectorException(COULD_NOT_FETCH_A_X_CSRF_TOKEN);
		}

		int httpCode = response.getStatusLine().getStatusCode();
		if ((httpCode < HttpStatus.SC_OK) || (httpCode > HttpStatus.SC_MULTI_STATUS)) {

			throw new ConnectorException(String.valueOf(httpCode), COULD_NOT_FETCH_A_X_CSRF_TOKEN);
		}
	}
}
