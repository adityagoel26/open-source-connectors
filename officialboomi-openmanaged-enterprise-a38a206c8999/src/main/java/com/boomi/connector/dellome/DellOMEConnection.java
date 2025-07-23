// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome;

import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import org.apache.commons.codec.binary.Base64;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.dellome.exception.DellOMEConnectorException;
import com.boomi.connector.dellome.util.DellOMEIPSSLContext;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.IOUtil;
import com.boomi.util.URLUtil;


public class DellOMEConnection extends BaseConnection {
	
	private Logger logger = Logger.getLogger(DellOMEConnection.class.getName());
	private String username;
	private String password;
	private String ipaddress;
	private String baseAlertsApiurl;
	private boolean disableSSL = true;
	private String basicAuth;
	private HttpsURLConnection con = null;
	
	public DellOMEConnection(BrowseContext context) {
		super(context);
		logger.info("DellOMEConnection::DellOMEConnection");

		PropertyMap pm = context.getConnectionProperties();
		this.username = pm.getProperty(USERNAME);
		this.password = pm.getProperty(PASSWORD);
		this.ipaddress = pm.getProperty(IPADDRESS);

		if (null != pm.getBooleanProperty(ENABLESSL)) {
			this.disableSSL = !pm.getBooleanProperty(ENABLESSL);
		}
		String userpass = username + ":" + password;
		basicAuth = "Basic " + new String(Base64.encodeBase64((userpass.getBytes())));

		this.baseAlertsApiurl = "https://" + this.ipaddress + API_ENDPOINT_ALERTS;

		logger.info("DellOMEConnection parameters: " + this.username + ":" + this.baseAlertsApiurl);
	}
	

	/**
	 * Executes a query. Call this when filters are empty
	 *  
	 * @param util instance to process alerts
	 * @return DellOMEResponse response of OME
	 * @throws IOException
	 * @throws DellOMEConnectorException 
	 * 
	 * DELL OME API Alert responses are paged at 100 Alerts. No additional paging needed at response of the results. 
	 */
	public DellOMEResponse doQuery(int count) throws DellOMEConnectorException, IOException {
		logger.info("DellOMEConnection::doQuery - Alerts : "); 

		String urlStr = this.baseAlertsApiurl;
		//DELL OME returns only a maximum of 100 alerts for a single api call.(Size of max response is ~ 150KB) 
		if (count != 0) {
			urlStr = baseAlertsApiurl + "?$skip=" + count + "&$top=100";
		}
		logger.info("DellOMEConnection::doQuery urlStr : " + urlStr);

		this.con = getConnection(METHOD_GET, urlStr);
		logger.info("DellOMEConnection::doQuery con.getResponseCode() : " + this.con.getResponseCode());

		return new DellOMEResponse(this.con);
	}
	

	/**
	 * Executes a query using the given query terms
	 * 
	 * @param queryTerms query filter terms for DELL OME API
	 * @param util instance to process alerts
	 * @return DellOMEResponse response of OME
	 * @throws IOException
	 * @throws DellOMEConnectorException 
	 * 
	 * DELL OME API Alert responses are paged at 100 Alerts. No additional paging needed at response of the results. 
	 */
	public DellOMEResponse doQuery(List<Map.Entry<String, String>> queryTerms, int count) throws IOException, DellOMEConnectorException {

		URL url = buildUrl(queryTerms, this.baseAlertsApiurl);
		String urlStr = url.toString();
		logger.info("DellOMEConnection::doQuery(queryTerms) - " + urlStr);

		//DELL OME returns only a maximum of 100 alerts for a single api call.(Size of max response is ~ 150KB)    
		if (count != 0) {
			urlStr = url.toString() + "&$skip=" + count + "&$top=100";
		}

		return new DellOMEResponse(getConnection(METHOD_GET, urlStr));
	}


	/**
	 * @param method HTTP Method
	 * @param urlStr API Endpoint
	 * @return HttpsURLConnection - connection to the API Endpoint
	 */
	public HttpsURLConnection getConnection(String method, String urlStr) {

		URL url = null;
		try {
			url = new URL(urlStr);
			this.con = (HttpsURLConnection) url.openConnection();
			if (this.disableSSL) {
				this.con.setSSLSocketFactory(DellOMEIPSSLContext.getInstance().getSocketFactory());
				this.con.setHostnameVerifier(DellOMEIPSSLContext.allHostsValid);
			}
			this.con.setDoOutput(true);
			this.con.setRequestMethod(method);
			this.con.setRequestProperty("Authorization", basicAuth);
			this.con.setRequestProperty("Accept", "*/*"); 

		} catch (Exception e) {
			logger.severe("DellOMEConnection::getConnection - "
					+ "Connection to DellOMEConnection could not be established: " + e.getMessage());
		}

		return this.con;
	}
	
	private static URL buildUrl(String... components) throws IOException {
		return buildUrl((List<Map.Entry<String, String>>) null, components);
	}

	private static URL buildUrl(List<Map.Entry<String, String>> params, String... components) throws IOException {
		// generate url with path
		return URLUtil.makeUrl(params, (Object[]) components);
	}

	private static Document parse(InputStream input) throws SAXException, IOException, ParserConfigurationException {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = dbf.newDocumentBuilder();
			return builder.parse(input);
		} finally {
			IOUtil.closeQuietly(input);
		}
	}

	public String getIpaddress() {
		return ipaddress;
	}

	public String getBaseAlertsApiurl() {
		return baseAlertsApiurl;
	}
	
}