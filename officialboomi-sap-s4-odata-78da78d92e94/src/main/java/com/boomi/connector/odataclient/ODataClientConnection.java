// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.odataclient;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONException;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

public class ODataClientConnection extends BaseConnection {
	public enum ConnectionProperties {URL, AUTHTYPE, USERNAME, PASSWORD, OAUTHOPTIONS, FETCH_HEADERS, TEST_CONNECTION_SERVICE, SAP_CLIENT_NUMBER};
	
	public enum AuthType {
		NONE,BASIC,OAUTH,CUSTOM
	}  
	
    private AuthType _authenticationType;
    private final String _username;
    private final String _password;
    protected final PropertyMap _connProps;
    private String acceptHeader = "application/json";
    private String xCSRFToken = null;
    private String sessionCookies = null;
    private String eTag = null;

    protected Logger logger = Logger.getLogger(this.getClass().getName());
    
	public ODataClientConnection(BrowseContext context) {
		super(context);
		
		_connProps = context.getConnectionProperties();
        _authenticationType = AuthType.valueOf(_connProps.getProperty(ConnectionProperties.AUTHTYPE.name(), AuthType.NONE.name()));
        _username = _connProps.getProperty(ConnectionProperties.USERNAME.name());
        _password = _connProps.getProperty(ConnectionProperties.PASSWORD.name());
	}
	
    public CloseableHttpResponse doExecute(String path, ObjectData input, String httpMethod, InputStream dataIn) throws IOException, GeneralSecurityException {
    	return this.doExecute(path, input, httpMethod, dataIn, this.getAuthenticationType());
    }

 	public CloseableHttpResponse doExecute(String path, ObjectData input, String httpMethod, InputStream dataIn, AuthType authType)
			throws IOException, GeneralSecurityException {
		
		String url = getBaseUrl() + path;
		String sapClient = getClientNumber();
		if (Long.parseLong(sapClient) >= 0)
		{
			if (url.contains("?"))
				url+="&";
			else 
				url+="?";
			url+="sap-client="+sapClient;
		}

		CloseableHttpResponse httpResponse = null;
		CloseableHttpClient httpClient = null;

		try {
			httpClient = HttpClients.custom().setConnectionManager(ODataClientConnector.GLOBAL_CONNECTION_MANAGER).build();
			HttpRequestBase httpRequest = null;
			switch (httpMethod) {
			case "DELETE":
				httpRequest = new HttpDelete(url);
				break;
			case "GET":
				httpRequest = new HttpGet(url);
				break;
			case "POST":
				httpRequest = new HttpPost(url);
				break;
			case "PATCH":
				httpRequest = new HttpPatch(url);
				break;
			case "PUT":
				httpRequest = new HttpPut(url);
				break;
			default:
				break;
			}
			
			// add request headers
			if(httpRequest!=null) {
				httpRequest.addHeader(ODataConstants.ACCEPT, this.getAcceptHeader());
				String authHeader = this.getAuthHeader(authType);
				dataIn = this.insertCustomHeaders(dataIn);
				if (authHeader != null)
					httpRequest.addHeader(ODataConstants.AUTHORIZATION, authHeader);
				if (ODataConstants.GET.contentEquals(httpMethod)) {
					//TODO only fetch if it is null...
					httpRequest.addHeader(ODataConstants.TOKEN, ODataConstants.FETCH);
				} else {
					//Set the headers from DP for DELETE, POST, PUT, PATCH Requests
					getSecurityHeadersFromDocumentProperties(input.getDynamicProperties());
					setSecurityRequestHeaders(httpRequest, ODataConstants.POST.contentEquals(httpMethod));

					if (!ODataConstants.DELETE.contentEquals(httpMethod)) {
						InputStreamEntity reqEntity = new InputStreamEntity(dataIn);
						reqEntity.setContentType(getRequestContentType());
						switch (httpMethod) {
							case "POST":
								((HttpPost) httpRequest).setEntity(reqEntity);
								break;
							case "PATCH":
								((HttpPatch) httpRequest).setEntity(reqEntity);
								break;
							case "PUT":
								((HttpPut) httpRequest).setEntity(reqEntity);
								break;
							default:
								break;
						}
					}
				}
				httpResponse = httpClient.execute(httpRequest);
				if (ODataConstants.GET.contentEquals(httpMethod))
					this.getSecurityResponseHeaders(httpResponse);
			}
		} finally {
			//httpclient cannot be closed. it throws connection pool shutdown error.
			//Instead httpclient will automatically close along with httpresponse.
			IOUtil.closeQuietly(dataIn);
		}
		return httpResponse;
	}

    private String getAuthHeader(AuthType authenticationType) throws IOException, JSONException, GeneralSecurityException
    {
    	String authHeader = null;
       	if (authenticationType != null)
    	{
           	if (AuthType.BASIC == authenticationType)
        	{
            	String userpass = getUsername() + ":" + getPassword();
            	authHeader = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
        	} else if (AuthType.OAUTH==authenticationType) {
        		OAuth2Context oAuth2Context = _connProps.getOAuth2Context(ConnectionProperties.OAUTHOPTIONS.name());
        		OAuth2Token oAuth2Token = oAuth2Context.getOAuth2Token(false);
        		String accessToken = oAuth2Token.getAccessToken();
        		authHeader = "Bearer " + accessToken;
           	} else if (AuthType.CUSTOM==authenticationType)
           	{
           		return getCustomAuthHeader();
           	}
    	}
       	return authHeader;
	}
    
	/**
	 * Override to implement a Test Connection button on the Connections page. Default behavior is to open an authenticated connection to the base host URL
	*/
    public void testConnection() throws IOException, GeneralSecurityException {
    	
    	CloseableHttpResponse httpResponse = null;
    	try {
    		try(CloseableHttpClient httpClient = HttpClients.createDefault()){
				String servicePath = _connProps.getProperty(ConnectionProperties.TEST_CONNECTION_SERVICE.name(), "");
				String url = this.getBaseUrl() + servicePath;
				String sapClient = getClientNumber();
				if (Long.parseLong(sapClient) >= 0)
					url+="?sap-client="+sapClient;
				HttpGet httpRequest = new HttpGet(url);
				String authHeader = this.getAuthHeader(this.getAuthenticationType());
				if (authHeader!=null)
					httpRequest.addHeader ("Authorization", authHeader);
				httpResponse = httpClient.execute(httpRequest);
				if (200 != httpResponse.getStatusLine().getStatusCode())
					throw new ConnectorException(String.format("Problem connecting to endpoint: %s %s %s", url, httpResponse.getStatusLine().getStatusCode(), httpResponse.getStatusLine().getReasonPhrase()));
			}
    	} finally {
    		IOUtil.closeQuietly(httpResponse);
    	}
    }

	/**
	 * Retrieves the SAP client number from the connection properties.
	 *
	 * @return A String representing the SAP client number. Returns "-1" as default
	 *         if the client number is not found as a String property. If retrieving
	 *         as String fails, attempts to get it as a Long property with -1L as default.
	 *
	 */
	private String getClientNumber() {

		try {
			return _connProps.getProperty(ConnectionProperties.SAP_CLIENT_NUMBER.name(), "-1");
		} catch (RuntimeException e) {
			return String.valueOf(_connProps.getLongProperty(ConnectionProperties.SAP_CLIENT_NUMBER.name(), -1L));
		}
	}
    
	/**
	 * @return the username set by the user in the Connections Page
	*/
    protected String getUsername()
    {
    	return _username;
    }
    
	/**
	 * @return the password set by the user in the Connections Page
	*/
    protected String getPassword()
    {
    	return _password;
    }
    
	/**
	 * Returns the AuthType enumeration to set the default authentication type. Defaults to AuthType.NONE
	 * @return AuthType value NONE | BASIC | OAUTH20 | CUSTOM
	*/
    protected AuthType getAuthenticationType()
    {
    	return this._authenticationType;
    }
    
	/**
	 * Returns the service url/host set by the user in the Connections page.
	 * @return the url for the service
	*/
    protected String getBaseUrl()
    {
    	String baseURL = _connProps.getProperty(ConnectionProperties.URL.name(), "").trim();    		
		if (baseURL.length()==0)
			throw new ConnectorException("A Service URL is required");
    	return baseURL;
    }
    
	/**
	 * Used by operations to set the logger to the Process Log logger
	 * @param logger
	*/
    public void setLogger(Logger logger)
    {
    	this.logger = logger;
    }
    
	/**
	 * Returns the Java logger so methods can can log to the container logs during Import, or the Process Log during process execution.
	 * @return the Logger
	*/
    protected Logger getLogger()
    {
    	return this.logger;
    }

	/**
	 * Returns the Connection Properties set by the user in the connectors Connection page.
	 * @return the connection PropertyMap
	*/
    protected PropertyMap getConnectionProperties()
    {
    	return this._connProps;
    }
    
    //TODO this should come from swagger but fast hack to support stripe request form-encoded
    protected String getRequestContentType()
    {
    	return ODataConstants.APPLICATIONJSON;
    }
        
	/**
	 * Allows the addition of custom headers to the connection. For example for Visa Cybersource, this is used to implement HTTP Signature Authentication
	 * @param data the InputStream for the request body 
	 * @return the header value ie. Bearer xxxxxxxxxxxxxxxxxxxxxxxxxx
	*/
	protected InputStream insertCustomHeaders(InputStream data)
	{
		return data;
	}
	/**
	 * Provide a custom Authorization header for AuthType.CUSTOM implementations.
	 * Useful for proprietary API driven authentication. 
	 * @return the header value ie. Bearer xxxxxxxxxxxxxxxxxxxxxxxxxx
	*/
    protected String getCustomAuthHeader() throws JSONException, IOException, GeneralSecurityException
    {
    	return null;
    }

	public String getAcceptHeader() {
		return acceptHeader;
	}

	public void setAcceptHeader(String acceptHeader) {
		this.acceptHeader = acceptHeader;
	}

	public String getxCSRFToken() {
		return xCSRFToken;
	}

	public void setxCSRFToken(String xCSRFToken) {
		this.xCSRFToken = xCSRFToken;
	}

	public String getSessionCookies() {
		return sessionCookies;
	}

	public void setSessionCookies(String sessionCookies) {
		this.sessionCookies = sessionCookies;
	}

	public String geteTag() {
		return eTag;
	}

	public void seteTag(String eTag) {
		this.eTag = eTag;
	}    
	
	//TODO the etag value might be in the response...meaning we have to get it when converting the payload from OData to JSON
	/**
	 * Called after a GET request to grab security headers
	 * @param httpResponse
	 */
	private void getSecurityResponseHeaders(CloseableHttpResponse httpResponse)
	{	
    	Header[] xCSRFTokens = httpResponse.getHeaders(ODataConstants.TOKEN);
    	if (xCSRFTokens.length>0)
    		this.xCSRFToken = xCSRFTokens[0].getValue();
		
    	Header[] etags = httpResponse.getHeaders(ODataConstants.Etag);
    	if (etags.length>0)
    		this.eTag = etags[0].getValue();
    	else
    		this.eTag=null; //It is a good decision to clear any existing etag to force it to be scanned from the response payload
    	
    	Header[] cookies = httpResponse.getHeaders(ODataConstants.SETCOOKIE);
		String cookieString="";
    	for (Header cookie : cookies)
    	{
    		cookieString+=cookie.getValue()+";";
    	}
    	if (cookieString.contains("SAP_SESSIONID"))
    		this.sessionCookies = cookieString;    	
	}
	
	/**
	 * Write non-null etag, cookie, xcsrf token to outbound document properties
	 * TODO, when called from a Query Operation, etag in the header would make no sense when multiple records are in the response body
	 * @param context
	 * @return
	 */
	protected PayloadMetadata setSecurityHeadersToDocumentProperties(OperationContext context)
	{
    	PayloadMetadata payloadMetadata = context.createMetadata();    
    	//Get the headers from the GET, QUERY response
    	if (StringUtil.isNotBlank(this.getxCSRFToken()))
    	{
    		payloadMetadata.setTrackedProperty(ODataConstants.XCSRF, xCSRFToken);
    	}

    	if (StringUtil.isNotBlank(this.geteTag()))
    	{
    		payloadMetadata.setTrackedProperty(ODataConstants.ETAG, eTag);
       	}
    	
    	if (StringUtil.isNotBlank(this.getSessionCookies()))
    	{
    		payloadMetadata.setTrackedProperty(ODataConstants.SAP_SESSIONID_COOKIE, sessionCookies);
       	}
    	return payloadMetadata;
	}
		
	/**
	 * Read etag, cookie, xcsrf from inboud document properties
	 * @param dynamicProperties
	 */
	void getSecurityHeadersFromDocumentProperties(Map<String, String> dynamicProperties)
	{
		String tmpXCSRFToken = dynamicProperties.get(ODataConstants.XCSRF);
		if (StringUtil.isNotBlank(tmpXCSRFToken))
		{
			this.xCSRFToken = tmpXCSRFToken;
		}
		
		String tmpETag = dynamicProperties.get(ODataConstants.ETAG);
		if (StringUtil.isNotBlank(tmpETag))
		{
			this.eTag = tmpETag;
		}
		
		String tmpSessionCookies = dynamicProperties.get(ODataConstants.SAP_SESSIONID_COOKIE);
		if (StringUtil.isNotBlank(tmpSessionCookies)) 
		{
			this.sessionCookies = tmpSessionCookies;
		}
	}
	
	void setSecurityRequestHeaders(HttpRequestBase httpRequest, boolean isPost)
	{
        if(!StringUtil.isEmpty(this.getxCSRFToken()))
        	httpRequest.addHeader(ODataConstants.TOKEN, this.getxCSRFToken());
        else
            throw new ConnectorException("The 'X-CSRF Token' Document Property must be set. This value is returned in the Document Property returned by GET operations.");

        if (!isPost)
        {
    	    if(!StringUtil.isEmpty(this.geteTag())) 
    	    	httpRequest.addHeader(ODataConstants.MATCH, this.geteTag());
    	    else
    	    	throw new ConnectorException("The 'ETag/Document Version Number' Document Property must be set for Edit operations. This value is returned in the Document Property returned by GET operations.");
        }
        
	    if (!StringUtil.isEmpty(this.getSessionCookies()))
	    {
	    	httpRequest.addHeader(ODataConstants.COOKIE, this.getSessionCookies());
	    }
	}
}