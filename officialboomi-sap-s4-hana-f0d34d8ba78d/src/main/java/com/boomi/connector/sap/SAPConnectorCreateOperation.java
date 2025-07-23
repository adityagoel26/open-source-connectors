// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sap.util.Action;
import com.boomi.connector.sap.util.BuildJsonSchema;
import com.boomi.connector.sap.util.CSRFTokenRequester;
import com.boomi.connector.sap.util.SAPConstants;
import com.boomi.connector.sap.util.SizeLimitedUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorCreateOperation extends SizeLimitedUpdateOperation {
	
	private static final Logger logger = Logger.getLogger(SAPConnectorCreateOperation.class.getName());
	private static final Header CONTENT_HEADER = new BasicHeader(SAPConstants.CONTENT_TYPE, SAPConstants.APPLICATION_JSON);
	private static final Header ACCEPT_HEADER = new BasicHeader(SAPConstants.ACCEPT, SAPConstants.APPLICATION_JSON);

	protected SAPConnectorCreateOperation(SAPConnectorConnection conn) {
		super(conn);
	}

	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		SAPConnectorConnection connection = getConnection();
		List<ObjectData> lTrackedData = getTrackedData(request);
		
		String sHost = connection.getHost();
		String sParentUrl = getContext().getOperationProperties().getProperty(SAPConstants.URL);
		String sApiUrl = sHost + sParentUrl + getContext().getObjectTypeId();
		
		CSRFTokenRequester csrfTokenReq = CSRFTokenRequester.getInstance(connection);
		Header[] csrfHeaders = csrfTokenReq.getCsrfTokenHeaders();
		
		for(ObjectData objData : lTrackedData) {
				URI uri;
				CloseableHttpClient httpClient = null;
				CloseableHttpResponse httpResponse = null;
				JsonNode rootNode = null;
				
				List<String> inputPathParamKeys = new ArrayList<>();
				Map<String, String> mPathParams = new HashMap<>();
				rootNode = processInputDataForPathParams(sApiUrl,objData.getData(),mPathParams, inputPathParamKeys);
				sApiUrl = buildUrlwithPathParams(sApiUrl,mPathParams, inputPathParamKeys);
				int statusCode = 0;
				try {
					uri = buildURI(response, sApiUrl);
					HttpPost postRequest = new HttpPost(uri);
					if(rootNode != null) {
						postRequest.setEntity(new StringEntity(rootNode.toString(), ContentType.APPLICATION_JSON));
					}else {
						postRequest.setEntity(new InputStreamEntity(objData.getData(),ContentType.APPLICATION_JSON));
					}
					postRequest.setHeader(CONTENT_HEADER);
					postRequest.setHeader(ACCEPT_HEADER);
					postRequest.setHeader("Accept", "application/json");
					postRequest.setHeader("Accept-Language","application/json");
					
					for(Header header : csrfHeaders) {
						postRequest.setHeader(header);
					}
					
					
					httpClient = HttpClients.createDefault();
					httpResponse = httpClient.execute(postRequest);
					
					statusCode = httpResponse.getStatusLine().getStatusCode();
					
					response.getLogger().log(Level.INFO,"Create Respones StatusCode : {0}",statusCode);
					
					if(statusCode == HttpStatus.SC_CREATED) {
						ResponseUtil.addSuccess(response, objData, StringUtil.toString(statusCode),ResponseUtil.toPayload( httpResponse.getEntity().getContent()));
					}else {
						ResponseUtil.addApplicationError(response, objData, StringUtil.toString(statusCode),
								ResponseUtil.toPayload(httpResponse.getEntity().getContent()));
					}
					
				}catch(Exception e) {
					logger.log(Level.SEVERE,"Exception while Create Operation : {0}",e.getMessage());
					ResponseUtil.addExceptionFailure(response, lTrackedData.get(0), e);
				}finally {
					IOUtil.closeQuietly(httpClient,httpResponse);
				}
		}
	}
	
	/**
	 * @param sApiUrl
	 * @param mPathParams
	 * @param inputPathParamKeys
	 * @return this method will build the URL with path parameters if exist else it will simply return the URL.
	 */
	private String buildUrlwithPathParams(String sApiUrl, Map<String, String> mPathParams, List<String> inputPathParamKeys) {
		StringBuilder sbUrl = new StringBuilder();
		
		if(BuildJsonSchema.isPathParamsExists(sApiUrl)) {
			StringBuilder params = null;
			params = BuildJsonSchema.getPathParamUrlFromInput(mPathParams, inputPathParamKeys);
			sbUrl.append(sApiUrl.substring(0, sApiUrl.indexOf('('))).append("(").append(params).append(")");
			sbUrl.append(sApiUrl.substring(sApiUrl.indexOf(')') + 1));
		}else {
			sbUrl.append(sApiUrl);
		}
		return sbUrl.toString();
	}

	/**
	 * @param sApiUrl
	 * @param inputdata
	 * @param mPathParams
	 * @param inputPathParamKeys
	 * @return this method will extract and delete the path parameters values from input json and return the input json.
	 */
	private JsonNode processInputDataForPathParams(String sApiUrl, InputStream data, Map<String, String> mPathParams, List<String> inputPathParamKeys) {
		JsonNode rootNode = null;
		if(BuildJsonSchema.isPathParamsExists(sApiUrl)) {
			inputPathParamKeys.addAll(BuildJsonSchema.getPathParamKeys(sApiUrl));
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			
			//create JsonNode
			try {
				rootNode = mapper.readTree(data);
			} catch (IOException e) {
				throw new ConnectorException(SAPConstants.INPUT_JSON_ERROR);
			}
			try {
				for(String param: inputPathParamKeys) {
					String value =rootNode.get(Action.PATH_PARAM+param).toString();
					value = value.replace("\"", "");
					mPathParams.put(param, value);
					((ObjectNode) rootNode).remove(Action.PATH_PARAM+param);
				}
			}catch (NullPointerException e) {
				throw new ConnectorException(SAPConstants.INVALID_PATH_PARAMS);
			}
			
		}
		return rootNode;
		
	}

	/**
	 * @param request
	 * @return this method will return the list of input data's from request.
	 */
	public List<ObjectData> getTrackedData(UpdateRequest request){
		List<ObjectData> lTrackedData = new ArrayList<>();
		
		Iterator<ObjectData> itr = request.iterator();
		while(itr.hasNext()) {
			lTrackedData.add(itr.next());
		}
		return lTrackedData;
	}
	
	/**
	 * @param response
	 * @param baseUrl
	 * @return create and return the URI with API URL.
	 */
	private URI buildURI(OperationResponse response, String baseUrl) {
		URI uri = null;
		 
		try {
			URIBuilder urlbuilder = new URIBuilder()
			        .setScheme(SAPConstants.HTTPS)
			        .setHost(baseUrl);
			uri = urlbuilder.build();
			response.getLogger().log(Level.INFO,"URL : {0}", uri + "");
			logger.log(Level.INFO,"URL : {0}", uri);
		} catch (URISyntaxException e) {
			logger.log(Level.SEVERE,"Unable to build URL : {0}", e.getMessage());
		}
		return uri;
	}
	


	@Override
    public SAPConnectorConnection getConnection() {
        return (SAPConnectorConnection) super.getConnection();
    }

}