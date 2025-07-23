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
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sap.util.Action;
import com.boomi.connector.sap.util.BuildJsonSchema;
import com.boomi.connector.sap.util.CSRFTokenRequester;
import com.boomi.connector.sap.util.SAPConstants;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorDeleteOperation extends BaseUpdateOperation{
	
	private static final Logger logger = Logger.getLogger(SAPConnectorDeleteOperation.class.getName());

	protected SAPConnectorDeleteOperation(SAPConnectorConnection conn) {
		super(conn);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		SAPConnectorConnection connection = getConnection();
		List<ObjectData> lTrackedData = getTrackedData(request);
		
		String sHost = connection.getHost();
		String sParentUrl = getContext().getOperationProperties().getProperty(SAPConstants.URL);
		String sApiUrl = sHost + sParentUrl + getContext().getObjectTypeId();
		
		CSRFTokenRequester csrfTokenReq = CSRFTokenRequester.getInstance(connection);
		
		for(ObjectData objData : lTrackedData) {
				URI uri;
				CloseableHttpClient httpClient = null;
				CloseableHttpResponse httpResponse = null;
				List<String> inputPathParamKeys = new ArrayList<>();
				Map<String, String> mPathParams = new HashMap<>();
				processInputDataForPathParams(sApiUrl,objData.getData(),mPathParams, inputPathParamKeys);
				sApiUrl = buildUrlwithPathParams(sApiUrl,mPathParams, inputPathParamKeys);
				Header[] csrfHeaders = csrfTokenReq.getCsrfTokenHeaders(sApiUrl);
				int statusCode = 0;
				try {
					uri = buildURI(response, sApiUrl);
					HttpDelete deleteRequest = new HttpDelete(uri);
					deleteRequest.setHeader("Accept", "application/json");
					deleteRequest.setHeader("Accept-Language","application/json");
					
					for(Header header : csrfHeaders) {
						deleteRequest.setHeader(header);
					}
					
					
					httpClient = HttpClients.createDefault();
					httpResponse = httpClient.execute(deleteRequest);
					
					statusCode = httpResponse.getStatusLine().getStatusCode();
					
					logger.log(Level.INFO,"Delete Respones StatusCode : {0}",statusCode);
					
					if(statusCode == HttpStatus.SC_NO_CONTENT) {
						ResponseUtil.addSuccess(response, objData, StringUtil.toString(statusCode), null);
					}else {
						ResponseUtil.addApplicationError(response, objData, StringUtil.toString(statusCode),
								ResponseUtil.toPayload(httpResponse.getEntity().getContent()));
					}
					
				}catch(Exception e) {
					logger.log(Level.SEVERE,"Exception while Delete Operation : {0}",e.getMessage());
					ResponseUtil.addExceptionFailure(response, lTrackedData.get(0), e);
				}finally {
					IOUtil.closeQuietly(httpClient,httpResponse);
				}
		}
	}
	
	/**
	 * This method will build the URL with path parameters if exist else it will simply return the URL.
	 * @param sApiUrl
	 * @param mPathParams
	 * @param inputPathParamKeys
	 * @return URL
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
	 * This method will extract and delete the path parameters values from input json and return the input json.
	 * @param sApiUrl
	 * @param inputdata
	 * @param mPathParams
	 * @param inputPathParamKeys 
	 */
	private void processInputDataForPathParams(String sApiUrl, InputStream data, Map<String, String> mPathParams, List<String> inputPathParamKeys) {
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
				}
			}catch (NullPointerException e) {
				throw new ConnectorException(SAPConstants.INVALID_PATH_PARAMS);
			}
			
		}
	}
	
	/**
	 * This method will return the list of input data's from request.
	 * @param request
	 * @return listofObjectData
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
	 * This method will create and return the URI with API URL.
	 * @param response
	 * @param baseUrl
	 * @return URI
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
