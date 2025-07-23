// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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
import com.boomi.connector.sap.util.SAPConstants;
import com.boomi.connector.sap.util.SAPUtils;
import com.boomi.connector.util.BaseUpdateOperation;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorExecuteOperation extends BaseUpdateOperation {

	private static final Logger logger = Logger.getLogger(SAPConnectorExecuteOperation.class.getName());
	private static final String SELECT = "select";
	private static final String EXPAND = "expand";
	private static final String URL = "url";
	private static final String INVALID_URL = "Please Provide Url before importing the profile";
	
	protected SAPConnectorExecuteOperation(SAPConnectorConnection conn) {
		super(conn);
	}

	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		logger.log(Level.INFO, "Custom Get Operation Begin");
		response.getLogger().log(Level.INFO, "Custom Get Operation Begin");

		SAPConnectorConnection con = getConnection();
		String objectType = getContext().getObjectTypeId();
	

		String sParentUrl = getContext().getOperationProperties().getProperty(URL);

		if (null == sParentUrl || sParentUrl.trim().isEmpty()) {
			throw new ConnectorException(INVALID_URL);
		}

		String baseUrl = con.getHost() + sParentUrl + objectType;
		ObjectData objectData = request.iterator().next();
		Map<String,String> parameters = null;
		baseUrl = buildBaseurlWithPathparams(baseUrl, objectData, response);
		parameters = buildQueryParams();

		URI uri = null;
		int statusCode = 0;
		InputStream httpResponse = null;
		
		CloseableHttpResponse res = null;
		CloseableHttpClient httpclient = null;
		
		
		try {

			httpclient = HttpClients.createDefault();

			uri = buildURI(baseUrl, parameters);
			logger.log(Level.INFO, "URL : {0}", uri);
			
			HttpGet httpget = new HttpGet(uri);
			httpget.setHeader("Authorization", SAPUtils.getBasicAuthToken(con));
		    httpget.setHeader("Content-Type",SAPConstants.APPLICATION_JSON);
		    httpget.setHeader("Accept",SAPConstants.APPLICATION_JSON);
		    httpget.setHeader("x-csrf-token","fetch");
		    res = httpclient.execute(httpget);
		    statusCode = res.getStatusLine().getStatusCode();
		    httpResponse = res.getEntity().getContent();

			logger.log(Level.INFO, "Status Code : {0}", statusCode);
			response.getLogger().log(Level.INFO, "Response Status : {0}", statusCode);

			if (statusCode == SAPConstants.HTTPSTATUS_OK) {
				ResponseUtil.addSuccess(response, objectData, String.valueOf(statusCode),
						ResponseUtil.toPayload(httpResponse));
			} else {
				ResponseUtil.addApplicationError(response, objectData, String.valueOf(statusCode),
						ResponseUtil.toPayload(httpResponse));
			}

			logger.log(Level.INFO, "Custom Get Operation End");
			response.getLogger().log(Level.INFO, "Custom Get Operation End");

		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, objectData, e);
			logger.log(Level.INFO, "Exception while calling Get API : {0}", e.getMessage());
		} finally {
			try {
				if (null != res) {
					res.close();
				}
			} catch (IOException e) {
				logger.warning(SAPConstants.CONNECTION_LEAK_MSG);
			}
			
			try {
				if (null != httpclient) {
					httpclient.close();
				}
			} catch (IOException e) {
				logger.warning(SAPConstants.CONNECTION_LEAK_MSG);
			}
			if (httpResponse != null) {
				try {
					httpResponse.close();
				} catch (Exception e) {
					logger.log(Level.WARNING, SAPConstants.CONNECTION_LEAK_MSG);
				}
			}

		}

	}

	/**
	 * This method is used to form the URL by parsing request data.
	 * 
	 * @param baseUrl
	 * @param objectData
	 * @return URL
	 */
	public String buildBaseurlWithPathparams(String baseUrl, ObjectData objectData, OperationResponse response) {
		StringBuilder sbUrl = new StringBuilder();
		if (BuildJsonSchema.isPathParamsExists(baseUrl)) {
			StringBuilder params = null;
			List<String> inputPathParamKeys= new ArrayList<>();
			Map<String, String> mPathParams = new HashMap<>();
			inputPathParamKeys.addAll(BuildJsonSchema.getPathParamKeys(baseUrl));
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);
			
			JsonNode rootNode = null;
			//create JsonNode
			try {
				rootNode  = mapper.readTree(objectData.getData());
			} catch (IOException e) {
				response.getLogger().log(Level.SEVERE, SAPConstants.INPUT_JSON_ERROR);
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
			params = BuildJsonSchema.getPathParamUrlFromInput(mPathParams, inputPathParamKeys);
			sbUrl.append(baseUrl.substring(0, baseUrl.indexOf('('))).append("(").append(params).append(")");
			sbUrl.append(baseUrl.substring(baseUrl.indexOf(')') + 1));
		}else {
			sbUrl.append(baseUrl);
		}
		return sbUrl.toString();
	}

	/**
	 * This method is used to build the query parameters to form the URL
	 * 
	 * @param sbUrl
	 * @return URL with query parameters if any
	 */
	private Map<String,String> buildQueryParams() {
		Map<String,String> parameters = new HashMap<>();
		Map<String, Object> mOperationProps = getContext().getOperationProperties();
		for (Map.Entry<String, Object> entry : mOperationProps.entrySet()) {
			if (mOperationProps.get(entry.getKey()) != null
					&& !mOperationProps.get(entry.getKey()).toString().trim().isEmpty()
					&& (entry.getKey().equals(SELECT) || entry.getKey().equals(EXPAND))) {
				String paramValue = (String) entry.getValue();
				parameters.put("$"+entry.getKey(), paramValue);
			}
		}
		return parameters;
	}
	
	/**
	 * @param baseUrl
	 * @param parameters
	 * @return build the URI with baseurl and parameters provided.
	 */
	private URI buildURI(String baseUrl, Map<String, String> parameters) {
		URI uri = null;
		 
		try {
			URIBuilder urlbuilder = new URIBuilder()
			        .setScheme(SAPConstants.HTTPS)
			        .setHost(baseUrl);
			for(Entry<String, String> entry : parameters.entrySet()) {
				urlbuilder.setParameter(entry.getKey(), entry.getValue());
			}
			        
			  uri = urlbuilder.build();
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