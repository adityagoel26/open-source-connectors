// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sap.util.Action;
import com.boomi.connector.sap.util.BuildJsonSchema;
import com.boomi.connector.sap.util.QueryBuilder;
import com.boomi.connector.sap.util.SAPConstants;
import com.boomi.connector.sap.util.SAPPayloadUtil;
import com.boomi.connector.sap.util.SAPUtils;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorQueryOperation extends BaseQueryOperation {

	private static final Logger logger = Logger.getLogger(SAPConnectorQueryOperation.class.getName());
	private static final String URL = "url";
	private static final String PROVIDE_PATH_PARAM_MSG= "Please provide input's for path parameters.";

	protected SAPConnectorQueryOperation(SAPConnectorConnection conn) {
		super(conn);
	}

	@Override
	protected void executeQuery(QueryRequest request, OperationResponse response) {
		logger.log(Level.INFO, "Query Operation Begin");
		response.getLogger().log(Level.INFO, "Query Operation Begin");

		FilterData input = request.getFilter();
		SAPConnectorConnection con = getConnection();
		String objectType = getContext().getObjectTypeId();
		String sParentUrl = getContext().getOperationProperties().getProperty(URL);

		if (null == sParentUrl || sParentUrl.trim().isEmpty()) {
			logger.log(Level.INFO, "url is : {0}", sParentUrl);
			throw new ConnectorException("Please Provide Url before importing the profile");
		}

		String baseUrl = con.getHost() + sParentUrl + objectType;

		request.getFilter().getUserDefinedProperties();

		// converting the sdk filter into url query filter pairs
		QueryBuilder queryBuilder = new QueryBuilder();
		
		Map<String,String> parameters = new HashMap<>();
		
		String queryFilter = null;
		String sortOrder = null;
		queryFilter = queryBuilder.consturctQueryFilter(input.getFilter());
		sortOrder = queryBuilder.consturctSortFilter(input.getFilter());
		parameters = buildQueryParams();

		baseUrl = buildBaseUrlWithPathParams(baseUrl, queryFilter);
		queryFilter = removePathParamsFromFilter(baseUrl, queryFilter);
		
		if(StringUtil.isNotBlank(queryFilter)) {
			parameters.put(SAPConstants.FILTER, queryFilter);
		}
		
		if(StringUtil.isNotBlank(sortOrder)) {
			parameters.put(SAPConstants.ORDERBY, sortOrder);
		}

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
			if (statusCode == SAPConstants.HTTPSTATUS_OK) {
				parseSAPData(httpResponse, response, statusCode, input);
				response.finishPartialResult(input);
			} else {
				ResponseUtil.addApplicationError(response, input, String.valueOf(statusCode),
						ResponseUtil.toPayload(httpResponse));
			}

			logger.log(Level.INFO, "Query Operation End");
			response.getLogger().log(Level.INFO, "Query Operation End");
		} catch (IOException ioException) {
			ResponseUtil.addExceptionFailure(response, input, ioException);
			logger.log(Level.WARNING, "IOException at Query Operation : {0}", ioException.getMessage());
		} catch (Exception exception) {
			ResponseUtil.addExceptionFailure(response, input, exception);
			logger.log(Level.WARNING, "Exception at Query Operation : {0}", exception.getMessage());
		} finally {
			IOUtil.closeQuietly(httpResponse, res, httpclient);
		}
	}

	

	/**
	 * @param baseUrl
	 * @param queryFilter
	 * @return after building the base URL with path parameters provided through
	 *         query filters, this method removes path parameters from the query
	 *         filters.
	 */
	private String removePathParamsFromFilter(String baseUrl, String queryFilter) {
		if (BuildJsonSchema.isPathParamsExists(baseUrl)) {
			try {
				String pathParam = queryFilter.substring(queryFilter.indexOf(Action.PATH_PARAM),
						queryFilter.indexOf(')'));
				queryFilter = queryFilter.replace("(" + pathParam + ")", "");
				if (queryFilter.contains("(and ")) {
					queryFilter = queryFilter.replace("(and ", "(");
				} else {
					queryFilter = "";
				}
			} catch (StringIndexOutOfBoundsException exception) {
				throw new ConnectorException(PROVIDE_PATH_PARAM_MSG);
			}
		}
		return queryFilter;
	}

	/**
	 * @param baseUrl
	 * @param queryFilter
	 * @return build the base URL with path parameters provided through query
	 *         filters.
	 */
	private String buildBaseUrlWithPathParams(String baseUrl, String queryFilter) {
		String pathParam = "";
		if (BuildJsonSchema.isPathParamsExists(baseUrl)) {
			try {
				pathParam = queryFilter.substring(queryFilter.indexOf(Action.PATH_PARAM), queryFilter.indexOf(')'));
			} catch (StringIndexOutOfBoundsException exception) {
				throw new ConnectorException(PROVIDE_PATH_PARAM_MSG);
			}
			String[] params = pathParam.replace(Action.PATH_PARAM, "").replace(" eq ", "=").split(" and ");
			StringBuilder pathParams = new StringBuilder();
			for (String param : params) {
				pathParams.append(param).append(",");
			}
			pathParams.deleteCharAt(pathParams.length() - 1);
			baseUrl = baseUrl.replace(baseUrl.substring(baseUrl.indexOf('(') + 1, baseUrl.indexOf(')')),
					pathParams.toString().trim());
		}
		return baseUrl;
	}
	
	
	/**
	 * @param baseUrl
	 * @param parameters
	 * @return create and return the URI with API URL.
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

	

	/**
	 * This method is used to build the query parameters to form the URL
	 * @param parameters 
	 * 
	 * @return URL (URL appended with the query parameters)
	 */
	private Map<String,String> buildQueryParams() {
		Map<String,String> queryParameters = new HashMap<>();
		Map<String, Object> mOperationProps = getContext().getOperationProperties();
		String selectFieldParam = getSelectedFieldsParam();
		for (Map.Entry<String, Object> entry : mOperationProps.entrySet()) {
			if (!entry.getKey().equals(URL) && entry.getValue() != null && !entry.getValue().toString().trim().isEmpty()
					&& !entry.getValue().equals("--")) {
				queryParameters.put("$"+entry.getKey(), (String) entry.getValue());
			}
		}
		if (!selectFieldParam.isEmpty()) {
			queryParameters.put("$select", getSelectedFieldsParam());
		}
		return queryParameters;
	}

	/**
	 * This method will parse the selected fields from filter and a helper for URL
	 * builder.
	 * 
	 * @return selectFields
	 */
	private String getSelectedFieldsParam() {
		StringBuilder selectFields = new StringBuilder();
		List<String> selectedFieldsList = getContext().getSelectedFields();
		for (String field : selectedFieldsList) {
			selectFields.append(field.substring(field.lastIndexOf('/') + 2, field.length())).append(",");
			// +2 to escape the appended underscore(_).
		}
		if (selectFields.length() > 0) {
			selectFields.deleteCharAt(selectFields.length() - 1);
		}
		return selectFields.toString();
	}

	/**
	 * This method will parse and add the response json objects to the payload.
	 * @param jsonParser
	 * @param response
	 * @param statusCode
	 * @param input
	 * @throws IOException
	 */
	private void parseSAPData(InputStream httpResponse, OperationResponse response, int statusCode, TrackedData input)
			throws IOException {
		
		JsonFactory factory = new JsonFactory();
		try (JsonParser parser = factory.createParser(httpResponse)) {
			final JsonToken token = parser.nextToken();
			if (token != JsonToken.START_OBJECT) {
				throw new IllegalStateException(
						"The first element of the Json structure was expected to be a start Object token, but it was: " + token);
			}
			while (token != JsonToken.END_OBJECT) {
				JsonToken nextToken = parser.nextToken();
				if (nextToken == JsonToken.FIELD_NAME && parser.getText().equals(SAPConstants.RESULTS)) {
					parser.nextToken();
					JsonToken objectToken = parser.nextToken();
					while (objectToken == JsonToken.START_OBJECT) {
						ResponseUtil.addPartialSuccess(response, input, String.valueOf(statusCode),
								SAPPayloadUtil.toPayload(parser));
						parser.skipChildren();
						if (parser.nextToken() == JsonToken.END_ARRAY) {
							break;
						}
					}
					break;
				}
			}
		}
		
	}

	@Override
	public SAPConnectorConnection getConnection() {
		return (SAPConnectorConnection) super.getConnection();
	}
}