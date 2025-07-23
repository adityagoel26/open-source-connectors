//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static com.boomi.connector.liveoptics.utils.LiveOpticsConstants.CONTENT_TYPE_HEADER;
import static com.boomi.connector.liveoptics.utils.LiveOpticsConstants.GET_API;
import static com.boomi.connector.liveoptics.utils.LiveOpticsConstants.JSON_CONTENT_TYPE;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.liveoptics.utils.LiveOpticsPayloadUtil;
import com.boomi.connector.liveoptics.utils.LiveOpticsResponse;
import com.boomi.connector.util.BaseGetOperation;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * @author Naveen Ganachari
 *
 *         ${tags}
 */
public class LiveOpticsConnectorGetOperation extends BaseGetOperation {

	
	protected LiveOpticsConnectorGetOperation(LiveOpticsConnectorConnection conn) {
		super(conn);
	}

	@Override
	/*
	 * This method is used to achieve the GET operation in the connector. It will
	 * first establish a login session, then the GET operation and then finally
	 * close the session through the API calls.
	 * 
	 * @see
	 * com.boomi.connector.util.BaseGetOperation#executeGet(com.boomi.connector.api.
	 * GetRequest, com.boomi.connector.api.OperationResponse)
	 */
	
	protected void executeGet(GetRequest request, OperationResponse response) {
		ObjectIdData input = request.getObjectId();
		boolean closeCall = false;
		String mSessionToken = null;
		try {
			response.getLogger().log(Level.INFO, "executeGetMethodEntry");
			LiveOpticsResponse lLoginResp = this.getConnection().doLogin();
			response.getLogger().log(Level.INFO, "Login call Response Status & Code: " + lLoginResp.getStatus() + ", "
					+ lLoginResp.getResponseCode());

			if (lLoginResp.getStatus() == OperationStatus.SUCCESS) {
				closeCall = true;
				String lSessionToken = lLoginResp.getSessionToken();
				mSessionToken = this.getConnection().parse(lSessionToken, this.getConnection().getSharedSecret(),
						Base64.decodeBase64(this.getConnection().getLoginSecret()));
				doGet(response, input, mSessionToken);
			} else {
				response.addErrorResult(input, OperationStatus.APPLICATION_ERROR, lLoginResp.getResponseCodeAsString(),
						lLoginResp.getResponseMessage(), new ConnectorException(lLoginResp.getResponseMessage()));
			}
		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, (TrackedData) input, new ConnectorException(e.toString()));
		} finally {
			try {
				if (closeCall) {
					LiveOpticsResponse lCloseRes = getConnection().doClose(mSessionToken);
					response.getLogger().log(Level.INFO, "Close Call Response Status & Code: " + lCloseRes.getStatus()
							+ ", " + lCloseRes.getResponseCode());
				}
			} catch (Exception exception) {
				response.getLogger().log(Level.WARNING, "Error while dolose() method Call");
			}
		}
	}

		
	/**
	 * This method is used to hit the get API URL and generate a response with the
	 * inputs like projectID and includeEntities
	 * 
	 * @param opResponse The operation response
	 * @param pInput The input data
	 * @param mSessionToken The session token
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public void doGet(OperationResponse opResponse, ObjectIdData pInput, String mSessionToken)
			throws URISyntaxException, IOException {
		LiveOpticsResponse getResponse = null;
		boolean noDataFlag = true;
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			URI uri = new URIBuilder(this.getConnection().getBaseUrl() + GET_API)
					.setParameter("projectLookupRequest.id", pInput.getObjectId())
					.setParameter("projectLookupRequest.includeEntities",
							this.getConnection().getIncludeEntities().toString())
					.build();
			RequestConfig config = RequestConfig.DEFAULT;
			HttpGet request = new HttpGet(uri);
			request.setConfig(config);
			request.addHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE);
			request.addHeader("Accept", JSON_CONTENT_TYPE);
			request.addHeader("session", mSessionToken);

			try (CloseableHttpResponse response = clientConnection.execute(request);) {
				getResponse = new LiveOpticsResponse();
				getResponse.setResponseCode(response.getStatusLine().getStatusCode());
				getResponse.setResponseMessage(response.getStatusLine().getReasonPhrase());
				if (getResponse.getStatus() == OperationStatus.SUCCESS) {
					try (JsonParser jp = new JsonFactory().createParser(response.getEntity().getContent());) {
						while (jp.nextToken() != null) {
							String fieldName = jp.getCurrentName();
							if (fieldName != null && fieldName.equals("Data")) {
								JsonToken nextToken = jp.nextToken();
								if (!nextToken.equals(JsonToken.VALUE_NULL)) {
									noDataFlag = false;
									opResponse.addResult((TrackedData) pInput, getResponse.getStatus(),
											getResponse.getResponseCodeAsString(), getResponse.getResponseMessage(),
											LiveOpticsPayloadUtil.toPayload(jp));
								}
							}
						}
						// This scenario will come only when given project id is invalid or not available in the system.
						// Live Optics sending response as null data with 200 OK.
						if (noDataFlag) {
							opResponse.addErrorResult(pInput, OperationStatus.APPLICATION_ERROR,
									getResponse.getResponseCodeAsString(), getResponse.getResponseMessage(),
									new ConnectorException("Invalid input."));
						}
					}
				} else {
					ResponseUtil.addApplicationError(opResponse, pInput, getResponse.getResponseCodeAsString(),
							ResponseUtil.toPayload(response.getEntity().getContent()));
				}
			}
		}
	}

	@Override
	public LiveOpticsConnectorConnection getConnection() {
		return (LiveOpticsConnectorConnection) super.getConnection();
	}
}