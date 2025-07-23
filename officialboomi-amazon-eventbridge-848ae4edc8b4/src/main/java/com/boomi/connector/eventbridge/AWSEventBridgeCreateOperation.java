/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.eventbridge;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.auth.AWSEventBridgeAWS4Signer;
import com.boomi.connector.auth.AWSEventBridgeAWS4SignerImpl;
import com.boomi.connector.awsmodel.AWSPutEventRequest;
import com.boomi.connector.awsmodel.AWSPutEventRequestType;
import com.boomi.connector.model.PutEventRequestType;
import com.boomi.connector.util.AWSEventBridgeConstant;
import com.boomi.connector.util.AWSEventBridgePayloadUtil;
import com.boomi.connector.util.AWSEventBridgeUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Executes the Create Operation from AWS Event Bridge Connector.
 * 
 * @author a.kumar.samantaray
 *
 */
public class AWSEventBridgeCreateOperation extends SizeLimitedUpdateOperation {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(AWSEventBridgeCreateOperation.class.getName());

	/** The Constant mapper. */
	private static final ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

	/**
	 * Instantiates a new AWS event bridge create operation.
	 *
	 * @param conn the conn
	 */
	protected AWSEventBridgeCreateOperation(AWSEventBridgeConnection conn) {
		super(conn);
	}

	/**
	 * It takes the UpdateRequest from the connector, Iterate all the ObjectData
	 * from request and add in trackedData, the executes the executeCreateOperation
	 * method.
	 *
	 * @param request  the request
	 * @param response the response
	 */

	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		LOGGER.info("Inside the ExecuteUpdate Method of AmazonEventBridgeCreateOperation class");
		List<ObjectData> trackedData = new ArrayList<>();
		for (ObjectData oData : request) {
			trackedData.add(oData);
		}
		this.executeCreateOperation(trackedData, response);
	}

	/**
	 * It takes the List<ObjectData> and OperationResponse, Iterate all the
	 * ObjectData and executes the process for each Object Data.It creates the AWS
	 * Signer including the all header elements, execute HttpURLConnection and write
	 * the response using ResponseUtil.
	 *
	 * @param trackedData the tracked data
	 * @param response    the response
	 */
	public void executeCreateOperation(List<ObjectData> trackedData, OperationResponse response) {
		AWSEventBridgeConnection awscon = this.getConnection();
		String objectType = awscon.getOperationContext().getObjectTypeId();
		String sUrl = awscon.getBaseUrl();
		HttpURLConnection connection = null;
		InputStream is = null;
		for (ObjectData objdata : trackedData) {
			is = objdata.getData();
			try {
				URL url = new URL(sUrl);
				List<String> requestList = this.getListofRequest(StreamUtil.toString(is, StandardCharsets.UTF_8), awscon);
				for (String awsFormatJson : requestList) {
					Map<String, String> headers = this.getAwsHeader(awscon, awsFormatJson.replaceAll("\\\\\\\\\\\\\\\"", "\\\\\\\""), url);
					connection = (HttpURLConnection) url.openConnection();
					connection = awscon.createHttpConnection(connection, headers);
					try (JsonParser jp = mapper.getFactory()
							.createParser(awscon.invokeHttpRequest(connection, awsFormatJson.replaceAll("\\\\\\\\\\\\\\\"", "\\\\\\\"")))) {
						final int responseCode = connection.getResponseCode();
						if (responseCode == HttpURLConnection.HTTP_OK) {
							writeToResponseUtil(jp, response, objdata, responseCode);
							response.getLogger().log(Level.INFO, AWSEventBridgeConstant.EVENTCREATIONPASSED,
									objectType);
						} else {
							response.getLogger().log(Level.SEVERE, AWSEventBridgeConstant.EVENTCREATIONFAILED,
									objectType);
							response.addErrorResult(objdata, OperationStatus.APPLICATION_ERROR,
									String.valueOf(responseCode),
									AWSEventBridgeConstant.EVENTCREATIONFAILED + objectType, null);
						}
					}

				}
				response.finishPartialResult(objdata);
			} catch (ConnectorException ex) {
				response.getLogger().log(Level.SEVERE, ex.getMessage());
				LOGGER.severe("ConnectorException catched while processing the request data :" + ex.getMessage());
				ResponseUtil.addExceptionFailures(response, trackedData, ex);
			} catch (Exception ex) {
				response.getLogger().log(Level.SEVERE, ex.getMessage());
				LOGGER.severe("Exception catched while processing the request data :" + ex.getMessage());
				ResponseUtil.addExceptionFailures(response, trackedData, ex);
			} finally {
				IOUtil.closeQuietly(is);
				if (null != connection)
					connection.disconnect();

			}
		}
	}

	/**
	 * It takes the inputData which is received as connector input data, AWS
	 * connection object. It convert the input request format to AWS API request
	 * body format.
	 *
	 * @param inputData the input data
	 * @param awscon    the awscon
	 * @return List<String>
	 * @throws JsonProcessingException 
	 */

	private List<String> getListofRequest(String inputData, AWSEventBridgeConnection awscon) throws JsonProcessingException {
		PutEventRequestType putEvents = null;
		try {
			putEvents = mapper.readValue(inputData, PutEventRequestType.class);
		} catch (JsonParseException e) {
			LOGGER.severe("Failed Parsing Request Data :" + e.getMessage());
		} catch (JsonMappingException e) {
			LOGGER.severe("Failed Mapping Request Data to PutEvent Model" + e.getMessage());
		} catch (IOException e) {
			LOGGER.severe("IOException while reading request Data" + e.getMessage());
		}
		List<AWSPutEventRequest> awsPutEventsList = AWSEventBridgeUtil.convertRequestMessageforPutEvent(putEvents, awscon.getRegion(), awscon.getAccountId());
		List<List<AWSPutEventRequest>> chunkList = getChunkList(awsPutEventsList, 10);
		return getChunkListString(chunkList);

	}

	/**
	 * It takes the chunk list of List object of AWSPutEventRequest and returns the
	 * chunk list.
	 *
	 * @param chunkList the chunk list
	 * @return List<String>
	 */
	private List<String> getChunkListString(List<List<AWSPutEventRequest>> chunkList) {
		List<String> awsfmtStringList = new ArrayList<>();
		for (List<AWSPutEventRequest> list : chunkList) {
			AWSPutEventRequestType awsrequestType = new AWSPutEventRequestType();
			awsrequestType.setRequest(list);
			try {
				awsfmtStringList.add(mapper.writeValueAsString(awsrequestType));
			} catch (JsonProcessingException e) {
				throw new ConnectorException(AWSEventBridgeConstant.ERRORINPUTMSGCONVERTION + e.getMessage(), e);
			}
		}
		return awsfmtStringList;
	}

	/**
	 * It takes the list of generic object type and returns the chunk of list object
	 * according to the input size.
	 *
	 * @param <T>       the generic type
	 * @param list      the list
	 * @param chunkSize the chunk size
	 * @return List<List<T>>
	 */
	public static <T> List<List<T>> getChunkList(List<T> list, int chunkSize) {
		if (chunkSize <= 0) {
			throw new IllegalArgumentException("Invalid chunk size: " + chunkSize);
		}
		List<List<T>> chunkList = new ArrayList<>(list.size() / chunkSize);
		for (int i = 0; i < list.size(); i += chunkSize) {
			chunkList.add(list.subList(i, i + chunkSize >= list.size() ? list.size() : i + chunkSize));
		}
		return chunkList;
	}

	/**
	 * It takes JSON Parser output object and write the output to Response Util.
	 *
	 * @param jp           the jp
	 * @param response     the response
	 * @param objdata      the objdata
	 * @param responseCode the response code
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void writeToResponseUtil(JsonParser jp, OperationResponse response, ObjectData objdata, int responseCode)
			throws IOException {
		while (jp.nextToken() != null) {
			if (jp.getCurrentToken() == JsonToken.FIELD_NAME
					&& jp.getCurrentName().equals(AWSEventBridgeConstant.ENTRIES)) {
				jp.nextToken();
			}
			if (jp.getCurrentToken() == JsonToken.START_ARRAY) {
				while (jp.getCurrentToken() != JsonToken.END_ARRAY) {
					jp.nextToken();
					if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
						ResponseUtil.addPartialSuccess(response, objdata, String.valueOf(responseCode),
								AWSEventBridgePayloadUtil.toPayload(jp));
					}
				}
				break;
			}
		}
	}

	/**
	 * It takes the AWSEventBridgeConnection , input Json String, and URL object, it
	 * creates and adds all the header required for PUT Event and return as Map.
	 *
	 * @param awscon  the awscon
	 * @param awsjson the awsjson
	 * @param url     the url
	 * @return Map<String, String>
	 */
	private Map<String, String> getAwsHeader(AWSEventBridgeConnection awscon, String awsjson, URL url) {

		String objectType = awscon.getOperationContext().getObjectTypeId();
		Map<String, String> headers = AWSEventBridgeUtil.createHeader();
		if (objectType.equalsIgnoreCase(AWSEventBridgeConstant.PUTEVENTS)) {
			headers.put(AWSEventBridgeConstant.XAMZTARGET, AWSEventBridgeConstant.AWSPUTEVENT);
		}
		byte[] contentHash = AWSEventBridgeAWS4Signer.hash(awsjson);
		String contentHashString = AWSEventBridgeUtil.toHex(contentHash);
		AWSEventBridgeAWS4SignerImpl signer = new AWSEventBridgeAWS4SignerImpl(url, AWSEventBridgeConstant.HTTPMETHOD,
				AWSEventBridgeConstant.SERVICE, awscon.getRegion());
		headers.put(AWSEventBridgeConstant.CONTENTSHA256, contentHashString);
		String authorization = signer.computeSignature(headers, contentHashString, awscon.getAwsAccessKey(),
				awscon.getAwsSecretKey());
		headers.put(AWSEventBridgeConstant.AUTHORIZATION, authorization);
		if (awscon.getRegion().equalsIgnoreCase(AWSEventBridgeConstant.CUSTOMEREGIONVALUE)) {
			throw new ConnectorException(AWSEventBridgeConstant.EMPTYCUSTOMREGION);
		}
		return headers;
	}

	/**
	 * It return the connection from AWSEventBridgeConnection.
	 *
	 * @return the connection
	 */
	@Override
	public AWSEventBridgeConnection getConnection() {
		return (AWSEventBridgeConnection) super.getConnection();
	}

}