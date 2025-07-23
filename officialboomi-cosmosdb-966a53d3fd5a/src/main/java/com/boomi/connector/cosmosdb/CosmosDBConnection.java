//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.ACCEPT;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.ACTUAL_MS_VERSION;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.APPLICATION_JSON;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.APPLICATION_JSON_QUERY;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.AUTHORIZATION;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.AUTH_TOKEN;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.COLL;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.COLLS;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.CONNECTION_ERROR_MSG;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.DB;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.DB_NAME;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.DEFAULTMAXRETRY;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.DOCS;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.FOR_DATABASE;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.HOST_URL;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.HTTP_DELETE;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.HTTP_GET;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.HTTP_POST;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.HTTP_PUT;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.JSON_PARSING_ERROR_MSG;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.MASTER_KEY;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.PARTITION_KEY_HEADER;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.PARTITION_KEY_HEADER_END;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.PARTITION_KEY_HEADER_START;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.QUERY_MAXRETRY;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.RFC_TIME;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.STATUS_CODE_SUCCESS;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.STATUS_MESSAGE_SUCCESS;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.X_MS_DATE;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.X_MS_VERSION;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.CONTENT_TYPE;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.cosmosdb.bean.CreateOperationRequest;
import com.boomi.connector.cosmosdb.bean.DeleteOperationRequest;
import com.boomi.connector.cosmosdb.bean.ErrorDetails;
import com.boomi.connector.cosmosdb.bean.OutputDocument;
import com.boomi.connector.cosmosdb.bean.UpdateOperationRequest;
import com.boomi.connector.cosmosdb.util.CosmosDBPayloadUtil;
import com.boomi.connector.cosmosdb.util.CosmosDbConstants;
import com.boomi.connector.cosmosdb.util.DocumentUtil;
import com.boomi.connector.cosmosdb.util.JsonSchemaBuilder;
import com.boomi.connector.cosmosdb.util.SignatureUtils;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.exception.CosmosDBRetryException;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBConnection extends BaseConnection {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(CosmosDBConnection.class.getName());

	private final String cosmosdbUrl;
	private final String masterKey;
	private final String databaseName;
	private String operationType;

	@SuppressWarnings({ "rawtypes" })
	private static List<Class> exceptionTermedFailure = initListOfExceptionsTermedFailure();

	public CosmosDBConnection(BrowseContext context) {
		super(context);
		this.cosmosdbUrl = getBaseUrl(context);
		this.masterKey = context.getConnectionProperties().getProperty(MASTER_KEY);
		this.databaseName = context.getConnectionProperties().getProperty(DB_NAME);
	}

	/**
	 * Build Http API Request as per the inputs and Operation type
	 * 
	 * @param urlPath The URL Path
	 * @param httpMethod The Http Method
	 * @return HttpRequestBase
	 * @throws URISyntaxException If any URL Syntax Exception occurs
	 * @throws InvalidKeyException If Invalid Key Exception occurs
	 * @throws NoSuchAlgorithmException If NoSuchAlgorithmException Exception occurs
	 * @throws UnsupportedEncodingException If UnsupportedEncodingException occurs
	 */
	public HttpRequestBase buildUriRequest(String urlPath, String httpMethod)
			throws URISyntaxException, InvalidKeyException, NoSuchAlgorithmException, UnsupportedEncodingException {

		URI uri = new URIBuilder(cosmosdbUrl + urlPath).build();
		RequestConfig config = RequestConfig.DEFAULT;
		Map<String, String> requestHeaders = SignatureUtils.generateHashSignature(cosmosdbUrl + urlPath, masterKey,
				httpMethod);
		switch (httpMethod) {
		case HTTP_GET:
			HttpGet request = new HttpGet(uri);
			request.setConfig(config);
			request.addHeader(AUTHORIZATION, requestHeaders.get(AUTH_TOKEN));
			request.addHeader(ACCEPT, APPLICATION_JSON);
			request.addHeader(X_MS_VERSION, ACTUAL_MS_VERSION);
			request.addHeader(X_MS_DATE, requestHeaders.get(RFC_TIME));
			return request;
		case HTTP_PUT:
			HttpPut putRequest = new HttpPut(uri);
			putRequest.setConfig(config);
			putRequest.addHeader(AUTHORIZATION, requestHeaders.get(AUTH_TOKEN));
			putRequest.addHeader(ACCEPT, APPLICATION_JSON);
			putRequest.addHeader(CONTENT_TYPE, APPLICATION_JSON);
			putRequest.addHeader(X_MS_VERSION, ACTUAL_MS_VERSION);
			putRequest.addHeader(X_MS_DATE, requestHeaders.get(RFC_TIME));
			return putRequest;
		case HTTP_DELETE:
			HttpDelete deleteRequest = new HttpDelete(uri);
			deleteRequest.setConfig(config);
			deleteRequest.addHeader(AUTHORIZATION, requestHeaders.get(AUTH_TOKEN));
			deleteRequest.addHeader(ACCEPT, APPLICATION_JSON);
			deleteRequest.addHeader(X_MS_VERSION, ACTUAL_MS_VERSION);
			deleteRequest.addHeader(X_MS_DATE, requestHeaders.get(RFC_TIME));
			return deleteRequest;
		case HTTP_POST:
			HttpPost createRequest = new HttpPost(uri);
			createRequest.setConfig(config);
			createRequest.addHeader(AUTHORIZATION, requestHeaders.get(AUTH_TOKEN));
			createRequest.addHeader(ACCEPT, APPLICATION_JSON);
			createRequest.addHeader(CONTENT_TYPE, APPLICATION_JSON);
			createRequest.addHeader(X_MS_VERSION, ACTUAL_MS_VERSION);
			createRequest.addHeader(X_MS_DATE, requestHeaders.get(RFC_TIME));
			if (operationType != null && operationType.equals(OperationType.UPSERT.toString())) {
				createRequest.addHeader(CosmosDbConstants.X_MS_DOCUMENT, CosmosDbConstants.TRUE);
			} else {
				createRequest.addHeader(CosmosDbConstants.X_MS_DOCUMENT, CosmosDbConstants.FALSE);
			}

			return createRequest;

		default:
			return null;
		}

	}

	/**
	 * Build Http API Request as per the inputs and Operation type
	 * 
	 * @param urlPath The Url Path
	 * @param httpMethod The Https Method
	 * @return HttpRequestBase
	 * @throws URISyntaxException If any URISyntax Error Occurs.
	 * @throws InvalidKeyException If any InvalidKey Exception Occurs.
	 * @throws NoSuchAlgorithmException If any NoSuchAlgorithm Exception Occurs.
	 * @throws UnsupportedEncodingException If any UnsupportedEncoding Exception occurs.
	 * @throws CosmosDBRetryException If any Recoverable Error Occurs.
	 * @throws CosmosDBConnectorException If any Irrecoverable Error Occurs.
	 */
	public HttpRequestBase buildUriRequestQuery(String urlPath, String httpMethod, String continuationHeader)
			throws URISyntaxException, InvalidKeyException, NoSuchAlgorithmException, UnsupportedEncodingException,
			CosmosDBConnectorException, CosmosDBRetryException {

		URI uri = new URIBuilder(cosmosdbUrl + urlPath).build();
		RequestConfig config = RequestConfig.DEFAULT;
		Map<String, String> requestHeaders = SignatureUtils.generateHashSignature(cosmosdbUrl + urlPath, masterKey,
				httpMethod);
		HttpPost queryRequest = new HttpPost(uri);
		queryRequest.setConfig(config);
		queryRequest.addHeader(AUTHORIZATION, requestHeaders.get(AUTH_TOKEN));
		queryRequest.addHeader(ACCEPT, APPLICATION_JSON);
		queryRequest.addHeader(CONTENT_TYPE, APPLICATION_JSON_QUERY);
		queryRequest.addHeader(X_MS_VERSION, ACTUAL_MS_VERSION);
		queryRequest.addHeader(X_MS_DATE, requestHeaders.get(RFC_TIME));
		queryRequest.addHeader("x-ms-documentdb-isquery", "true");
		queryRequest.addHeader("x-ms-documentdb-query-enablecrosspartition", "true");
		queryRequest.addHeader("x-ms-documentdb-partitionkeyrangeid",getPartitionKeyRange(urlPath));
		queryRequest.addHeader("x-ms-max-item-count", "1000");
		if(StringUtil.isNotBlank(continuationHeader)) {
			queryRequest.addHeader(CosmosDbConstants.X_MS_CONTINUATION,continuationHeader);
		}
		return queryRequest;

	}

	/**
	 * These method is used for getting the Partition Key range which will be
	 * passed as Header in QUERY Operation API
	 * @param urlPath
	 * @return string
	 * @throws CosmosDBConnectorException If any Irrovable Error Occurs.
	 * @throws CosmosDBRetryException If any Recoverable Error Occurs
	 */
	private String getPartitionKeyRange(String urlPath) throws CosmosDBConnectorException, CosmosDBRetryException {
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			try (CloseableHttpResponse response = clientConnection
					.execute(buildUriRequest(urlPath.substring(0, urlPath.length() - 5) + "/pkranges", HTTP_GET))) {
				if (response.getStatusLine().getStatusCode() == 200) {
					return DocumentUtil.getPartitionKeyRange(response.getEntity().getContent());
				} else if (response.getStatusLine().getStatusCode() == 449
						|| response.getStatusLine().getStatusCode() == 503) {
					throw new CosmosDBRetryException(response.getStatusLine().getReasonPhrase(), null,
							response.getStatusLine().getStatusCode());
				} else {
					throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(), null,
							response.getStatusLine().getStatusCode());
				}
			} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
				throw ex;
			} catch (Exception exception) {
				throw new CosmosDBConnectorException(exception.toString(), null, exception);
			}

		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			throw new CosmosDBConnectorException(e.getMessage(), null, e);
		}

	}

	/**
	 * List collections of DB.
	 *
	 * @return the List of String
	 */
	public List<String> listCollectionsOfDB() {
		List<String> collections = null;
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			collections = getAllCollections(clientConnection);
		} catch (Exception e) {
			throw new ConnectorException(e.getMessage());
		}

		return collections;
	}

	/**
	 * It returns list of all the collections available in the selected Database in
	 * Connection.
	 * 
	 * @param clientConnection
	 * @return List<String>
	 * @throws CosmosDBConnectorException
	 */
	private List<String> getAllCollections(CloseableHttpClient clientConnection) throws CosmosDBConnectorException {
		StringBuilder errMsg = null;
		Exception ex = null;
		try (CloseableHttpResponse response = clientConnection
				.execute(buildUriRequest(DB + databaseName + COLL, HTTP_GET));) {
			if (response.getStatusLine().getStatusCode() == 200) {
				return DocumentUtil.getCollectionFromStream(response.getEntity().getContent());
			} else {
				errMsg = new StringBuilder(CONNECTION_ERROR_MSG).append(cosmosdbUrl).append(FOR_DATABASE + databaseName)
						.append(StringUtil.FAILURE_SEPARATOR).append(response.getStatusLine().getReasonPhrase());
				throw new CosmosDBConnectorException(errMsg.toString());
			}
		} catch (Exception exception) {
			ex = exception;
			if (errMsg == null) {
				errMsg = new StringBuilder(CONNECTION_ERROR_MSG).append(cosmosdbUrl).append(FOR_DATABASE + databaseName)
						.append(StringUtil.FAILURE_SEPARATOR).append(exception.toString());
				throw new CosmosDBConnectorException(errMsg.toString());
			} else {
				throw new CosmosDBConnectorException(exception.getMessage());
			}

		} finally {
			if (null != ex) {
				logger.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}
	}

	public Map<String, Object> prepareInputConfig() {
		Map<String, Object> inputConfig = new HashMap<>();
		inputConfig.put(QUERY_MAXRETRY, DEFAULTMAXRETRY);
		return inputConfig;
	}

	/**
	 * Find document by id.
	 *
	 * @param objectTypeId
	 * @param objectId
	 * @param partitionKeyValue
	 * @return the document
	 */
	public String findDocumentById(String objectTypeId, String objectId, String partitionKeyValue) {
		StringBuilder errMsg = null;
		Exception ex = null;
		HttpRequestBase httpRequestBase = null;
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			
			if(objectId != null) {
				httpRequestBase = buildUriRequest(DB + databaseName + COLLS
					+ objectTypeId + DOCS + objectId, HTTP_GET);
				httpRequestBase.addHeader(PARTITION_KEY_HEADER,
					PARTITION_KEY_HEADER_START
							+ (StringUtil.isBlank(partitionKeyValue) ? objectId : partitionKeyValue)
							+ PARTITION_KEY_HEADER_END);
			} else {
				httpRequestBase = buildUriRequestQuery(DB + databaseName + COLLS
						+ objectTypeId+"/docs", HTTP_POST, null);
				HttpEntity entity = new StringEntity(CosmosDbConstants.QUERY_PREFIX+"SELECT top 1 *  FROM root objectType order by objectType._ts desc"+CosmosDbConstants.QUERY_SUFFIX);
				((HttpPost)httpRequestBase).setEntity(entity);
			}
			try (CloseableHttpResponse response = clientConnection.execute(httpRequestBase);) {
				if (response.getStatusLine().getStatusCode() == 200) {
					if(objectId != null) {
						return JsonSchemaBuilder.extractJsonSchema(response.getEntity().getContent(), null);
					} else {
						return JsonSchemaBuilder.extractJsonSchemaForUnstructuredData(response.getEntity().getContent(), null);
					}
				} else {
					errMsg = new StringBuilder(CONNECTION_ERROR_MSG).append(cosmosdbUrl)
							.append(FOR_DATABASE + databaseName).append(StringUtil.FAILURE_SEPARATOR)
							.append(response.getStatusLine().getReasonPhrase());
					throw new ConnectorException(errMsg.toString());
				}
			} catch (Exception exception) {
				ex = exception;
				if (errMsg == null) {
					errMsg = new StringBuilder(CONNECTION_ERROR_MSG).append(cosmosdbUrl)
							.append(FOR_DATABASE + databaseName).append(StringUtil.FAILURE_SEPARATOR)
							.append(ex.getMessage());
					throw new ConnectorException(errMsg.toString());
				} else {
					throw exception;
				}
			} finally {
				if (null != ex) {
					logger.log(Level.SEVERE, ex.getMessage(), ex);
				}
			}
		} catch (Exception e) {
			throw new ConnectorException(e.getMessage());
		}
	}

	/*
	 * Remove white spaces from the start and beginning of the Host URL
	 */
	private String getBaseUrl(BrowseContext context) {
		if (context.getConnectionProperties().getProperty(HOST_URL) != null) {
			String connectionUrl = context.getConnectionProperties().getProperty(HOST_URL).trim();
			if (connectionUrl.substring(connectionUrl.length() - 1).equals("/")) {
				return context.getConnectionProperties().getProperty(HOST_URL).trim();
			} else {
				return context.getConnectionProperties().getProperty(HOST_URL).trim() + "/";
			}
		}
		return null;
	}

	/**
	 * Gets the connection url.
	 *
	 * @return the connection url
	 */
	public String getConnectionUrl() {
		return cosmosdbUrl;
	}

	/**
	 * Gets the master key.
	 *
	 * @return the master key
	 */
	public String getCosmosMasterKey() {
		return masterKey;
	}

	/**
	 * Gets the Database name.
	 *
	 * @return the Database name
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * As per the responses and exceptions while executing CosmosDB Operations, it
	 * sends the results and responses back to Platform.
	 * 
	 * @param shouldRetry
	 * @param response
	 * @param ex
	 * @param objectData
	 * @param errorFlg
	 * @param objectId
	 */
	public void addOperationResponse(boolean shouldRetry, OperationResponse response, Exception ex,
			ObjectData objectData, boolean errorFlg, String objectId, int statusCode, String opearationType) {
		if (errorFlg) {
			if (!shouldRetry) {
				ErrorDetails err = getErrorDetails(ex);
				OperationStatus operationStatus = getOperationStatus(ex);
				response.addResult(objectData, operationStatus, String.valueOf(err.getErrorCode()),
						err.getErrorMessage(),
						CosmosDBPayloadUtil.toPayload(new OutputDocument(operationStatus, objectId, err)));
			}
		} else {
			if (objectId != null) {
				if (opearationType.equals(OperationType.UPDATE.toString())) {
					response.addResult(objectData, OperationStatus.SUCCESS, STATUS_CODE_SUCCESS, STATUS_MESSAGE_SUCCESS,
							CosmosDBPayloadUtil.toPayload(new OutputDocument(OperationStatus.SUCCESS, objectId, null)));

				} else if (opearationType.equals(HTTP_DELETE)) {
					response.addResult(objectData, OperationStatus.SUCCESS, "204", STATUS_MESSAGE_SUCCESS,
							CosmosDBPayloadUtil.toPayload(new OutputDocument(OperationStatus.SUCCESS, objectId, null)));
				}
				else if(opearationType.equals(OperationType.UPSERT.toString()) || opearationType.equals(OperationType.CREATE.toString())) {
					response.addResult(objectData, OperationStatus.SUCCESS, String.valueOf(statusCode),
							STATUS_MESSAGE_SUCCESS,
							CosmosDBPayloadUtil.toPayload(new OutputDocument(OperationStatus.SUCCESS, objectId, null)));
				}
			} else {
				ResponseUtil.addEmptySuccess(response, objectData, "404");
			}
		}

	}

	/**
	 * As per the responses and exceptions while executing CosmosDB Query Operation, it
	 * sends the results and responses back to Platform.
	 * 
	 * @param shouldRetry
	 * @param response
	 * @param ex
	 * @param objectData
	 */
	public void addQueryOperationResponse(boolean shouldRetry, OperationResponse response, Exception ex,
			FilterData objectData) {
		if (!shouldRetry) {
			ErrorDetails err = getErrorDetails(ex);
			OperationStatus operationStatus = getOperationStatus(ex);
			response.addResult(objectData, operationStatus, String.valueOf(err.getErrorCode()), err.getErrorMessage(),
					CosmosDBPayloadUtil.toPayload(new OutputDocument(operationStatus, err)));
		}
	}

	/**
	 * Check if the exception occurred can be termed as APPLICATION_ERROR or FAILURE
	 * 
	 * @param ex
	 * @return OperationStatus
	 */
	public static OperationStatus getOperationStatus(Exception ex) {
		if (exceptionTermedFailure.contains(ex.getClass())
				|| (ex.getCause() != null && exceptionTermedFailure.contains(ex.getCause().getClass()))) {
			return OperationStatus.FAILURE;
		}
		return OperationStatus.APPLICATION_ERROR;

	}

	/**
	 * Populate ErrorDetails object as per the Exception input.
	 * 
	 * @param ex
	 * @return ErrorDetails
	 */
	public ErrorDetails getErrorDetails(Exception ex) {
		if (ex != null) {
			Integer errorCode;
			if (CosmosDBConnectorException.class.equals(ex.getClass())) {
				errorCode = ((CosmosDBConnectorException) ex).getErrorCode() != null
						? ((CosmosDBConnectorException) ex).getErrorCode()
						: 500;
				String errorMessage = CosmosDBConnectorException.class.equals(ex.getClass())
						? ((CosmosDBConnectorException) ex).getMessage()
						: CosmosDbConstants.UNKNOWN_FAILURE;
				return new ErrorDetails(errorCode, errorMessage);
			} else if (CosmosDBRetryException.class.equals(ex.getClass())) {
					errorCode = ((CosmosDBRetryException) ex).getErrorCode() != null
							? ((CosmosDBRetryException) ex).getErrorCode()
							: 500;
					String errorMessage = CosmosDBRetryException.class.equals(ex.getClass())
							? ((CosmosDBRetryException) ex).getMessage()
							: CosmosDbConstants.UNKNOWN_FAILURE;
					return new ErrorDetails(errorCode, errorMessage);
			}
		}
		return new ErrorDetails(500, CosmosDbConstants.UNKNOWN_FAILURE);
	}

	/**
	 * This method fetchs all records as per the Inputs
	 * 
	 * @param request
	 * @param collectionName The Collection Name
	 * @param filterParameters The Filter  Paramters
	 * @param operationResponse The Operation Response
	 * @throws CosmosDBConnectorException If any irrecoverable error occurs.
	 * @throws CosmosDBRetryException If any Recoverable error occurs.
	 */
	public void doQuery(String collectionName, List<Entry<String, String>> filterParameters,
			OperationResponse operationResponse, FilterData filterData)
			throws CosmosDBConnectorException, CosmosDBRetryException {

		UpdateOperationRequest updateRequest = new UpdateOperationRequest();
		String errorMessage = null;
		ObjectMapper mapper = new ObjectMapper();
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			
			boolean isPaginationExists = false;
			String continuationHeader = null;
			do {
				HttpPost queryRequest = (HttpPost) buildUriRequestQuery(
						DB + getDatabaseName() + COLLS + collectionName + "/docs", HTTP_POST, continuationHeader);
				HttpEntity entity = new StringEntity(DocumentUtil.getRequestData(filterParameters));
				queryRequest.setEntity(entity);
				try (CloseableHttpResponse response = clientConnection.execute(queryRequest);) {
					if (response.getStatusLine().getStatusCode() != 200) {
						isPaginationExists = false;
						if (response.getStatusLine().getStatusCode() == 449
								|| response.getStatusLine().getStatusCode() == 503) {
							throw new CosmosDBRetryException(
									response.getStatusLine().getStatusCode() + ":"
											+ response.getStatusLine().getReasonPhrase(),
									null, response.getStatusLine().getStatusCode());
						} else {
							throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(), null,
									response.getStatusLine().getStatusCode());
						}
					} else {
						addResponse(response, operationResponse, filterData);
						continuationHeader = response.getFirstHeader(CosmosDbConstants.X_MS_CONTINUATION) != null ? response.getFirstHeader(CosmosDbConstants.X_MS_CONTINUATION).getValue() : null;
						
						if(StringUtil.isNotBlank(continuationHeader)) {
							isPaginationExists = true;
						} else {
							isPaginationExists = false;
							operationResponse.finishPartialResult(filterData);
							logger.info("Query Successful");
						}
					}
				}
			} while (isPaginationExists);
			
		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			if (e.getClass().equals(JsonParseException.class)) {
				errorMessage = JSON_PARSING_ERROR_MSG;
			} else {
				errorMessage = e.toString();
			}
			throw new CosmosDBConnectorException(errorMessage, updateRequest.getId(), e);
		}

	}

	/**
	 * This method will send the output to response payload.
	 * @param response
	 * @param operationResponse
	 * @param filterData
	 * @throws IOException
	 */
	private void addResponse(CloseableHttpResponse response, OperationResponse operationResponse, FilterData filterData) throws IOException {
		ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
		try (JsonParser jp = mapper.getFactory().createParser(response.getEntity().getContent())) {
			while (jp.nextToken() != null) {
				if (jp.getCurrentToken() == JsonToken.FIELD_NAME
						&& jp.getCurrentName().equals("Documents")) {
					jp.nextToken();
					if (jp.getCurrentToken() == JsonToken.START_ARRAY) {
						while (jp.getCurrentToken() != JsonToken.END_ARRAY) {
							jp.nextToken();
							if (jp.getCurrentToken() == JsonToken.START_OBJECT) {
								ResponseUtil.addPartialSuccess(operationResponse, filterData,
										response.getStatusLine().getReasonPhrase(),
										CosmosDBPayloadUtil.toPayloadQuery(jp));
							} 
						}
						break;
					}
				}

			}
		}
		
	}

	/**
	 * This method updates the input JSON into the Cosmos DB.
	 * 
	 * @param request
	 * @param collectionName
	 * @return UpdateOperationRequest
	 * @throws CosmosDBConnectorException
	 * @throws CosmosDBRetryException
	 */
	public UpdateOperationRequest doUpdate(ObjectData request, String collectionName)
			throws CosmosDBConnectorException, CosmosDBRetryException {
		UpdateOperationRequest updateRequest = null;
		String errorMessage = null;
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			updateRequest = DocumentUtil.getUpdateRequestData(request, getPartitionKey(collectionName));
			HttpPut putRequest = (HttpPut) buildUriRequest(
					DB + getDatabaseName() + COLLS + collectionName + DOCS + updateRequest.getId(), HTTP_PUT);
			String partitionKey = !StringUtil.isBlank(updateRequest.getPartitionKey()) ? updateRequest.getPartitionKey()
					: updateRequest.getId();
			putRequest.addHeader(PARTITION_KEY_HEADER,
					PARTITION_KEY_HEADER_START + partitionKey + PARTITION_KEY_HEADER_END);
			putRequest.setEntity(new InputStreamEntity(request.getData()));
			try (CloseableHttpResponse response = clientConnection.execute(putRequest);) {
				if (response.getStatusLine().getStatusCode() != 200) {
					if (response.getStatusLine().getStatusCode() == 449
							|| response.getStatusLine().getStatusCode() == 503) {
						throw new CosmosDBRetryException(
								response.getStatusLine().getStatusCode() + ":"
										+ response.getStatusLine().getReasonPhrase(),
								updateRequest.getId(), response.getStatusLine().getStatusCode());
					} else {
						throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(),
								updateRequest.getId(), response.getStatusLine().getStatusCode());
					}
				} else {
					logger.info("Update Successful");
					return updateRequest;
				}
			}
		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			if (e.getClass().equals(JsonParseException.class)) {
				errorMessage = JSON_PARSING_ERROR_MSG;
			} else {
				errorMessage = e.toString();
			}
			throw new CosmosDBConnectorException(errorMessage, updateRequest != null ? updateRequest.getId() : null, e);
		}
	}

	/**
	 * This method creates the document in the CosmosDB.
	 * 
	 * @param request
	 * @param collection
	 * @param partitionValue
	 * @return CreateOperationRequest
	 * @throws CosmosDBConnectorException
	 * @throws CosmosDBRetryException
	 */
	public CreateOperationRequest doCreate(ObjectData request, String collection)
			throws CosmosDBConnectorException, CosmosDBRetryException {

		CreateOperationRequest createRequest = null;
		String errorMessage = null;

		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			createRequest = DocumentUtil.getCreateRequestData(request, getPartitionKey(collection));
			HttpPost postRequest = (HttpPost) buildUriRequest(
					DB + getDatabaseName() + COLLS + collection + CosmosDbConstants.CREATE_DOCS, HTTP_POST);
			String partitionKey = !StringUtil.isBlank(createRequest.getPartitionValue())
					? createRequest.getPartitionValue()
					: createRequest.getRequestId();
			postRequest.addHeader(PARTITION_KEY_HEADER,
					PARTITION_KEY_HEADER_START + partitionKey + PARTITION_KEY_HEADER_END);
			postRequest.setEntity(new InputStreamEntity(request.getData()));
			try (CloseableHttpResponse response = clientConnection.execute(postRequest);) {
				if (response.getStatusLine().getStatusCode() != 201) {
					if (response.getStatusLine().getStatusCode() == 449
							|| response.getStatusLine().getStatusCode() == 503) {
						throw new CosmosDBRetryException(
								response.getStatusLine().getStatusCode() + ":"
										+ response.getStatusLine().getReasonPhrase(),
								createRequest.getRequestId(), response.getStatusLine().getStatusCode());
					} else {
						throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(),
								createRequest.getRequestId(), response.getStatusLine().getStatusCode());
					}
				} else {
					logger.log(Level.FINE, "Record created successfully in cosmosDB");
					createRequest.setStatusCode(response.getStatusLine().getStatusCode());
					return createRequest;
				}
			}

		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			if (e.getClass().equals(JsonParseException.class)) {
				errorMessage = JSON_PARSING_ERROR_MSG;
			} else {
				errorMessage = e.toString();
			}
			throw new CosmosDBConnectorException(errorMessage,
					createRequest != null ? createRequest.getRequestId() : null, e);
		}
	}

	/**
	 * This method will create the document in the CosmosDB if Document id doesn't
	 * exists. If exists it will do update.
	 * 
	 * @param request
	 * @param collection
	 * @param partitionValue
	 * @return CreateOperationRequest
	 * @throws CosmosDBConnectorException
	 * @throws CosmosDBRetryException
	 */
	public CreateOperationRequest doUpsert(ObjectData request, String collection)
			throws CosmosDBConnectorException, CosmosDBRetryException {
		CreateOperationRequest createRequest = null;
		String errorMessage = null;
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			createRequest = DocumentUtil.getCreateRequestData(request, getPartitionKey(collection));
			HttpPost postRequest = (HttpPost) buildUriRequest(
					DB + getDatabaseName() + COLLS + collection + CosmosDbConstants.CREATE_DOCS, HTTP_POST);
			String partitionKey = !StringUtil.isBlank(createRequest.getPartitionValue())
					? createRequest.getPartitionValue()
					: createRequest.getRequestId();
			postRequest.addHeader(PARTITION_KEY_HEADER,
					PARTITION_KEY_HEADER_START + partitionKey + PARTITION_KEY_HEADER_END);
			postRequest.setEntity(new InputStreamEntity(request.getData()));
			try (CloseableHttpResponse response = clientConnection.execute(postRequest);) {
				if (response.getStatusLine().getStatusCode() == 200
						|| response.getStatusLine().getStatusCode() == 201) {
					createRequest.setStatusCode(response.getStatusLine().getStatusCode());
					return createRequest;
				} else if (response.getStatusLine().getStatusCode() == 449
						|| response.getStatusLine().getStatusCode() == 503) {
					throw new CosmosDBRetryException(
							response.getStatusLine().getStatusCode() + ":" + response.getStatusLine().getReasonPhrase(),
							createRequest.getRequestId(), response.getStatusLine().getStatusCode());
				} else {
					throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(),
							createRequest.getRequestId(), response.getStatusLine().getStatusCode());
				}

			}
		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			if (e.getClass().equals(JsonParseException.class)) {
				errorMessage = JSON_PARSING_ERROR_MSG;
			} else {
				errorMessage = e.toString();
			}
			throw new CosmosDBConnectorException(errorMessage,
					createRequest != null ? createRequest.getRequestId() : null, e);
		}

	}

	/**
	 * Thid mrthod checks in the Cosmos DB, and return the partition key field name
	 * as per the collection input.
	 * 
	 * @param collectionName
	 * @return String
	 * @throws CosmosDBConnectorException
	 */
	private String getPartitionKey(String collectionName) throws CosmosDBConnectorException, CosmosDBRetryException {
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			try (CloseableHttpResponse response = clientConnection
					.execute(buildUriRequest(DB + getDatabaseName() + COLLS + collectionName, HTTP_GET))) {
				if (response.getStatusLine().getStatusCode() == 200) {
					return DocumentUtil.getPartitionKeyField(response.getEntity().getContent());
				} else if (response.getStatusLine().getStatusCode() == 449
						|| response.getStatusLine().getStatusCode() == 503) {
					throw new CosmosDBRetryException(response.getStatusLine().getReasonPhrase(), null,
							response.getStatusLine().getStatusCode());
				} else {
					throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(), null,
							response.getStatusLine().getStatusCode());
				}
			} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
				throw ex;
			} catch (Exception exception) {
				throw new CosmosDBConnectorException(exception.toString(), null, exception);
			}

		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			throw new CosmosDBConnectorException(e.getMessage(), null, e);
		}
	}

	/**
	 * Returns list of all the exceptions where process gets failed
	 */
	@SuppressWarnings("rawtypes")
	private static List<Class> initListOfExceptionsTermedFailure() {
		List<Class> listOfExceptionsTermedFailure = new ArrayList<>();
		listOfExceptionsTermedFailure.add(UnknownHostException.class);

		return listOfExceptionsTermedFailure;
	}

	/**
	 * To check, if the exception can be termed as process failure.
	 * 
	 * @param ex
	 * @return boolean
	 */
	@SuppressWarnings("rawtypes")
	public boolean isProcessFailed(Exception ex) {
		boolean isBatchFailed = false;
		if (null != ex) {
			for (Class failureEx : exceptionTermedFailure) {
				isBatchFailed = failureEx.isInstance(ex.getCause());
				if (isBatchFailed) {
					break;
				}
			}
		}
		return isBatchFailed;
	}

	/**
	 * This method deletes the input id in the Cosmos DB.
	 * 
	 * @param request
	 * @param collectionName
	 * @param clientConnection
	 * @return UpdateOperationRequest
	 * @throws CosmosDBConnectorException
	 * @throws CosmosDBRetryException
	 */
	public DeleteOperationRequest doDelete(ObjectData request, String collectionName,
			CloseableHttpClient clientConnection) throws CosmosDBConnectorException, CosmosDBRetryException {
		DeleteOperationRequest deleteOperationRequest = null;
		String errorMessage = null;
		try {
			deleteOperationRequest = DocumentUtil.getUpdateRequestData(request);
			HttpDelete deleteRequest = (HttpDelete) buildUriRequest(
					DB + getDatabaseName() + COLLS + collectionName + DOCS + deleteOperationRequest.getId(),
					HTTP_DELETE);
			deleteRequest.addHeader(PARTITION_KEY_HEADER,
					PARTITION_KEY_HEADER_START + deleteOperationRequest.getPartitionKey() + PARTITION_KEY_HEADER_END);
			try (CloseableHttpResponse response = clientConnection.execute(deleteRequest);) {
				if (response.getStatusLine().getStatusCode() != 204) {
					if (response.getStatusLine().getStatusCode() == 449
							|| response.getStatusLine().getStatusCode() == 503) {
						throw new CosmosDBRetryException(
								response.getStatusLine().getStatusCode() + ":"
										+ response.getStatusLine().getReasonPhrase(),
								deleteOperationRequest.getId(), response.getStatusLine().getStatusCode());
					} else {
						if (response.getStatusLine().getStatusCode() == 404) {
							return new DeleteOperationRequest();
						} else {
							throw new CosmosDBConnectorException(response.getStatusLine().getReasonPhrase(),
									deleteOperationRequest.getId(), response.getStatusLine().getStatusCode());
						}
					}
				} else {
					logger.info("Delete Successful");
					return deleteOperationRequest;
				}
			}

		} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
			throw ex;
		} catch (Exception e) {
			if (e.getClass().equals(JsonParseException.class)) {
				errorMessage = JSON_PARSING_ERROR_MSG;
			} else {
				errorMessage = e.toString();
			}
			throw new CosmosDBConnectorException(errorMessage,
					deleteOperationRequest != null ? deleteOperationRequest.getId() : null, e);
		}
	}

	public void setOperationType(String operationType) {
		this.operationType = operationType;
	}
	
	public String getOperationType() {
		return this.operationType;
	}
}