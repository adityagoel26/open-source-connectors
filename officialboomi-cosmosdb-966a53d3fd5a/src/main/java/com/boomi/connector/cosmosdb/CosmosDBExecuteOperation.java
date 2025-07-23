//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.client.cache.HttpCacheContext;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.bean.GetOperationRequest;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.*;
import com.boomi.connector.cosmosdb.util.DocumentUtil;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.StringUtil;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBExecuteOperation extends BaseUpdateOperation {

	/**
	 * Instantiates a new Cosmos DB connector for GET operation.
	 *
	 * @param conn the conn
	 */
	protected CosmosDBExecuteOperation(CosmosDBConnection conn) {
		super(conn);
	}

	/**
	 * This method is used to achieve the GET operation of Cosmos DB connector.
	 */
	@Override
	protected void executeUpdate(UpdateRequest updateRequest, OperationResponse operationResponse) {
		String objectTypeId = this.getContext().getObjectTypeId();
		Logger responseLogger = operationResponse.getLogger();
		ObjectData data = null;
		try {
			data = updateRequest.iterator().next();
			doGet(data, objectTypeId, responseLogger, operationResponse);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(operationResponse, (TrackedData) data, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.util.BaseUpdateOperation#executeUpdate(com.boomi.
	 * connector.api.UpdateRequest, com.boomi.connector.api.OperationResponse)
	 */
	public void doGet(ObjectData data, String objectTypeId, Logger responseLogger,
			OperationResponse operationResponse) {

		GetOperationRequest getOperationRequest = null;
		CacheConfig cacheConfig = CacheConfig.custom()
                .setMaxCacheEntries(3000).setMaxObjectSize(10240) // 10MB will be the maximum object size that can be stored inside Cache.
                .build();
		try (CloseableHttpClient clientConnection = CachingHttpClients.custom()
                .setCacheConfig(cacheConfig)
                .build()) {
			getOperationRequest = DocumentUtil.getRequestData(data);
			HttpCacheContext context = HttpCacheContext.create();
			HttpRequestBase getRequest = getConnection()
					.buildUriRequest(
							DB + getConnection().getDatabaseName() + COLLS
									+ objectTypeId + DOCS + getOperationRequest.getId(),
							HTTP_GET);
			String partitionKey = !StringUtil.isBlank(getOperationRequest.getPartitionKey())
					? getOperationRequest.getPartitionKey()
					: getOperationRequest.getId();
			getRequest.addHeader(PARTITION_KEY_HEADER, PARTITION_KEY_HEADER_START
					+ partitionKey + PARTITION_KEY_HEADER_END);
			try (CloseableHttpResponse response = clientConnection.execute(getRequest,context)) {
				if (response.getStatusLine().getStatusCode() == 200) {
					if (response.getEntity().getContent() != null) {
						ResponseUtil.addSuccess(operationResponse, (TrackedData)data, STATUS_CODE_SUCCESS,
								ResponseUtil.toPayload(response.getEntity().getContent()));
					} else {
						ResponseUtil.addEmptySuccess(operationResponse, data, STATUS_CODE_SUCCESS);
					}
				} else {
					if (response.getStatusLine().getStatusCode() == 404) {
						ResponseUtil.addEmptySuccess(operationResponse, data, "404");
					} else {
					responseLogger.log(Level.SEVERE, response.getStatusLine().getReasonPhrase());
					operationResponse.addResult(data, OperationStatus.APPLICATION_ERROR,
							String.valueOf(response.getStatusLine().getStatusCode()),
							response.getStatusLine().getReasonPhrase(),
							PayloadUtil.toPayload(response.getEntity().getContent()));
					}
				}
			} catch (Exception exception) {
				ResponseUtil.addExceptionFailure(operationResponse, data, exception);
			}
		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(operationResponse, data, e);
		}
	}

	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

}
