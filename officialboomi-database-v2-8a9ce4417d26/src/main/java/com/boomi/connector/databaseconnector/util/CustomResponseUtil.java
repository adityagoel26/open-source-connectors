// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.databaseconnector.model.BatchResponse;
import com.boomi.connector.databaseconnector.model.ErrorDetails;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This Class will generate the Custom Payload Based on the Functionality and
 * adds it to the Response for each ObjectData
 *
 * @author swastik.vn
 */
public class CustomResponseUtil {

	/**
	 * Instantiates a new custom response util.
	 */
	private CustomResponseUtil() {

	}

	/**
	 * Util method for writing the error responses based on the exception thrown.
	 *
	 * @param e        the e
	 * @param objdata  the objdata
	 * @param response the response
	 */
	public static void writeErrorResponse(Exception e, ObjectData objdata, OperationResponse response) {
		if (e.getMessage() == null || e.getMessage().isEmpty()) {
			try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(405, e.toString()))) {
				response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
						DatabaseConnectorConstants.DUPLICATE_PRIMARY_KEY, e.toString(), payload);
			} catch (IOException ioE) {
				throw new ConnectorException(ioE.toString());
			}

		} else {
			try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(405, e.getMessage()))) {
				response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
						DatabaseConnectorConstants.DUPLICATE_PRIMARY_KEY, e.getMessage(), payload);
			} catch (Exception ge) {
				throw new ConnectorException(ge.toString());
			}

		}

	}
	
	public static void writeInvalidInputResponse(IllegalArgumentException e, ObjectData objdata,
			OperationResponse response) {
		try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(405, e.toString() + " - " + DatabaseConnectorConstants.INPUT_ERROR ))) {
			response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
					DatabaseConnectorConstants.DUPLICATE_PRIMARY_KEY, e.toString() + " - " + DatabaseConnectorConstants.INPUT_ERROR, payload);
		} catch (IOException ioE) {
			throw new ConnectorException(ioE.toString());
		}
		
	}

	/**
	 * Method to write the SqlErrorResponse.
	 *
	 * @param e        the e
	 * @param objdata  the objdata
	 * @param response the response
	 */
	public static void writeSqlErrorResponse(SQLException e, ObjectData objdata, OperationResponse response) {
		String errorMessage = e.getMessage().replace("'", "");
		try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(e.getErrorCode(), errorMessage))) {
			response.addResult(objdata, OperationStatus.APPLICATION_ERROR, String.valueOf(e.getErrorCode()),
					e.getMessage(), payload);
		} catch (Exception ge) {
			throw new ConnectorException(ge.toString());
		}

	}

	/**
	 * Batch execute error. This Method will add the Application error result for
	 * exception occurred while executing the Jdbc Batch
	 *
	 * @param objectData the object data
	 * @param response   the response
	 * @param batchnum   the batchnum
	 * @param b          the b
	 */
	public static void batchExecuteError(ObjectData objectData, OperationResponse response, int batchnum, int b) {
		try (Payload payload = JsonPayloadUtil.toPayload(new BatchResponse("Batch Failed to execute", batchnum, b))) {
			response.addResult(objectData, OperationStatus.APPLICATION_ERROR, "400", "Bad request", payload);
		} catch (IOException e) {
			throw new ConnectorException(e.toString());
		}

	}

	/**
	 * Batch execute error. This Method will add the Application error result for
	 * exception occurred while executing the Jdbc Batch
	 *
	 *  @param batchUpdateException batch Update Exception
	 * @param objectData the object data
	 * @param response   the response
	 * @param batchNum   the batch number
	 * @param batchCount the batch count
	 */
	public static void batchExecuteError(BatchUpdateException batchUpdateException,
										 ObjectData objectData, OperationResponse response,
										 int batchNum, int batchCount) {
		try (Payload payload =
					 JsonPayloadUtil.toPayload(new BatchResponse("Batch Failed to execute", batchNum, batchCount))) {
			response.addResult(objectData, OperationStatus.APPLICATION_ERROR,
					String.valueOf(batchUpdateException.getErrorCode()), batchUpdateException.getMessage(), payload);
		} catch (Exception exception) {
			throw new ConnectorException(DatabaseConnectorConstants.FAILED_BATCH_NUM + batchNum, exception);
		}

	}

	/**
	 * Log failed batch with batch number and records in the batch.
	 *
	 * @param response the response
	 * @param batchnum the batchnum
	 * @param b        the b
	 */
	public static void logFailedBatch(OperationResponse response, int batchnum, int b) {
		response.getLogger().log(Level.SEVERE, DatabaseConnectorConstants.FAILED_BATCH_NUM + batchnum);
		response.getLogger().log(Level.SEVERE, DatabaseConnectorConstants.FAILED_BATCH_RECORDS + b);

	}

	/**
	 * Creates {@link PayloadMetadata} from properties.
	 * @param operationResponse
	 * @param properties
	 * @return
	 */
	public static PayloadMetadata createMetadata(OperationResponse operationResponse, Map<String, String> properties) {
		PayloadMetadata metadata = operationResponse.createMetadata();
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			metadata.setTrackedProperty(entry.getKey(), entry.getValue());
		}
		return metadata;
	}

	/**
	 * Adds the result to response
	 * @param data
	 * @param response
	 * @param payloadMetadata
	 * @param input
	 * @throws IOException
	 */
	public static void handleSuccess(ObjectData data, OperationResponse response, PayloadMetadata payloadMetadata,
			Object input) throws IOException {
		Payload payload = null;
		try {
			payload = createPayloadWithMetadata(input, payloadMetadata);
			response.addResult(data, OperationStatus.SUCCESS, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
					DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, payload);
		} finally {
			IOUtil.closeQuietly(payload);
		}
	}

	/**
	 * Creates a Payload with given object and metadata.
	 * @param object
	 * @param payloadMetadata
	 * @return
	 * @throws JsonProcessingException
	 */
	public static Payload createPayloadWithMetadata(Object object, PayloadMetadata payloadMetadata)
            throws JsonProcessingException {
		if (payloadMetadata == null) {
			return JsonPayloadUtil.toPayload(object);
		} else {
			return PayloadUtil.toPayload(DBv2JsonUtil.getObjectMapper().writeValueAsString(object),
					payloadMetadata);
		}
	}

	/**
	 * Generate Transaction Property
	 * @param transactionCacheKey
	 * @return
	 */
	public static Map<String, String> getInProgressTransactionProperties(TransactionCacheKey transactionCacheKey) {
		Map<String, String> properties = new HashMap<>();
		properties.put(TransactionConstants.TRANSACTION_ID, transactionCacheKey.toString());
		properties.put(TransactionConstants.TRANSACTION_STATUS, TransactionConstants.TRANSACTION_IN_PROGRESS);
		return properties;
	}

	/**
	 * Log In Progress Transaction Properties
	 * @param transactionCacheKey
	 * @param logger
	 */
	public static void logInProgressTransactionProperties(TransactionCacheKey transactionCacheKey, Logger logger) {
		logger.log(Level.INFO, "Transaction Id: " + transactionCacheKey);
		logger.log(Level.INFO, "Transaction Status: " + TransactionConstants.TRANSACTION_IN_PROGRESS);
	}

	/**
	 * Log to process log when an operation has joined a transaction along with transaction ID
	 *
	 * @param responseLogger The logger from OperationResponse to write to process log.
	 * @param transactionId  The key or identifier for the transaction being processed.
	 */
	public static void logJoinTransactionStatus(Logger responseLogger,
			String transactionId) {
		String logMessage = String.format("Joined transaction %s", transactionId);
		responseLogger.log(Level.INFO, logMessage);
	}
}
