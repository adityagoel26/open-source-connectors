// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.oracledatabase.model.BatchResponse;
import com.boomi.connector.oracledatabase.model.ErrorDetails;
import com.boomi.util.json.JsonPayloadUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.FAILED_BATCH_NUM;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.FAILED_BATCH_RECORDS;

/**
 * This Class will generate the Custom Payload Based on the Functionality and
 * adds it to the Response for each ObjectData.
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
						OracleDatabaseConstants.DUPLICATE_PRIMARY_KEY, e.getMessage(), payload);
			} catch (IOException ioE) {
				throw new ConnectorException(ioE.toString());
			}

		} else {
			try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(405, e.getMessage()))) {
				response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
						OracleDatabaseConstants.DUPLICATE_PRIMARY_KEY, e.getMessage(), payload);
			} catch (Exception ge) {
				throw new ConnectorException(ge.toString());
			}

		}

	}
	
	/**
	 * Util method for writing the error responses based on the exception thrown.
	 *
	 * @param e        the e
	 * @param objdata  the objdata
	 * @param response the response
	 */
	public static void writeErrorResponseForEmptyMessages(Exception e, ObjectData objdata, OperationResponse response) {
		String message = null;
		if (e.getMessage() == null || e.getMessage().isEmpty()) {
			message= e.toString();
		} else {
			message= e.getMessage();
		}
		try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(405, message))) {
			response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
					OracleDatabaseConstants.DUPLICATE_PRIMARY_KEY, e.toString(), payload);
		} catch (Exception ioE) {
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
	 * Writes invalid input response.
	 *
	 * @param e the e
	 * @param objdata the objdata
	 * @param response the response
	 */
	public static void writeInvalidInputResponse(IllegalArgumentException e, ObjectData objdata,
			OperationResponse response) {
		try (Payload payload = JsonPayloadUtil.toPayload(new ErrorDetails(405, e.toString() + " - " + OracleDatabaseConstants.INPUT_ERROR ))) {
			response.addResult(objdata, OperationStatus.APPLICATION_ERROR,
					OracleDatabaseConstants.DUPLICATE_PRIMARY_KEY, e.toString() + " - " + OracleDatabaseConstants.INPUT_ERROR, payload);
		} catch (IOException ioE) {
			throw new ConnectorException(ioE.toString());
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
	 * Log failed batch with batch number and records in the batch.
	 *
	 * @param response the response
	 * @param batchNumber the batchNumber
	 * @param b        the b
	 */
	public static void logFailedBatch(OperationResponse response, int batchNumber, int b) {
		response.getLogger().log(Level.SEVERE, FAILED_BATCH_NUM, batchNumber);
		response.getLogger().log(Level.SEVERE, FAILED_BATCH_RECORDS, b);

	}

}
