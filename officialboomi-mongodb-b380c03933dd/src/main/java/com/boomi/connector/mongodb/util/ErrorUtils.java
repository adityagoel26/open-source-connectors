// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.util;

import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.bean.ErrorDetails;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.mongodb.MongoException;

import java.util.List;

/**
 * The Class ErrorUtils contains logic to update error details for given batch.
 *
 */
public class ErrorUtils {

	/**
	 * Instantiates a new error utils.
	 */
	private ErrorUtils() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Update error details in batch.
	 *
	 * @param ex    the exception
	 * @param batch the batch
	 */
	public static void updateErrorDetailsinBatch(Exception ex, List<TrackedDataWrapper> batch) {
		Integer errorCode = null;
		String errorMessage = ex.getMessage();
		if (ex instanceof MongoException) {
			errorCode = ((MongoException) ex).getCode();
		}
		updateErrorDetails(batch, errorCode, errorMessage);
	}

	/**
	 * Fetch error code.
	 *
	 * @param errorDetails the error details
	 * @return the string
	 */
	public static String fetchErrorCode(ErrorDetails errorDetails) {
		String errorCode = null;

		if (null == errorDetails) {
			errorCode = MongoDBConstants.STATUS_CODE_FAILURE;
		} else {
			errorCode = getNullSafeErrorCode(errorDetails.getErrorCode());
		}

		return errorCode;
	}

	/**
	 * Gets the null safe error code.
	 *
	 * @param errorCode the error code
	 * @return the null safe error code
	 */
	private static String getNullSafeErrorCode(Integer errorCode) {
		String nullSafeErrorCodeVal = null;
		if (null == errorCode) {
			nullSafeErrorCodeVal = MongoDBConstants.STATUS_CODE_FAILURE;
		} else {
			nullSafeErrorCodeVal = String.valueOf(errorCode);
		}
		return nullSafeErrorCodeVal;
	}

	/**
	 * Update error details.
	 *
	 * @param batch        the batch
	 * @param errorCode    the error code
	 * @param errorMessage the error message
	 */
	public static void updateErrorDetails(List<TrackedDataWrapper> batch, int errorCode, String errorMessage) {
		for (TrackedDataWrapper item : batch) {
			item.setErrorDetails(errorCode, errorMessage);
		}
	}

}
