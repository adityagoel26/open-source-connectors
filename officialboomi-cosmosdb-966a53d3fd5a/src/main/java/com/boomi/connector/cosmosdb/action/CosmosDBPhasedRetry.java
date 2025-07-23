//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.action;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.cosmosdb.CosmosDBConnection;
import com.boomi.connector.exception.CosmosDBRetryException;
import com.boomi.util.StringUtil;
import com.boomi.util.retry.PhasedRetry;

/**
 * Base class for all retryable operations.
 * @author abhijit.d.mishra
 */
public class CosmosDBPhasedRetry extends PhasedRetry{
	
	private int maxRetries;
	CosmosDBConnection connection;
	
	CosmosDBPhasedRetry(int maxRetries, CosmosDBConnection connection) {
        super(maxRetries);
        setMaxretries(maxRetries);
        this.connection = connection;
    }
	/**
	 * This function is for logging the retry operation.
	 **/
	public void logRetryAttempt(int attemptNumber, boolean exceptionOccurred, boolean shouldRetry, Logger logger) {
		StringBuilder logMsg = new StringBuilder();
		Level logLevel = Level.FINER;
		if (exceptionOccurred) {
			logLevel = getFailureLogLevel(attemptNumber);
			if (attemptNumber == getMaxRetries()) {
				logMsg.append("Max Retry Attempts: ").append(getMaxRetries()).append(" completed");
			} else {
				logMsg.append("Operation failed for attempt : ").append(attemptNumber);
				logMsg.append(shouldRetry ? "Attempting to reconnect" : StringUtil.EMPTY_STRING);
			}
		} else {
			logMsg.append("Operation succeeded for attempt: ").append(attemptNumber);
		}
		String logMessage = logMsg.toString();
		logger.log(logLevel, logMessage);
	}
	
	/**
	 * This function is to check if the exception is recoverable.
	 **/
	public boolean isErrorRecoverable(Exception ex) {
		boolean isErrorRecoverable = false;
		if (null != ex) {
			isErrorRecoverable = CosmosDBRetryException.class.isInstance(ex);
		}
		return isErrorRecoverable;
	}
	
	/**
	 * This function is to check if the retry is possible after any exception
	 **/
	public boolean shouldRetry(int retryNumber, Object status, Exception exception) {
		return isErrorRecoverable(exception) && super.shouldRetry(retryNumber, status)
				&& retryNumber < getMaxRetries();
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxretries(int maxretries) {
		this.maxRetries = maxretries;
	}

}
