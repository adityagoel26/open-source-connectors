//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPClient;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.common.MeteredTempOutputStream;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;
import com.boomi.util.retry.PhasedRetry;
import com.boomi.util.retry.RetryStrategy;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

import java.io.Closeable;
import java.util.logging.Level;


/**
 * The Class RetryableQueryAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableQueryAction implements Closeable {
	
	/**
	 * Checks if is reconnect failed.
	 *
	 * @return true, if is reconnect failed
	 */
	public boolean isReconnectFailed() {
		return reconnectFailed;
	}

	/**
	 * Sets the reconnect failed.
	 *
	 * @param reconnectFailed the new reconnect failed
	 */
	public void setReconnectFailed(boolean reconnectFailed) {
		this.reconnectFailed = reconnectFailed;
	}

	/** The Constant NULL_STATUS. */
	private static final Object NULL_STATUS = null;
	
	/** The connection. */
	private final SFTPConnection connection;
	
	
	/** The input. */
	private final TrackedData input;
	
	/** The retry factory. */
	private final RetryStrategyFactory retryFactory;
	
	/** The client. */
	private final SFTPClient client;
	
	/** The file path. */
	private final String filePath;
	
	
	/** The output stream. */
	private final MeteredTempOutputStream outputStream = new MeteredTempOutputStream();
	
	/** The reconnect failed. */
	private boolean reconnectFailed;

	/**
	 * Gets the output stream.
	 *
	 * @return the output stream
	 */
	public MeteredTempOutputStream getOutputStream() {
		return outputStream;
	}

	/**
	 * Instantiates a new retryable query action.
	 *
	 * @param connection the connection
	 * @param input the input
	 * @param retryFactory the retry factory
	 * @param client the client
	 * @param filePath the file path
	 * @param fileName
	 */
	public RetryableQueryAction(SFTPConnection connection, TrackedData input,
			RetryStrategyFactory retryFactory, SFTPClient client, String filePath) {
		this.connection = connection;
		this.input = input;
		this.retryFactory = retryFactory;
		this.client = client;
		this.filePath = filePath;
	}

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public TrackedData getInput() {
		return input;
	}

	/**
	 * Execute.
	 */
	public void execute() {
		RetryStrategy retry = this.retryFactory.createRetryStrategy();
		int numAttempts = 0;
		do {
			try {
				this.doExecute();
				return;
			} catch (SFTPSdkException e) {

				if (isSshFxException(e)) {
					numAttempts = shouldRetryOrThrowSFTPSdkException(retry, numAttempts, e);
					this.input.getLogger().log(Level.WARNING,
							"Failure Occured.Attempting to Retry the action. Retry attempt no {0}. " ,numAttempts);
					this.reconnect();
				} else {
					throw e;
				}
			} catch (ConnectorException e) {
				numAttempts = shouldRetryOrThrowConnectorException(retry, numAttempts, e);
				this.input.getLogger().log(Level.WARNING,
						"Failure Occured.Attempting to Retry the action. Retry attempt no {0}. " ,numAttempts);
				this.reconnect();
			}

		} while (true);
	}

	/**
	 * @param e the exception
	 * @return boolean if exception is  SSH FX exception
	 */
	private static boolean isSshFxException(SFTPSdkException e) {
		int exceptionId = ((SftpException)e.getCause()).id;
		return (exceptionId == ChannelSftp.SSH_FX_CONNECTION_LOST
				|| exceptionId == ChannelSftp.SSH_FX_NO_CONNECTION
				|| exceptionId == ChannelSftp.SSH_FX_FAILURE);
	}

	/**
	 * @param retry Retry strategy
	 * @param numAttempts number of attempts for retrying
	 * @param e exception
	 * @return updated number of attempts
	 */
	private static int shouldRetryOrThrowSFTPSdkException(RetryStrategy retry, int numAttempts, SFTPSdkException e) {
		if (!retry.shouldRetry(++numAttempts, NULL_STATUS)) {
			throw e;
		}
		return numAttempts;
	}

	/**
	 * @param retry Retry strategy
	 * @param numAttempts number of attempts for retrying
	 * @param e exception
	 * @return updated number of attempts
	 */
	private static int shouldRetryOrThrowConnectorException(RetryStrategy retry, int numAttempts, ConnectorException e) {
		if (!retry.shouldRetry(++numAttempts, NULL_STATUS)) {
			throw e;
		}
		return numAttempts;
	}

	/**
	 * Do execute.
	 */
	public void doExecute()  {
		this.client.openConnection();
		this.getConnection().getFile(this.filePath, this.outputStream, this.client);
	}

	/**
	 * Reconnect.
	 */
	private void reconnect() {
		PhasedRetry reconnectRetry = new PhasedRetry();
		int numAttempts = 0;
		do {
			try {
				this.input.getLogger().log(Level.WARNING, "Attempting to re-establish connection with remote system,"
						+ "Phased Reconnect attempt no: {0} ", (numAttempts + 1));
				this.client.openConnection();
				this.input.getLogger().log(Level.WARNING,
						"Established connectivity with the remote system.Resuming operation");
				return;
			} catch (ConnectorException e) {
				if (!reconnectRetry.shouldRetry(++numAttempts, NULL_STATUS)) {
					reconnectFailed = true;
					throw e;
				}
				this.input.getLogger().log(Level.WARNING,
						"Attempted re-establishing connection, backing off. Cause: {0}." , e.getMessage());
				long timeBeforeBackoff = System.currentTimeMillis();
				reconnectRetry.backoff(numAttempts);
				long timeAfterBackoff = System.currentTimeMillis();
				this.input.getLogger().log(Level.WARNING,
						"Waited for " + (timeAfterBackoff - timeBeforeBackoff) + " milliseconds");

			} 

		} while (true);
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	public SFTPConnection getConnection() {
		return this.connection;
	}


	/**
	 * Close.
	 */
	@Override
	public void close() {
		IOUtil.closeQuietly(this.outputStream);
	}
}
