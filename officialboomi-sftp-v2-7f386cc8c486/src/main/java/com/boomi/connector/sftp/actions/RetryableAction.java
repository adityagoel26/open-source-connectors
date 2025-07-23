//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.retry.PhasedRetry;
import com.boomi.util.retry.RetryStrategy;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

import java.util.logging.Level;

/**
 * The Class RetryableAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public abstract class RetryableAction {
	
	/** The Constant NULL_STATUS. */
	protected static final Object NULL_STATUS = null;
	
	/** The connection. */
	private final SFTPConnection connection;
	
	/** The remote dir. */
	private final String remoteDir;
	
	/** The input. */
	protected final TrackedData input;
	
	/** The retry factory. */
	protected final RetryStrategyFactory retryFactory;

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public TrackedData getInput() {
		return input;
	}

	/**
	 * Instantiates a new retryable action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param input the input
	 * @param retryFactory the retry factory
	 */
	RetryableAction(SFTPConnection connection, String remoteDir, TrackedData input, RetryStrategyFactory retryFactory) {
		this.connection = connection;
		this.remoteDir = remoteDir;
		this.input = input;
		this.retryFactory = retryFactory;
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

				if (((SftpException) e.getCause()).id == ChannelSftp.SSH_FX_CONNECTION_LOST
						|| ((SftpException) e.getCause()).id == ChannelSftp.SSH_FX_NO_CONNECTION
						|| ((SftpException) e.getCause()).id == ChannelSftp.SSH_FX_FAILURE) {
					if (!retry.shouldRetry(++numAttempts, NULL_STATUS)) {
						throw e;
					}
					this.input.getLogger().log(Level.WARNING,
							"Failure Occured.Attempting to Retry the action. Retry attempt no {0}. " , numAttempts);
					this.reconnect();
				} else {
					throw e;
				}
			}

		} while (true);
	}

	/**
	 * Do execute.
	 */
	abstract void doExecute();

	/**
	 * Reconnect.
	 */
	protected void reconnect() {
		PhasedRetry reconnectRetry = new PhasedRetry();
		int numAttempts = 0;
		do {
			try {
				this.input.getLogger().log(Level.WARNING, "Attempting to re-establish connection with remote system,"
						+ "Phased Reconnect attempt no:  {0}." , numAttempts);
				this.getConnection().reconnect();
				this.input.getLogger().log(Level.WARNING,
						"Established connectivity with the remote system.Resuming operation");
				return;
			} catch (ConnectorException e) {
				if (!reconnectRetry.shouldRetry(++numAttempts, NULL_STATUS)) {
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
	SFTPConnection getConnection() {
		return this.connection;
	}

	/**
	 * Gets the remote dir.
	 *
	 * @return the remote dir
	 */
	String getRemoteDir() {
		return this.remoteDir;
	}
}
