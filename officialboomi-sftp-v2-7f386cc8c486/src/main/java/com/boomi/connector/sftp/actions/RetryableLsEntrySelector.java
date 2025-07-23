//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import java.util.logging.Level;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.CustomLsEntrySelector;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.exception.FileNameNotSupportedException;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;

/**
 * The Class RetryableLsEntrySelector.
 *         
 */
public class RetryableLsEntrySelector {

	/** The selector. */
	private final LsEntrySelector selector;

	/** The dir full path. */
	private String dirFullPath;

	/** The input. */
	private TrackedData input;

	/** The connection. */
	private SFTPConnection connection;
	


	/**
	 * Instantiates a new retryable ls entry selector.
	 *
	 * @param connection the connection
	 * @param remoteDir  the remote dir
	 * @param input      the input
	 * @param selector   the selector
	 */
	public RetryableLsEntrySelector(SFTPConnection connection, String remoteDir, TrackedData input,
			LsEntrySelector selector) {
		this.selector = selector;
		this.dirFullPath = remoteDir;
		this.input = input;
		this.connection = connection;
	}

	/**
	 * Gets the selector.
	 *
	 * @return the selector
	 */
	public LsEntrySelector getSelector() {
		return selector;
	}

	/**
	 * Execute.
	 */
	public void execute() {

		try {
			this.doExecute();

		} catch (SFTPSdkException sftpSdkException) {
			if (sftpSdkException.getCause() != null && sftpSdkException.getCause().getCause() != null && sftpSdkException.getCause().getCause().getCause() != null
					&& sftpSdkException.getCause().getCause().getCause().toString().contains("DataStoreLimitException")) {
				throw sftpSdkException;
			}

			CustomLsEntrySelector customLsEntrySelector = (CustomLsEntrySelector) selector;
			if (customLsEntrySelector.isReconnectFailed()) {
				this.input.getLogger().log(Level.WARNING,
						"Failed to re-establish connectivity with remote server after maximum allowed attempts. Could "
								+ "not fetch all the documents", sftpSdkException);
			} else {
				this.input.getLogger().log(Level.WARNING,
						"Connection successfully re-established but failed to fetch one or more documents",
						sftpSdkException);
			}
		} catch (FileNameNotSupportedException fileNameNotSupportedException) {
			this.input.getLogger().log(Level.WARNING,
					"Failed to fetch one or more documents due to non-permissible characters in the filename. \n"
							+ fileNameNotSupportedException.getMessage(), fileNameNotSupportedException);
		}
	}

	/**
	 * Do execute.
	 */
	public void doExecute() {
		connection.listDirectoryContentWithSelector(dirFullPath, selector);
	}

}
