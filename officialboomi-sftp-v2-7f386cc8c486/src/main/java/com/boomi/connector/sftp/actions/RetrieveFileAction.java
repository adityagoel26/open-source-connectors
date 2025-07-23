//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.actions;

import java.io.Closeable;

import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.common.MeteredTempOutputStream;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;


/**
 * The Class RetrieveFileAction.
 * @author sweta.b.das
 */
public class RetrieveFileAction implements Closeable{

	/** The file path. */
	private final String filePath;

	/** The output stream. */
	private final MeteredTempOutputStream outputStream = new MeteredTempOutputStream();
	
	/** The connection. */
	private final SFTPConnection connection;

	/**
	 * Instantiates a new retryable retrieve file action.
	 *
	 * @param connection the connection
	 * @param filePath the file path
	 * @param retryFactory the retry factory
	 */
	public RetrieveFileAction(SFTPConnection connection, String filePath,
			RetryStrategyFactory retryFactory) {
		this.filePath = filePath;
		this.connection = connection;
	}

	/**
	 * Gets the output stream.
	 *
	 * @return the output stream
	 */
	public MeteredTempOutputStream getOutputStream() {
		return outputStream;
	}

	/**
	 * Do execute.
	 */
	
	public void doExecute() {
		connection.getFile(this.filePath, this.outputStream);
	}

	/**
	 * Close.
	 */
	@Override
	public void close() {
		IOUtil.closeQuietly(this.outputStream);
	}
}
