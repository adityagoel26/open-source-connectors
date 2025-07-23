//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.common.MeteredTempOutputStream;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;
import java.io.Closeable;

/**
 * The Class RetryableRetrieveFileAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableRetrieveFileAction extends RetryableAction implements Closeable {
	
	/** The file path. */
	private final String filePath;
	
	
	/** The output stream. */
	private final MeteredTempOutputStream outputStream = new MeteredTempOutputStream();

	/**
	 * Instantiates a new retryable retrieve file action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param filePath the file path
	 * @param input the input
	 * @param retryFactory the retry factory
	 * @param fileName 
	 */
	public RetryableRetrieveFileAction(SFTPConnection connection, String remoteDir, String filePath, TrackedData input,
			RetryStrategyFactory retryFactory) {
		super(connection, remoteDir, input, retryFactory);
		this.filePath = filePath;
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
	@Override
	public void doExecute() {
		this.getConnection().getFile(this.filePath, this.outputStream);
	}

	/**
	 * Close.
	 */
	@Override
	public void close() {
		IOUtil.closeQuietly(this.outputStream);
	}
}
