//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;
import java.io.Closeable;
import java.io.InputStream;

/**
 * The Class RetryableUploadFileAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableUploadFileAction extends RetryableAction implements Closeable {
	
	/** The file path. */
	private final String filePath;
	
	/** The input stream. */
	private final InputStream inputStream;
	
	/** The append offset. */
	private final long appendOffset;

	/**
	 * Instantiates a new retryable upload file action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param retryFactory the retry factory
	 * @param filePath the file path
	 * @param input the input
	 * @param appendOffset the append offset
	 */
	public RetryableUploadFileAction(SFTPConnection connection, String remoteDir, RetryStrategyFactory retryFactory,
			String filePath, ObjectData input, long appendOffset) {
		super(connection, remoteDir, (TrackedData) input, retryFactory);
		this.filePath = filePath;
		this.inputStream = input.getData();
		this.appendOffset = appendOffset;
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		this.getConnection().uploadFile(this.filePath, this.inputStream, this.appendOffset);
	}

	/**
	 * Close.
	 */
	@Override
	public void close() {
		IOUtil.closeQuietly(this.inputStream);
	}
}
