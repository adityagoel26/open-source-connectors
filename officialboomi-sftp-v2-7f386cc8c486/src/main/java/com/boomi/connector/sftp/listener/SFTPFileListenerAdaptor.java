//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.listener;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.actions.RetrieveFileAction;
import com.boomi.connector.sftp.common.PathsHandler;
import com.boomi.connector.sftp.common.SFTPFileMetadata;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.github.drapostolos.rdp4j.DirectoryListener;
import com.github.drapostolos.rdp4j.FileAddedEvent;
import com.github.drapostolos.rdp4j.FileModifiedEvent;
import com.github.drapostolos.rdp4j.FileRemovedEvent;
import com.github.drapostolos.rdp4j.spi.FileElement;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * The Class SFTPFileListenerAdaptor.
 * @author sweta.b.das
 */
public class SFTPFileListenerAdaptor implements DirectoryListener {

	/** The Constant logger. */
	public static final Logger logger = Logger.getLogger(SFTPFileListenerAdaptor.class.getName());

	/** The connection. */
	private SFTPConnection connection;

	/** The remote directory. */
	private String remoteDirectory;

	/** The listener. */
	private Listener listener;

	/** The paths handler. */
	final PathsHandler pathsHandler;


	String filename;

	/**
	 * Instantiates a new SFTP file listener adaptor.
	 *
	 * @param connection      the connection
	 * @param remoteDirectory the remote directory
	 * @param listener        the listener
	 */
	public SFTPFileListenerAdaptor(SFTPConnection connection, String remoteDirectory, Listener listener) {
		super();
		this.connection = connection;
		this.remoteDirectory = remoteDirectory;
		this.listener = listener;
		this.pathsHandler = this.connection.getPathsHandler();
	}

	/**
	 * To full path.
	 *
	 * @param childPath the child path
	 * @return the string
	 */
	private String toFullPath(String childPath) {
		return this.pathsHandler.resolvePaths(connection.getHomeDirectory(), childPath);
	}


	/**
	 * Gets the metadata.
	 *
	 * @param path the path
	 * @return the metadata
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public PayloadMetadata getMetadata(String path) {
		PayloadMetadata metadata = listener.createMetadata();
		SFTPFileMetadata ftpFileMetadata = this.pathsHandler.splitIntoDirAndFileName(path);
		if (path != null) {
			metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, ftpFileMetadata.getName());
			metadata.setTrackedProperty(SFTPConstants.REMOTE_DIRECTORY, ftpFileMetadata.getDirectory());
		}
		return metadata;
	}

	/**
	 * File added.
	 *
	 * @param event the event
	 * @throws InterruptedException the interrupted exception
	 */
	@Override
	public void fileAdded(FileAddedEvent event) throws InterruptedException {
		fileAddedOrModified(event.getFileElement());
	}



	private void submit(Payload payload) {
		listener.submit(payload);
	}

	public String getFilename() {
		return filename;
	}

	/**
	 * File removed.
	 *
	 * @param event the event
	 * @throws InterruptedException the interrupted exception
	 */
	@Override
	public void fileRemoved(FileRemovedEvent event) {
		InputStream inputStream = null;
		RetrieveFileAction rQuery = null;
		try {
			filename = event.getFileElement().getName();
			String remoteDir;

			if (StringUtil.isBlank(remoteDirectory) || !pathsHandler.isFullPath(remoteDirectory)) {
				remoteDir = this.toFullPath(remoteDirectory);
			} else {
				remoteDir = remoteDirectory;
			}
			String fullFilePath = pathsHandler.joinPaths(remoteDir, filename);
			PayloadMetadata metadata = this.getMetadata(fullFilePath);
			submit(PayloadUtil.toPayload(filename, metadata));
		} finally {
			IOUtil.closeQuietly(inputStream);
			IOUtil.closeQuietly(rQuery);
		}

	}

	/**
	 * File modified.
	 *
	 * @param event the event
	 * @throws InterruptedException the interrupted exception
	 */
	@Override
	public void fileModified(FileModifiedEvent event){
		fileAddedOrModified(event.getFileElement());
	}

	/**
	 * @param fileElement the file element
	 */
	private void fileAddedOrModified(FileElement fileElement) {
		InputStream inputStream = null;
		filename = fileElement.getName();
		String remoteDir;

		if (StringUtil.isBlank(remoteDirectory) || !pathsHandler.isFullPath(remoteDirectory)) {
			remoteDir = this.toFullPath(remoteDirectory);
		} else {
			remoteDir = remoteDirectory;
		}
		String fullFilePath = pathsHandler.joinPaths(remoteDir, filename);
		try(RetrieveFileAction rQuery = new RetrieveFileAction(connection, fullFilePath, RetryStrategyFactory.createFactory(1));) {
			rQuery.doExecute();
			inputStream = rQuery.getOutputStream().toInputStream();
			PayloadMetadata metadata = this.getMetadata(fullFilePath);
			submit(PayloadUtil.toPayload(inputStream, metadata));
		} catch (IOException e) {
			throw new ConnectorException(e);
		} finally {
			IOUtil.closeQuietly(inputStream);
		}
	}
}
