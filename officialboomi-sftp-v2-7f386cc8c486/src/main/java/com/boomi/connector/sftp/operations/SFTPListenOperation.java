// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.operations;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.listen.ListenManager;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.SingletonListenOperation;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.listener.SFTPDirectory;
import com.boomi.connector.sftp.listener.SFTPFileListenerAdaptor;
import com.boomi.connector.util.listen.UnmanagedListenOperation;
import com.boomi.util.StringUtil;
import com.github.drapostolos.rdp4j.DirectoryPoller;
import com.github.drapostolos.rdp4j.spi.PolledDirectory;

/**
 * The Class SFTPListenOperation.
 * @author sweta.b.das
 */
public class SFTPListenOperation extends UnmanagedListenOperation implements SingletonListenOperation<ListenManager> {

	/** The Constant logger. */
	public static final Logger logger = Logger.getLogger(SFTPListenOperation.class.getName());
	/** The Directory Poller**/
	private DirectoryPoller directoryPoller = null;
	
	/**
	 * Instantiates a new SFTP listen operation.
	 *
	 * @param sftpConnection the sftp connection
	 */
	public SFTPListenOperation(SFTPConnection sftpConnection) {
		super(sftpConnection);
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	@Override
	public SFTPConnection getConnection() {
		return (SFTPConnection) super.getConnection();
	}

	/**
	 * Checks if is singleton.
	 *
	 * @return true, if is singleton
	 */
	@Override
	public boolean isSingleton() {
		return getContext().getOperationProperties().getBooleanProperty("isSingleton", false);
	}

	/**
	 * Stop.
	 */
	@Override
	public void stop() {
		getConnection().closeConnection();
		if(directoryPoller != null) {
			directoryPoller.stop();
		}
	}

	/**
	 * Start.
	 *
	 * @param listener the listener
	 */
	@Override
	protected void start(Listener listener) {
		try {
			this.getConnection().openConnection();
			long pollingInterval = getContext().getOperationProperties()
					.getLongProperty(SFTPConstants.POLLING_INTERVAL);
			String remoteDirectory = getConnection().getContext().getConnectionProperties()
					.getProperty(SFTPConstants.REMOTE_DIRECTORY);
			if(!StringUtil.isEmpty(remoteDirectory) || !StringUtil.isBlank(remoteDirectory)) {
			PolledDirectory polledDirectory = new SFTPDirectory(getConnection(), remoteDirectory);
			directoryPoller = DirectoryPoller.newBuilder().addPolledDirectory(polledDirectory)
					.addListener(new SFTPFileListenerAdaptor(getConnection(), remoteDirectory, listener))
					.setPollingInterval(pollingInterval, TimeUnit.MILLISECONDS)
					.start();
			}else {
				throw new ConnectorException("Please provide a value for the Remote Directory field!");
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
	}

}
