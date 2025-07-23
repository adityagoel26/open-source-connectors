//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.listener;

import java.util.logging.Logger;

import com.boomi.connector.api.listen.ListenManager;


/**
 * The Class SFTPListenManager.
 * @author sweta.b.das
 */
public class SFTPListenManager implements ListenManager {
	
    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(SFTPListenManager.class.getName());

    /**
     * Instantiates a new SFTP listen manager.
     */
    public SFTPListenManager() {
		super();
	}

	/**
	 * Start.
	 */
	@Override
	public void start() {
		logger.info("There is nothing to configure in this method from the server perspective... Moving to ListenerOperation.start()...");
	}

	/**
	 * Stop.
	 */
	@Override
	public void stop() {
		logger.info("All Resources are closed in ListenerOperation.stop().. Server will be manually stopped by the end user.. ");
	}

}
