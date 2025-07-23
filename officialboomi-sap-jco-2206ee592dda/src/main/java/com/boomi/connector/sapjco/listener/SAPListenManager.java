// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.listener;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.listen.ListenManager;


/**
 * @author a.kumar.samantaray
 *
 */
public class SAPListenManager implements ListenManager{

	 private static final Logger logger = Logger.getLogger("com.boomi.connector.sap.listener");
	    
	public SAPListenManager() {
		super();
	}

	@Override
	public void start() {
		 logger.log(Level.INFO, "SAPListenManager started.");
		
	}

	@Override
	public void stop() {
		 logger.log(Level.INFO, "SAPListenManager stopped.");
		
	}

}
