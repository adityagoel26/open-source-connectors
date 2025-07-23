/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.eventbridge;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;

/**
 * It overrides the methods from BaseConnector.
 * 
 * @author swastik.vn
 *
 */
public class AWSEventBridgeConnector extends BaseConnector {

	/**
	 * Overrides createBrowser method from BaseConnector.
	 * 
	 * @param context
	 * @return the AWSEventBridgeBrowser instance.
	 *
	 */
	@Override
	public Browser createBrowser( BrowseContext context) {
		return new AWSEventBridgeBrowser(createConnection(context));
	}

	/**
	 * Overrides createCreateOperation method from BaseConnector.
	 * 
	 * @param context
	 * @return the AWSEventBridgeCreateOperation instance.
	 *
	 */
	
	@Override
	protected Operation createCreateOperation(OperationContext context) {
		return new AWSEventBridgeCreateOperation(createConnection(context));
	}

	/**
	 * Create the AWSEventBridgeConnection instance from BrowseContext.
	 * 
	 * @param context
	 * @return the AWSEventBridgeConnection instance.
	 *
	 */
	private AWSEventBridgeConnection createConnection(BrowseContext context) {
		return new AWSEventBridgeConnection(context);
	}
}