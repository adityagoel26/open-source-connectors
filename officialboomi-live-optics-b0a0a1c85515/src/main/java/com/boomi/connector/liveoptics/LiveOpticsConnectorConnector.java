//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;

/**
 * @author Naveen Ganachari
 *
 * ${tags}
 */
public class LiveOpticsConnectorConnector extends BaseConnector {

	@Override
	public Browser createBrowser(BrowseContext context) {
		return new LiveOpticsConnectorBrowser(createConnection(context));
	}

	@Override
	protected Operation createGetOperation(OperationContext context) {
		return new LiveOpticsConnectorGetOperation(createConnection(context));
	}


	private LiveOpticsConnectorConnection createConnection(BrowseContext context) {
		return new LiveOpticsConnectorConnection(context);
	}	
}