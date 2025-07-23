// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.sap.util.Action;
import com.boomi.connector.util.BaseConnector;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorConnector extends BaseConnector {

	@Override
	public Browser createBrowser(BrowseContext context) {
		return new SAPConnectorBrowser(createConnection(context));
	}

	@Override
	protected Operation createQueryOperation(OperationContext context) {
		return new SAPConnectorQueryOperation(createConnection(context));
	}
	
	@Override
	protected Operation createExecuteOperation(OperationContext context) {
		String operationType = context.getCustomOperationType();

		switch (operationType) {
		case Action.GET_WITH_PARAMS:
			return new SAPConnectorExecuteOperation(createConnection(context));
		case Action.DELETE_WITH_PARAMS:
			return new SAPConnectorDeleteOperation(createConnection(context));
		default:
			throw new UnsupportedOperationException();
		}
	}
	
	@Override
	protected Operation createCreateOperation(OperationContext context) {
		return new SAPConnectorCreateOperation(createConnection(context));
	}
	
	@Override
	protected Operation createUpdateOperation(OperationContext context) {
		return new SAPConnectorUpdateOperation(createConnection(context));
	}
	
	@Override
	protected Operation createDeleteOperation(OperationContext context) {
		return new SAPConnectorDeleteOperation(createConnection(context));
	}

	private SAPConnectorConnection createConnection(BrowseContext context) {
		return new SAPConnectorConnection(context);
	}
}