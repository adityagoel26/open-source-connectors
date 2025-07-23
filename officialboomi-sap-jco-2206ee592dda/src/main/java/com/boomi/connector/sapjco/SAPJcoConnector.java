// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.listen.ListenConnector;
import com.boomi.connector.api.listen.ListenOperation;
import com.boomi.connector.sapjco.listener.SAPJcoListenOperation;
import com.boomi.connector.sapjco.listener.SAPListenManager;
import com.boomi.connector.sapjco.operation.CustomOperationType;
import com.boomi.connector.sapjco.operation.SAPJCoSendOperation;
import com.boomi.connector.sapjco.operation.SAPJcoExecuteOperation;
import com.boomi.connector.util.BaseConnector;
import com.boomi.util.IOUtil;

/**
 * @author kishore.pulluru
 *
 */

public class SAPJcoConnector extends BaseConnector implements ListenConnector<SAPListenManager> {

	@Override
	public Browser createBrowser(BrowseContext context) {
		return new SAPJcoBrowser(createConnection(context));
	}

	@Override
	protected Operation createExecuteOperation(OperationContext context) {
		CustomOperationType operationType = CustomOperationType.fromContext(context);
		SAPJcoConnection sapJcoConnection= null;
		switch (operationType) {
		case EXECUTE:
			try {
				sapJcoConnection = createConnection(context);
				return new SAPJcoExecuteOperation(sapJcoConnection);
			}catch(NoClassDefFoundError noClassDefFoundError) {
				//closing the sapjco connection before throwing NoClassDefFoundError in case of sapjco jars missing.
				IOUtil.closeQuietly(sapJcoConnection);
				throw noClassDefFoundError;
			}
			
		case SEND:
			try {
				sapJcoConnection = createConnection(context);
				return new SAPJCoSendOperation(sapJcoConnection);
			}catch(NoClassDefFoundError noClassDefFoundError) {
				//closing the sapjco connection before throwing NoClassDefFoundError in case of sapjco jars missing.
				IOUtil.closeQuietly(sapJcoConnection);
				throw noClassDefFoundError;
			}
			

		default:
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * This method returns the connection.
	 * @param context
	 * @return sapjcoConnection
	 */
	private SAPJcoConnection createConnection(BrowseContext context) {
		return new SAPJcoConnection(context);
	}

	@Override
	public SAPListenManager createListenManager(ConnectorContext context) {
		return new SAPListenManager();
	}

	@Override
	public ListenOperation<SAPListenManager> createListenOperation(OperationContext context) {
		SAPJcoConnection sapJcoConnection= null;
		try {
			sapJcoConnection = createConnection(context);
			return new SAPJcoListenOperation(sapJcoConnection);
		}catch(NoClassDefFoundError noClassDefFoundError) {
			//closing the sapjco connection before throwing NoClassDefFoundError in case of sapjco jars missing.
			IOUtil.closeQuietly(sapJcoConnection);
			throw noClassDefFoundError;
		}
	}

}