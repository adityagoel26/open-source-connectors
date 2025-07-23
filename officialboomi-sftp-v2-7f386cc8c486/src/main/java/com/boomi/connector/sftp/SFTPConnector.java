//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.listen.ListenConnector;
import com.boomi.connector.api.listen.ListenManager;
import com.boomi.connector.api.listen.ListenOperation;
import com.boomi.connector.sftp.listener.SFTPListenManager;
import com.boomi.connector.sftp.operations.SFTPCreateOperation;
import com.boomi.connector.sftp.operations.SFTPDeleteOperation;
import com.boomi.connector.sftp.operations.SFTPGetOperation;
import com.boomi.connector.sftp.operations.SFTPListenOperation;
import com.boomi.connector.sftp.operations.SFTPQueryOperation;
import com.boomi.connector.sftp.operations.SFTPUpsertOperation;
import com.boomi.connector.util.BaseConnector;

/**
 * The Class SFTPConnector.
 *
 * @author Omesh Deoli
 * 
 */
public class SFTPConnector extends BaseConnector implements ListenConnector<ListenManager> {

	/**
	 * Creates the browser.
	 *
	 * @param context the context
	 * @return the browser
	 */
	@Override
	public Browser createBrowser(BrowseContext context) {
		return new SFTPBrowser(createConnection(context));
	}

	/**
	 * Creates the connection.
	 *
	 * @param context the context
	 * @return the SFTP connection
	 */
	private SFTPConnection createConnection(BrowseContext context) {
		return new SFTPConnection(context);
	}

	/**
	 * Creates the create operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createCreateOperation(OperationContext context) {
		return new SFTPCreateOperation(createConnection(context));
	}

	/**
	 * Creates the get operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createGetOperation(OperationContext context) {
		return new SFTPGetOperation(createConnection(context));
	}

	/**
	 * Creates the query operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createQueryOperation(OperationContext context) {
		return new SFTPQueryOperation(createConnection(context));
	}

	/**
	 * Creates the delete operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createDeleteOperation(OperationContext context) {
		return new SFTPDeleteOperation(createConnection(context));
	}

	/**
	 * Creates the upsert operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createUpsertOperation(OperationContext context) {
		return new SFTPUpsertOperation(createConnection(context));
	}

	@Override
	public ListenOperation<ListenManager> createListenOperation(OperationContext context) {
		return new SFTPListenOperation(createConnection(context));
	}

	@Override
	public ListenManager createListenManager(ConnectorContext context) {
		return new SFTPListenManager();
	}
}
