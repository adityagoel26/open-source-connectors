// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.oracledatabase.get.DynamicGetOperation;
import com.boomi.connector.oracledatabase.get.StandardGetOperation;
import com.boomi.connector.oracledatabase.storedprocedureoperation.StoredProcedureOperation;
import com.boomi.connector.oracledatabase.upsert.DynamicUpsert;
import com.boomi.connector.oracledatabase.upsert.StandardUpsert;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.util.BaseConnector;

/**
 * The Class DatabaseConnectorConnector.
 *
 * @author swastik.vn
 */
public class OracleDatabaseConnector extends BaseConnector {

	/**
	 * Creates the browser.
	 *
	 * @param context the context
	 * @return the browser
	 */
	@Override
	public Browser createBrowser(BrowseContext context) {
		return new OracleDatabaseBrowser(createConnection(context));
	}

	/**
	 * Creates the create operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createCreateOperation(OperationContext context) {
		String insertionType = (String) context.getOperationProperties().get(OracleDatabaseConstants.INSERTION_TYPE);
		switch (insertionType) {
		case OracleDatabaseConstants.DYNAMIC_INSERT:
			return new DynamicInsertOperation(createConnection(context));
		case OracleDatabaseConstants.STANDARD_INSERT:
			return new StandardOperation(createConnection(context));

		default:
			return null;
		}
	}

	/**
	 * Creates the update operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createUpdateOperation(OperationContext context) {
		String updateType = (String) context.getOperationProperties().get(OracleDatabaseConstants.TYPE);
		switch (updateType) {
		case OracleDatabaseConstants.DYNAMIC_UPDATE:
			return new DynamicUpdateOperation(createConnection(context));
		case OracleDatabaseConstants.STANDARD_UPDATE:
			return new StandardOperation(createConnection(context));
		default:
			return null;
		}
	}

	/**
	 * Creates the execute operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createExecuteOperation(OperationContext context) {
		String opsType = context.getCustomOperationType();
		String getType = (String) context.getOperationProperties().get(OracleDatabaseConstants.GET_TYPE);
		String deleteType = (String) context.getOperationProperties().get(OracleDatabaseConstants.DELETE_TYPE);
		switch (opsType) {
		case OracleDatabaseConstants.GET:
			if (getType.equals(OracleDatabaseConstants.DYNAMIC_GET)) {
				return new DynamicGetOperation(createConnection(context));
			} else {
				return new StandardGetOperation(createConnection(context));
			}

		case OracleDatabaseConstants.STOREDPROCEDUREWRITE:
			return new StoredProcedureOperation(createConnection(context));

		case OracleDatabaseConstants.DELETE:
			if (deleteType.equals(OracleDatabaseConstants.DYNAMIC_DELETE)) {
				return new DynamicDeleteOperation(createConnection(context));
			} else {
				return new StandardOperation(createConnection(context));
			}
		default:
			return null;
		}

	}

	/**
	 * Creates the upsert operation.
	 *
	 * @param context the context
	 * @return the operation
	 */
	@Override
	protected Operation createUpsertOperation(OperationContext context) {
		String upsertType = (String) context.getOperationProperties().get(OracleDatabaseConstants.UPSERT_TYPE);
		switch (upsertType) {
		case OracleDatabaseConstants.DYNAMIC_UPSERT:
			return new DynamicUpsert(createConnection(context));
		case OracleDatabaseConstants.STANDARD_UPSERT:
			return new StandardUpsert(createConnection(context));
		default:
			return null;
		}
		
	}

	/**
	 * Creates the connection.
	 *
	 * @param context the context
	 * @return the database connector connection
	 */
	private OracleDatabaseConnection createConnection(BrowseContext context) {
		return new OracleDatabaseConnection(context);
	}
}