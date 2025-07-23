//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;
import com.boomi.connector.workdayprism.operations.CompleteBucketOperation;
import com.boomi.connector.workdayprism.operations.CreateOperation;
import com.boomi.connector.workdayprism.operations.GetOperation;
import com.boomi.connector.workdayprism.operations.ImportOperation;
import com.boomi.connector.workdayprism.operations.UploadOperation;
import com.boomi.connector.workdayprism.utils.Constants;

/**
 * Connector implementations for Workday Prism Connector version 2.0.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class PrismConnector extends BaseConnector {

	/**
	 * Creates a new instance of PrismOperationConnection extending from PrismConnection
	 *
	 * @param context An instance of OperationContext the operation context
	 * @return an operation instance of PrismOperationConnection
	 */
	private static PrismOperationConnection createConnection(OperationContext context) {
		return new PrismOperationConnection(context);
	}

	/**
	 * Creates a new instance of PrismConnection by extending from BaseConnector
	 *
	 * @param context An instance of BrowseContext the operation context
	 * @return an operation instance of PrismConnection
	 */
	private static PrismConnection createConnection(BrowseContext context) {
		return new PrismConnection(context);
	}

	@Override
	public Browser createBrowser(BrowseContext context) {
		return new PrismBrowser(createConnection(context));
	}

	/**
	 * Creates a new instance of CreateOperation specific to version 2.0 of Workday
	 * Prism
	 * 
	 *
	 * @param context an instance of OperationContext
	 * 
	 * @return the Operation instance
	 */
	@Override
	protected Operation createCreateOperation(OperationContext context) {
		return new CreateOperation(createConnection(context));
	}

	/**
	 * Creates a new instance of GetOperation specific to version 2.0 of Workday
	 * Prism
	 * 
	 *
	 * @param context an instance of OperationContext
	 * 
	 * @return the Operation instance
	 */
	@Override
	protected Operation createGetOperation(OperationContext context) {
		return new GetOperation(createConnection(context));
	}

	/**
	 * Creates a new instance of execute operation - CompleteBucketOperation
	 * specific to version 2.0 of Workday Prism
	 * 
	 *
	 * @param context an instance of OperationContext
	 * 
	 * @return the Operation instance
	 */
	@Override
	protected Operation createExecuteOperation(OperationContext context) {
		if (Constants.COMPLETE_BUCKET_CUSTOM_TYPE_ID.equals(context.getCustomOperationType())) {
			return new CompleteBucketOperation(createConnection(context));
		} else if (Constants.IMPORT_CUSTOM_TYPE_ID.equals(context.getCustomOperationType())) {
			return new ImportOperation(createConnection(context)); 
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Creates a new instance of upsert operation - ImportOperation/UploadOperation
	 * specific to version 2.0 of Workday Prism
	 *
	 * @param context an instance of OperationContext
	 * 
	 * @return the Operation instance
	 */
	@Override
	protected Operation createUpsertOperation(OperationContext context) {
		return new UploadOperation(createConnection(context));
	}
}
