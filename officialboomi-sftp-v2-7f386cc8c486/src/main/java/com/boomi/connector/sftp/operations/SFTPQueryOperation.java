//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.sftp.ListResultBuilder;
import com.boomi.connector.sftp.QueryResultBuilder;
import com.boomi.connector.sftp.ResultBuilder;
import com.boomi.connector.sftp.SFTPClient;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.SFTPCustomType;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.handlers.QueryHandler;
import com.boomi.connector.sftp.results.ErrorResult;
import com.boomi.connector.sftp.results.MultiResult;
import com.boomi.connector.util.BaseConnection;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.LogUtil;

/**
 * The Class SFTPQueryOperation.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SFTPQueryOperation extends BaseQueryOperation {
	
	/**
	 * Instantiates a new SFTP query operation.
	 *
	 * @param conn the conn
	 */
	public SFTPQueryOperation(SFTPConnection conn) {
		super((BaseConnection<?>) conn);
	}

	/**
	 * Execute query.
	 *
	 * @param queryRequest the query request
	 * @param operationResponse the operation response
	 */
	protected void executeQuery(QueryRequest queryRequest, OperationResponse operationResponse) {
		SFTPConnection conn = this.getConnection();
		FilterData input = queryRequest.getFilter();
		MultiResult multiResult = new MultiResult(operationResponse, input);
		try {
			conn.openConnection();
			SFTPCustomType type = SFTPCustomType.valueOf(this.getContext().getCustomOperationType());
			QueryHandler qhandler= new QueryHandler(conn, operationResponse);
			switch (type) {
			case LIST: {
				ResultBuilder resultBuilder=new ListResultBuilder();
				qhandler.doQuery(multiResult, false, resultBuilder, getContext().getOperationProperties());
				return;
			}
			case QUERY: {
				ResultBuilder resultBuilder=new QueryResultBuilder();
				qhandler.doQuery(multiResult, true, resultBuilder, getContext().getOperationProperties());
				return;
			}
			default: {
				throw new IllegalArgumentException(SFTPConstants.INVALID_QUERY_TYPE + type);
			}
			}
		} catch(SFTPSdkException e) {
			input.getLogger().log(Level.WARNING,SFTPConstants.ERROR_REMOTE_DIRECTORY_NOT_FOUND);
			multiResult.addPartialResult(new ErrorResult(OperationStatus.APPLICATION_ERROR, e));
		} catch (Exception e) {
			LogUtil.severe(input.getLogger(), SFTPConstants.ERROR_SEARCHING, e);
			OperationStatus status = multiResult.isEmpty() ? OperationStatus.FAILURE
					: OperationStatus.APPLICATION_ERROR;
			multiResult.addPartialResult(new ErrorResult(status, e));

		} finally {
			multiResult.finish();
			conn.closeConnection();
		}
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

}
