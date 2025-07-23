//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.actions.RetryableDeleteFileAction;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.NoSuchFileFoundException;
import com.boomi.connector.sftp.results.EmptySuccess;
import com.boomi.connector.sftp.results.ErrorResult;
import com.boomi.connector.sftp.results.Result;
import com.boomi.connector.util.BaseConnection;
import com.boomi.connector.util.BaseDeleteOperation;
import com.boomi.util.LogUtil;

/**
 * The Class SFTPDeleteOperation.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SFTPDeleteOperation extends BaseDeleteOperation {
	
	/**
	 * Instantiates a new SFTP delete operation.
	 *
	 * @param conn the conn
	 */
	public SFTPDeleteOperation(SFTPConnection conn) {
		super((BaseConnection<?>) conn);
	}

	/**
	 * Execute delete.
	 *
	 * @param deleteRequest the delete request
	 * @param operationResponse the operation response
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void executeDelete(DeleteRequest deleteRequest, OperationResponse operationResponse) {
		
		SFTPConnection conn = this.getConnection();
		try {
		conn.openConnection();
		for (ObjectIdData input : deleteRequest) {
			executeRetryableDeleteAndAddToResponse(operationResponse, conn, input);
		}
		}
		catch(Exception e) {
			ResponseUtil.addExceptionFailures(operationResponse, (Iterable) deleteRequest, e);
		}
		finally {
			conn.closeConnection();
		}
	}

	/**
	 * @param operationResponse operation response
	 * @param conn SFTP connection
	 * @param input the input
	 */
	private static void executeRetryableDeleteAndAddToResponse(OperationResponse operationResponse, SFTPConnection conn, ObjectIdData input) {
		Result result;
		try {
			RetryableDeleteFileAction retryableDelete = new RetryableDeleteFileAction(conn, input);
			retryableDelete.execute();
			result = retryableDelete.getResult();
			LogUtil.fine(input.getLogger(),SFTPConstants.FILE_DELETED);
			result.addToResponse(operationResponse, (TrackedData) input);
		} catch (NoSuchFileFoundException e) {
			result = new EmptySuccess();
			result.addToResponse(operationResponse, (TrackedData) input);
			LogUtil.severe(input.getLogger(),SFTPConstants.FILE_DOESNOT_EXISTS, e);
		} catch (Exception e) {
			LogUtil.severe(input.getLogger(), e,
					 SFTPConstants.ERROR_DELETING_FILE,
			input.getObjectId());
			result = new ErrorResult(e);
			result.addToResponse(operationResponse, (TrackedData) input);
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
