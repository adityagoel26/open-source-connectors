//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.handlers.UpsertHandler;
import com.boomi.connector.util.BaseUpdateOperation;

/**
 * The Class SFTPUpsertOperation.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SFTPUpsertOperation extends BaseUpdateOperation {

	/**
	 * Instantiates a new SFTP upsert operation.
	 *
	 * @param conn the conn
	 */
	public SFTPUpsertOperation(SFTPConnection conn) {
		super(conn);
	}

	/**
	 * Execute update.
	 *
	 * @param request the request
	 * @param response the response
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		try {
			this.getConnection().openConnection();
			UpsertHandler handler = new UpsertHandler(this.getConnection(), response);
			handler.processMultiInput(request);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, (Iterable) request, e);
		} finally {
			this.getConnection().closeConnection();
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
