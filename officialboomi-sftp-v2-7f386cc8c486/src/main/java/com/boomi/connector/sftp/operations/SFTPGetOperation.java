//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.handlers.DownloadHandler;
import com.boomi.connector.util.BaseGetOperation;

/**
 * The Class SFTPGetOperation.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SFTPGetOperation extends BaseGetOperation {

	/**
	 * Instantiates a new SFTP get operation.
	 *
	 * @param conn the conn
	 */
	public SFTPGetOperation(SFTPConnection conn) {
		super(conn);
	}

	/**
	 * Execute get.
	 *
	 * @param request the request
	 * @param response the response
	 */
	@Override
	protected void executeGet(GetRequest request, OperationResponse response) {

		try {
			this.getConnection().openConnection();
			DownloadHandler handler = new DownloadHandler(this.getConnection(), response);
			handler.processInput(request.getObjectId(),this.getContext());
		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, request.getObjectId(), e);
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
