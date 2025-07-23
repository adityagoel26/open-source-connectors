//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.handlers;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.actions.RetryableVerifyFileExistsAction;
import com.boomi.connector.sftp.common.PathsHandler;
import com.boomi.connector.sftp.common.SFTPFileMetadata;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.results.ErrorResult;
import com.boomi.util.StringUtil;

import java.util.logging.Level;

/**
 * The Class BaseMultiInputHandler.
 *
 * @author Omesh Deoli
 * 
 * 
 * @param <T> the generic type
 */
abstract class BaseMultiInputHandler<T extends TrackedData> {
	
	/** The connection. */
	final SFTPConnection connection;
	
	/** The operation response. */
	final OperationResponse operationResponse;
	
	/** The paths handler. */
	public final PathsHandler pathsHandler;

	/**
	 * Instantiates a new base multi input handler.
	 *
	 * @param connection the connection
	 * @param operationResponse the operation response
	 */
	BaseMultiInputHandler(SFTPConnection connection, OperationResponse operationResponse) {
		this.connection = connection;
		this.operationResponse = operationResponse;
		this.pathsHandler = this.connection.getPathsHandler();

	}

	/**
	 * Process multi input.
	 *
	 * @param updateRequest the update request
	 */
	@SuppressWarnings("unchecked")
	public void processMultiInput(UpdateRequest updateRequest) {
		for (ObjectData input : updateRequest) {
			try {
				if(connection.isConnected())
				{
				this.processInput((T) input);
				}
				else {
				throw new ConnectorException("Lost Connectivity During Operation");
				}
			} catch (Exception e) {
				this.addApplicationErrorResult((T) input, e);
			}			
		}
	}

	/**
	 * Process input.
	 *
	 * @param var1 the var 1
	 */
	abstract void processInput(T var1);

	/**
	 * Adds the application error result.
	 *
	 * @param input the input
	 * @param e the e
	 */
	private void addApplicationErrorResult(T input, Exception e) {
		input.getLogger().log(Level.WARNING, e.getMessage(), e);
		String statusCode = ErrorResult.inferCode(e);
		OperationStatus status = ErrorResult.inferStatus(e);
		this.operationResponse.addResult(input, status, statusCode, e.getMessage(), null);
	}

	/**
	 * To full path.
	 *
	 * @param childPath the child path
	 * @return the string
	 */
	String toFullPath(String childPath) {
		return this.pathsHandler.resolvePaths(connection.getHomeDirectory(), childPath);
	}

	/**
	 * Extract remote dir and file name.
	 *
	 * @param input the input
	 * @return the SFTP file metadata
	 */
	SFTPFileMetadata extractRemoteDirAndFileName(ObjectIdData input) {
		return this.extractRemoteDirAndFileName((TrackedData) input, input.getObjectId());
	}

	/**
	 * Extract remote dir and file name.
	 *
	 * @param input the input
	 * @param filePath the file path
	 * @return the SFTP file metadata
	 */
	SFTPFileMetadata extractRemoteDirAndFileName(TrackedData input, String filePath) {
		String remoteDir = getRemoteDir(input);
		boolean fileExists = doesFileExists(input, remoteDir);

		SFTPFileMetadata fileMetadata = this.pathsHandler.splitIntoDirAndFileName(remoteDir, filePath);
		if (!fileExists) {
			throw new SFTPSdkException(SFTPConstants.ERROR_REMOTE_DIRECTORY_NOT_FOUND);
		}

		return fileMetadata;
	}

	/**
	 * @param input input
	 * @param remoteDir path to remote directory
	 * @return boolean if file exists
	 */
	boolean doesFileExists(TrackedData input, String remoteDir) {
		RetryableVerifyFileExistsAction fileExistsAction = new RetryableVerifyFileExistsAction(connection, remoteDir,
				input);
		fileExistsAction.execute();
		return fileExistsAction.getFileExists();
	}

	/**
	 * @param input input
	 * @return path to remote directory
	 */
	String getRemoteDir(TrackedData input) {
		String enteredRemoteDir = this.connection.getEnteredRemoteDirectory(input);
		String remoteDir;
		if(StringUtil.isBlank(enteredRemoteDir) || !pathsHandler.isFullPath(enteredRemoteDir)) {
			 input.getLogger().log(Level.INFO,
						"Entered remote directory path is blank or not a absolute full path.Setting home directory of user as default working path");
			remoteDir=this.toFullPath(enteredRemoteDir);
		}
		else {
			remoteDir=enteredRemoteDir;
		}
		return remoteDir;
	}
}
