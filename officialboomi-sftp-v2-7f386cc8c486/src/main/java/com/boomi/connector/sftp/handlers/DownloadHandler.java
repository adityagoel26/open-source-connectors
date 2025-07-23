//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.handlers;

import static com.boomi.connector.sftp.constants.SFTPConstants.PROPERTY_INCLUDE_METADATA;

import java.io.InputStream;
import java.util.logging.Level;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.SFTPUtil;
import com.boomi.connector.sftp.actions.RetryableDeleteFileAtPathAction;
import com.boomi.connector.sftp.actions.RetryableGetFileMetadataAction;
import com.boomi.connector.sftp.actions.RetryableRetrieveFileAction;
import com.boomi.connector.sftp.common.GetSFTPFileMetadata;
import com.boomi.connector.sftp.common.PathsHandler;
import com.boomi.connector.sftp.common.SFTPFileMetadata;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.NoSuchFileFoundException;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.results.BaseResult;
import com.boomi.connector.sftp.results.EmptySuccess;
import com.boomi.connector.sftp.results.ErrorResult;
import com.boomi.connector.sftp.results.Result;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

/**
 * The Class DownloadHandler.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class DownloadHandler {

	/** The connection. */
	final SFTPConnection connection;

	/** The operation response. */
	final OperationResponse operationResponse;

	/** The paths handler. */
	final PathsHandler pathsHandler;

	/** The isdelete after enabled. */
	Boolean isdeleteAfterEnabled;

	/** The isfail delete after. */
	Boolean isfailDeleteAfter;

	/** The file metadata. */
	SFTPFileMetadata fileMetadata;

	/** The download retry factory. */
	private final RetryStrategyFactory downloadRetryFactory;

	/**
	 * Instantiates a new download handler.
	 *
	 * @param connection        the connection
	 * @param operationResponse the operation response
	 */
	public DownloadHandler(SFTPConnection connection, OperationResponse operationResponse) {

		this.connection = connection;
		this.operationResponse = operationResponse;
		this.pathsHandler = this.connection.getPathsHandler();
		@SuppressWarnings("deprecation")
		PropertyMap opProperties = this.connection.getOperationContext().getOperationProperties();
		isdeleteAfterEnabled = opProperties.getBooleanProperty(SFTPConstants.PROPERTY_DELETE_AFTER);
		isfailDeleteAfter = opProperties.getBooleanProperty(SFTPConstants.PROPERTY_FAIL_DELETE_AFTER);
		this.downloadRetryFactory = RetryStrategyFactory.createFactory(1);
	}

	/**
	 * Process input.
	 *
	 * @param input            the input
	 * @param operationContext
	 */
	public void processInput(ObjectIdData input, OperationContext operationContext) {

		Result res = null;
		InputStream inputStream = null;
		RetryableRetrieveFileAction getFileAction = null;
		try {
			DownloadPaths downloadPaths = this.getAndValidateNormalizedPaths((TrackedData) input);
			String fullFilePath = pathsHandler.joinPaths(downloadPaths.getRemoteDirFullPath(), fileMetadata.getName());
			getFileAction = new RetryableRetrieveFileAction(connection, downloadPaths.getRemoteDirFullPath(),
					fullFilePath, input, downloadRetryFactory);

			getFileAction.execute();
			PayloadMetadata metadata = operationResponse.createMetadata();
			metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, fileMetadata.getName());
			inputStream = getFileAction.getOutputStream().toInputStream();
			boolean includeAllMetadata = operationContext.getOperationProperties()
					.getBooleanProperty(PROPERTY_INCLUDE_METADATA, Boolean.FALSE);
			if (isdeleteAfterEnabled) {
				res = deleteFileInRemoteDir(fullFilePath, input, res, inputStream);
				if (res != null) {
					res.addToResponse(operationResponse, (TrackedData) input);
					return;
				}
			}
			if (!includeAllMetadata) {
				res = new BaseResult(PayloadUtil.toPayload(inputStream, metadata));
				res.addToResponse(operationResponse, (TrackedData) input);
			} else {
				RetryableGetFileMetadataAction retryableMetadataaction = new RetryableGetFileMetadataAction(connection,
						downloadPaths.getRemoteDirFullPath(), fileMetadata.getName());
				retryableMetadataaction.doExecute();
				String formattedDate = SFTPUtil
						.formatDate(SFTPUtil.parseDate(retryableMetadataaction.getFileMetaData().getMTime()));
				GetSFTPFileMetadata getFileMetadata = new GetSFTPFileMetadata(inputStream, fileMetadata.getName(),
						formattedDate);
				res = new BaseResult(PayloadUtil.toPayload(getFileMetadata.toJson(), metadata));
				res.addToResponse(operationResponse, (TrackedData) input);
			}
		} catch (SFTPSdkException e) {
			input.getLogger().log(Level.WARNING, e.getMessage());
			operationResponse.addErrorResult((TrackedData) input, OperationStatus.APPLICATION_ERROR, "400",
					e.getMessage(), (Throwable) e);
		} catch (NoSuchFileFoundException e) {
			input.getLogger().log(Level.WARNING, e.getMessage(), e);
			res = new EmptySuccess();
			res.addToResponse(operationResponse, (TrackedData) input);
		} catch (Exception e) {
			res = new ErrorResult(e);
			res.addToResponse(operationResponse, (TrackedData) input);
		} finally {
			IOUtil.closeQuietly(inputStream);
			IOUtil.closeQuietly(getFileAction);
		}
	}

	/**
	 * Delete file in remote dir.
	 *
	 * @param fullFilePath the full file path
	 * @param input        the input
	 * @param res          the res
	 * @param fileContent  the file content
	 * @return the result
	 */
	private Result deleteFileInRemoteDir(String fullFilePath, ObjectIdData input, Result res, InputStream fileContent) {
		try {
			RetryableDeleteFileAtPathAction deleteAtPath = new RetryableDeleteFileAtPathAction(connection, fullFilePath,
					input);
			deleteAtPath.execute();

		} catch (Exception e) {
			// log a warning message delete failure
			input.getLogger().log(Level.WARNING, SFTPConstants.UNABLE_TO_DELETE_FILE, e);
			// check if "Fail if unable to delete" checkbox is selected in operation tab
			// if selected, then mark the operation status as application error.
			if (isfailDeleteAfter) {
				res = new BaseResult(PayloadUtil.toPayload(fileContent), "-1", SFTPConstants.UNABLE_TO_DELETE_FILE,
						OperationStatus.APPLICATION_ERROR);

			}
		}
		return res;
	}

	/**
	 * The Class DownloadPaths.
	 */
	private static final class DownloadPaths {

		/** The remote dir full path. */
		private final String remoteDirFullPath;

		/**
		 * Instantiates a new download paths.
		 *
		 * @param remoteDirFullPath the remote dir full path
		 */
		DownloadPaths(String remoteDirFullPath) {
			this.remoteDirFullPath = remoteDirFullPath;

		}

		/**
		 * Gets the remote dir full path.
		 *
		 * @return the remote dir full path
		 */
		String getRemoteDirFullPath() {
			return this.remoteDirFullPath;
		}

	}

	/**
	 * Gets the and validate normalized paths.
	 *
	 * @param input the input
	 * @return the and validate normalized paths
	 */
	private DownloadPaths getAndValidateNormalizedPaths(TrackedData input) throws SFTPSdkException{
		String enteredFilePath = ((ObjectIdData) input).getObjectId();

		if (StringUtil.isBlank(enteredFilePath)) {
			throw new ConnectorException(SFTPConstants.ERROR_MISSING_INPUT_FILENAME_ID);
		}
		fileMetadata = this.extractRemoteDirAndFileName(input, enteredFilePath);
		String remoteDir = fileMetadata.getDirectory();

		return new DownloadPaths(remoteDir);
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
	 * @param input    the input
	 * @param filePath the file path
	 * @return the SFTP file metadata
	 */
	SFTPFileMetadata extractRemoteDirAndFileName(TrackedData input, String filePath) {
		String enteredRemoteDir = this.connection.getEnteredRemoteDirectory(input);
		String remoteDir;
		if (StringUtil.isBlank(enteredRemoteDir) || !pathsHandler.isFullPath(enteredRemoteDir)) {
			input.getLogger().log(Level.INFO,
					"Entered remote directory path is blank or not a absolute full path.Setting home directory of user as default working path");
			remoteDir = this.toFullPath(enteredRemoteDir);
		} else {
			remoteDir = enteredRemoteDir;
		}
		boolean fileExists = this.connection.fileExists(remoteDir);
		SFTPFileMetadata fileMetaData = this.pathsHandler.splitIntoDirAndFileName(remoteDir, filePath);
		if (!fileExists) {
			throw new SFTPSdkException(SFTPConstants.ERROR_REMOTE_DIRECTORY_NOT_FOUND);
		}
		return fileMetaData;
	}
}
