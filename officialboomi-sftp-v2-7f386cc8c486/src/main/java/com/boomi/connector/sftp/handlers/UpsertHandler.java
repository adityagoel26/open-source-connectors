//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.handlers;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.SFTPUtil;
import com.boomi.connector.sftp.actions.RetryableCreateDirectoryAction;
import com.boomi.connector.sftp.actions.RetryableDeleteFileAtPathAction;
import com.boomi.connector.sftp.actions.RetryableFindSizeOnRemote;
import com.boomi.connector.sftp.actions.RetryableRenameFileAction;
import com.boomi.connector.sftp.actions.RetryableUploadFileAction;
import com.boomi.connector.sftp.actions.RetryableVerifyFileExistsAction;
import com.boomi.connector.sftp.common.ExtendedSFTPFileMetadata;
import com.boomi.connector.sftp.common.SFTPFileMetadata;
import com.boomi.connector.sftp.common.SimpleSFTPFileMetadata;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.ObjectUtil;
import com.boomi.util.StringUtil;

import java.io.IOException;
import java.util.UUID;

/**
 * The Class UpsertHandler.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class UpsertHandler extends BaseMultiInputHandler<ObjectData> {

	/** The include all metadata. */
	private final boolean includeAllMetadata;

	/** The is append enabled. */
	private final boolean isAppendEnabled;
	
	/** The create dir. */
	private final boolean createDir;
	
	/** The upload retry factory. */
	private final RetryStrategyFactory uploadRetryFactory;

	/**
	 * Instantiates a new upsert handler.
	 *
	 * @param connection the connection
	 * @param operationResponse the operation response
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public UpsertHandler(SFTPConnection connection, OperationResponse operationResponse) throws IOException {
		super(connection, operationResponse);
		@SuppressWarnings("deprecation")
		PropertyMap opProperties = this.connection.getOperationContext().getOperationProperties();
		this.includeAllMetadata = opProperties.getBooleanProperty(SFTPConstants.INCLUDE_ALL);
		this.isAppendEnabled = opProperties.getBooleanProperty(SFTPConstants.APPEND);
		this.createDir = opProperties.getBooleanProperty(SFTPConstants.CREATE_DIR);
		this.uploadRetryFactory = RetryStrategyFactory.createFactory(1);
	}

	/**
	 * Gets the and validate normalized paths.
	 *
	 * @param input the input
	 * @return the and validate normalized paths
	 */
	private UploadPaths getAndValidateNormalizedPaths(TrackedData input) {
		String enteredFilePath = SFTPUtil.getDocProperty(input, SFTPConstants.PROPERTY_FILENAME);
		if (StringUtil.isBlank(enteredFilePath)) {
			throw new ConnectorException(SFTPConstants.ERROR_MISSING_INPUT_FILENAME);
		}
		SFTPFileMetadata fileMetadata = this.extractRemoteDirAndFileName(input, enteredFilePath);
		String remoteDir = fileMetadata.getDirectory();
		String enteredFileName = fileMetadata.getName();

		return new UploadPaths(remoteDir, enteredFileName);
	}

	/**
	 * Extract temp file name.
	 *
	 * @param enteredFileName the entered file name
	 * @return the string
	 */
	private String extractTempFileName(String enteredFileName) {
		if (this.isAppendEnabled) {
			return enteredFileName;
		}
		return enteredFileName + UUID.randomUUID();
	}

	/**
	 * Must rename file.
	 *
	 * @param tempFileFullPath the temp file full path
	 * @param finalFileFullPath the final file full path
	 * @return true, if successful
	 */
	private static boolean mustRenameFile(String tempFileFullPath, String finalFileFullPath) {
		return !ObjectUtil.equals(tempFileFullPath, finalFileFullPath);
	}

	/**
	 * Gets the SFTP file metadata.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 * @return the SFTP file metadata
	 */
	private SFTPFileMetadata getSFTPFileMetadata(String remoteDir, String fileName) {
		if (!this.includeAllMetadata) {
			return new SimpleSFTPFileMetadata(remoteDir, fileName);
		}
		return this.getSFTPFileMetadataFromRemote(remoteDir, fileName);
	}

	/**
	 * Gets the SFTP file metadata from remote.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 * @return the SFTP file metadata from remote
	 */
	private ExtendedSFTPFileMetadata getSFTPFileMetadataFromRemote(String remoteDir, String fileName) {
		return this.connection.getFileMetadata(remoteDir, fileName);
	}

	/**
	 * The Class UploadPaths.
	 */
	private static final class UploadPaths {
		
		/** The remote dir full path. */
		private final String remoteDirFullPath;

		/** The final file name. */
		private final String finalFileName;

		/**
		 * Instantiates a new upload paths.
		 *
		 * @param remoteDirFullPath the remote dir full path
		 * @param finalFileName the final file name
		 */
		UploadPaths(String remoteDirFullPath, String finalFileName) {
			this.remoteDirFullPath = remoteDirFullPath;

			this.finalFileName = finalFileName;

		}

		/**
		 * Gets the remote dir full path.
		 *
		 * @return the remote dir full path
		 */
		String getRemoteDirFullPath() {
			return this.remoteDirFullPath;
		}

		/**
		 * Gets the final file name.
		 *
		 * @return the final file name
		 */
		String getFinalFileName() {
			return this.finalFileName;
		}

	}

	/**
	 * Extract remote dir and file name.
	 *
	 * @param input the input
	 * @param filePath the file path
	 * @return the SFTP file metadata
	 */
	@Override
	SFTPFileMetadata extractRemoteDirAndFileName(TrackedData input, String filePath) {
		String remoteDir = getRemoteDir(input);
		boolean fileExists = doesFileExists(input, remoteDir);
		if (createDir && !fileExists) {
			RetryableCreateDirectoryAction createDirectory = new RetryableCreateDirectoryAction(connection, remoteDir,
					input);
			createDirectory.execute();
			fileExists = true;
		}
		SFTPFileMetadata fileMetadata = this.pathsHandler.splitIntoDirAndFileName(remoteDir, filePath);
		if (!fileExists) {
			throw new SFTPSdkException(SFTPConstants.ERROR_REMOTE_DIRECTORY_NOT_FOUND);
		}
		return fileMetadata;
	}

	/**
	 * @param input the input
	 * @param tempFileFullPath full path to temp file
	 * @param finalFileFullPath full path to final file
	 */
	private void executeRetryRenameFile(ObjectData input, String tempFileFullPath, String finalFileFullPath) {
		if (UpsertHandler.mustRenameFile(tempFileFullPath, finalFileFullPath)) {
			if (!isAppendEnabled) {
				RetryableVerifyFileExistsAction fileExistsAction = new RetryableVerifyFileExistsAction(connection,
						finalFileFullPath, input);
				fileExistsAction.execute();
				if (fileExistsAction.getFileExists()) {
					RetryableDeleteFileAtPathAction retrydeleteAtPath = new RetryableDeleteFileAtPathAction(
							this.connection, finalFileFullPath, input);
					retrydeleteAtPath.execute();
				}
			}
			RetryableRenameFileAction retryRenameFile = new RetryableRenameFileAction(connection, tempFileFullPath,
					finalFileFullPath, input);
			retryRenameFile.execute();

		}
	}

	/**
	 * Process input.
	 *
	 * @param input the input
	 */
	@Override
	void processInput(ObjectData input) {

		String finalFileName;
		String remoteDir;
		RetryableUploadFileAction uploadAction = null;
		try {
			UploadPaths uploadPaths = this.getAndValidateNormalizedPaths((TrackedData) input);
			finalFileName = uploadPaths.getFinalFileName();
			remoteDir = uploadPaths.getRemoteDirFullPath();
			String tempFileName = extractTempFileName(finalFileName);
			String tempFileFullPath = this.pathsHandler.joinPaths(remoteDir, tempFileName);
			long appendOffset = 0L;
			if (isAppendEnabled) {
				RetryableFindSizeOnRemote findSizeAction = new RetryableFindSizeOnRemote(connection, remoteDir,
						finalFileName, input);
				findSizeAction.execute();
				appendOffset = findSizeAction.getFileSize();
			}
			uploadAction = new RetryableUploadFileAction(this.connection, remoteDir, this.uploadRetryFactory,
					tempFileFullPath, input, appendOffset);
			uploadAction.execute();
			String finalFileFullPath = this.pathsHandler.joinPaths(remoteDir, finalFileName);
			executeRetryRenameFile(input, tempFileFullPath, finalFileFullPath);
			SFTPFileMetadata fileMetadata = this.getSFTPFileMetadata(remoteDir, finalFileName);
			//Add result
			operationResponse.addResult((TrackedData) input, OperationStatus.SUCCESS, "0", SFTPConstants.FILE_CREATED,
					fileMetadata.toJsonPayload());
		}
		catch (Exception ex) {
			input.getLogger().info(ex.getMessage());
			throw ex;
		} finally {
			if (uploadAction != null) {
				uploadAction.close();
			}
		}
	}
}
