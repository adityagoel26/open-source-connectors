//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.handlers;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.ActionIfFileExists;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.SFTPUtil;
import com.boomi.connector.sftp.actions.RetryableDeleteFileAtPathAction;
import com.boomi.connector.sftp.actions.RetryableFindSizeOnRemote;
import com.boomi.connector.sftp.actions.RetryableFindUniqueFilenameAction;
import com.boomi.connector.sftp.actions.RetryableRenameFileAction;
import com.boomi.connector.sftp.actions.RetryableUploadFileAction;
import com.boomi.connector.sftp.actions.RetryableVerifyFileExistsAction;
import com.boomi.connector.sftp.common.ExtendedSFTPFileMetadata;
import com.boomi.connector.sftp.common.SFTPFileMetadata;
import com.boomi.connector.sftp.common.SimpleSFTPFileMetadata;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.NumberUtil;
import com.boomi.util.ObjectUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.logging.Level;

/**
 * The Class UploadHandler.
 *
 * @author Omesh Deoli
 * 
 *
 */
public class UploadHandler extends BaseMultiInputHandler<ObjectData> {

	/** The action if file exists. */
	private final ActionIfFileExists actionIfFileExists;

	/** The include all metadata. */
	private final boolean includeAllMetadata;
	/** The upload retry factory. */
	private final RetryStrategyFactory uploadRetryFactory;

	/**
	 * Instantiates a new upload handler.
	 *
	 * @param connection        the connection
	 * @param operationResponse the operation response
	 */
	public UploadHandler(SFTPConnection connection, OperationResponse operationResponse) {
		super(connection, operationResponse);
		@SuppressWarnings("deprecation")
		PropertyMap opProperties = this.connection.getOperationContext().getOperationProperties();
		this.includeAllMetadata = UploadHandler.mustIncludeAllMetadata(this.connection);
		this.actionIfFileExists = NumberUtil.toEnum(ActionIfFileExists.class,
				opProperties.getProperty(SFTPConstants.OPERATION_PROP_ACTION_IF_FILE_EXISTS), ActionIfFileExists.ERROR);
		this.uploadRetryFactory = RetryStrategyFactory.createFactory(1);
	}

	/**
	 * Gets the and validate normalized paths.
	 *
	 * @param input the input
	 * @return the and validate normalized paths
	 */
	public UploadPaths getAndValidateNormalizedPaths(TrackedData input) {
		String enteredFilePath = SFTPUtil.getDocProperty(input, SFTPConstants.PROPERTY_FILENAME);
		if (StringUtil.isBlank(enteredFilePath)) {
			throw new SFTPSdkException(SFTPConstants.ERROR_MISSING_INPUT_FILENAME);
		}
		SFTPFileMetadata fileMetadata = this.extractRemoteDirAndFileName(input, enteredFilePath);
		String remoteDir = fileMetadata.getDirectory();
		String enteredFileName = fileMetadata.getName();
		String stagingDir = this.extractStagingDirectory(input, remoteDir);
		String finalFileName = this.extractFinalFileName(enteredFileName, remoteDir, input);
		String tempFileName = this.extractTempFileName(input, remoteDir, stagingDir, enteredFileName, finalFileName);
		return new UploadPaths(remoteDir, stagingDir, finalFileName, tempFileName);
	}

	/**
	 * Extract staging directory.
	 *
	 * @param input     the input
	 * @param remoteDir the remote dir
	 * @return the string
	 */
	private String extractStagingDirectory(TrackedData input, String remoteDir) {
		if (this.actionIfFileExists == ActionIfFileExists.APPEND) {
			return remoteDir;
		}
		String stagingDir = this.connection.getDocOrOperationProperty(input, SFTPConstants.PROPERTY_STAGING_DIRECTORY);
		if (StringUtil.isBlank(stagingDir)) {
			return remoteDir;
		}

		if (!pathsHandler.isFullPath(stagingDir)) {
			input.getLogger().log(Level.INFO,
					"Entered Staging directory path is not a absolute full path.Setting home directory of user as default working path");
			stagingDir = this.toFullPath(stagingDir);
		}
		RetryableVerifyFileExistsAction fileExistsAction = new RetryableVerifyFileExistsAction(connection, stagingDir,
				input);
		fileExistsAction.execute();
		boolean fileExists = fileExistsAction.getFileExists();

		if (!fileExists) {
			throw new SFTPSdkException(SFTPConstants.ERROR_STAGING_DIR_NOT_FOUND);
		}

		return stagingDir;
	}

	/**
	 * Parameter replacement.
	 *
	 * @param text            the text
	 * @param enteredFileName the entered file name
	 * @return the string
	 */
	private String parameterReplacement(String text, String enteredFileName) {
		String enteredFileNameBase = enteredFileName;
		String enteredFileNameExtension = "";
		int extensionIndex = enteredFileName.lastIndexOf(SFTPConstants.EXTENSION_SEPARATOR);
		if (extensionIndex != -1) {
			enteredFileNameBase = enteredFileName.substring(0, extensionIndex);
			enteredFileNameExtension = enteredFileName.substring(extensionIndex + 1);
		}
		text = text.replace(SFTPConstants.PARAMETER_BASE, enteredFileNameBase);
		text = text.replace(SFTPConstants.PARAMETER_EXTENSION, enteredFileNameExtension);
		text = text.replace(SFTPConstants.PARAMETER_UUID, UUID.randomUUID().toString());
		text = text.replace(SFTPConstants.PARAMETER_TIME,
				DateTimeFormatter.ofPattern(SFTPConstants.DEFAULT_TIME_FOMRAT).format(LocalDateTime.now()));
		text = text.replace(SFTPConstants.PARAMETER_DATE,
				DateTimeFormatter.ofPattern(SFTPConstants.DEFAULT_DATE_FORMAT).format(LocalDateTime.now()));
		return text;
	}

	/**
	 * Extract temp file name.
	 *
	 * @param input           the input
	 * @param remoteDir       the remote dir
	 * @param stagingDir      the staging dir
	 * @param enteredFileName the entered file name
	 * @param finalFileName   the final file name
	 * @return the string
	 */
	@SuppressWarnings("deprecation")
	private String extractTempFileName(TrackedData input, String remoteDir, String stagingDir, String enteredFileName,
			String finalFileName) {
		if (ObjectUtil.equals(actionIfFileExists, ActionIfFileExists.APPEND)) {
			return finalFileName;
		}
		String tempExtension = this.connection.getDocOrOperationProperty(input, SFTPConstants.PROPERTY_TEMP_EXTENSION);
		String tempFileName = this.connection.getOperationContext().getOperationProperties()
				.getProperty(SFTPConstants.PROPERTY_TEMP_FILE_NAME);
		boolean tempExtensionSpecified = StringUtil.isNotBlank(tempExtension);
		boolean tempFileNameSpecified = StringUtil.isNotBlank(tempFileName);
		if (ObjectUtil.equals(remoteDir, stagingDir) && !tempFileNameSpecified && !tempExtensionSpecified
				&& !actionIfFileExists.equals(ActionIfFileExists.OVERWRITE)) {
			return finalFileName;
		}

		if (tempExtensionSpecified) {
			tempExtension = UploadHandler.assureExtensionStartsWithExtensionSeparator(tempExtension);
			return enteredFileName + UUID.randomUUID() + tempExtension;
		}

		if (tempFileNameSpecified && ObjectUtil.equals(actionIfFileExists, ActionIfFileExists.FORCE_UNIQUE_NAMES)) {
			return parameterReplacement(tempFileName, enteredFileName);
		}

		return enteredFileName;
	}

	/**
	 * Assure extension starts with extension separator.
	 *
	 * @param extension the extension
	 * @return the string
	 */
	private static String assureExtensionStartsWithExtensionSeparator(String extension) {
		return StringUtil.startsWith(extension, SFTPConstants.EXTENSION_SEPARATOR) ? extension
				: (SFTPConstants.EXTENSION_SEPARATOR + extension);
	}

	/**
	 * Must rename file.
	 *
	 * @param tempFileFullPath  the temp file full path
	 * @param finalFileFullPath the final file full path
	 * @return true, if successful
	 */
	private static boolean mustRenameFile(String tempFileFullPath, String finalFileFullPath) {
		return !ObjectUtil.equals(tempFileFullPath, finalFileFullPath);
	}

	/**
	 * Extract final file name.
	 *
	 * @param enteredFileName the entered file name
	 * @param remoteDir       the remote dir
	 * @param input           the input
	 * @return the string
	 */
	@SuppressWarnings("deprecation")
	private String extractFinalFileName(String enteredFileName, String remoteDir, TrackedData input) {

		if (ObjectUtil.equals(this.actionIfFileExists, ActionIfFileExists.FORCE_UNIQUE_NAMES)) {
			String targetFileName = this.connection.getOperationContext().getOperationProperties()
					.getProperty(SFTPConstants.PROPERTY_TARGET_FILE_NAME);
			if (StringUtil.isNotBlank(targetFileName)) {
				enteredFileName = parameterReplacement(targetFileName, enteredFileName);
			}

			RetryableFindUniqueFilenameAction uniqueFilenameAction = new RetryableFindUniqueFilenameAction(connection,
					enteredFileName, remoteDir, input);
			uniqueFilenameAction.execute();
			return uniqueFilenameAction.getUniqueFileName();

		}

		RetryableVerifyFileExistsAction fileExistsAction = new RetryableVerifyFileExistsAction(connection, remoteDir,
				enteredFileName);
		fileExistsAction.execute();

		if (fileExistsAction.getFileExists() && ObjectUtil.equals(this.actionIfFileExists, ActionIfFileExists.ERROR)) {
			String errorMessage = MessageFormat.format(SFTPConstants.ERROR_FILE_ALREADY_EXISTS_FORMAT, enteredFileName);
			throw new SFTPSdkException(errorMessage);
		}
		return enteredFileName;
	}

	/**
	 * Gets the SFTP file metadata.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName  the file name
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
	 * @param fileName  the file name
	 * @return the SFTP file metadata from remote
	 */
	private ExtendedSFTPFileMetadata getSFTPFileMetadataFromRemote(String remoteDir, String fileName) {
		return this.connection.getFileMetadata(remoteDir, fileName);
	}

	/**
	 * Must include all metadata.
	 *
	 * @param connection the connection
	 * @return true, if successful
	 */
	private static boolean mustIncludeAllMetadata(SFTPConnection connection) {
		JsonNode jsonCookie;
		@SuppressWarnings("deprecation")
		String cookie = connection.getOperationContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
		if (cookie == null) {
			throw new IllegalStateException(SFTPConstants.ERROR_INVALID_COOKIE);
		}
		try {
			jsonCookie = JSONUtil.getDefaultObjectMapper().readTree(cookie);
		} catch (IOException e) {
			throw new IllegalStateException(SFTPConstants.ERROR_INVALID_COOKIE, e);
		}
		return jsonCookie.path("includeAllMetadata").asBoolean();
	}

	/**
	 * The Class UploadPaths.
	 */
	public static final class UploadPaths {

		/** The remote dir full path. */
		private final String remoteDirFullPath;

		/** The staging dir full path. */
		private final String stagingDirFullPath;

		/** The final file name. */
		private final String finalFileName;

		/** The temp file name. */
		private final String tempFileName;

		/**
		 * Instantiates a new upload paths.
		 *
		 * @param remoteDirFullPath  the remote dir full path
		 * @param stagingDirFullPath the staging dir full path
		 * @param finalFileName      the final file name
		 * @param tempFileName       the temp file name
		 */
		UploadPaths(String remoteDirFullPath, String stagingDirFullPath, String finalFileName, String tempFileName) {
			this.remoteDirFullPath = remoteDirFullPath;
			this.stagingDirFullPath = stagingDirFullPath;
			this.finalFileName = finalFileName;
			this.tempFileName = tempFileName;
		}

		/**
		 * Gets the remote dir full path.
		 *
		 * @return the remote dir full path
		 */
		public String getRemoteDirFullPath() {
			return this.remoteDirFullPath;
		}

		/**
		 * Gets the staging dir full path.
		 *
		 * @return the staging dir full path
		 */
		public String getStagingDirFullPath() {
			return this.stagingDirFullPath;
		}

		/**
		 * Gets the final file name.
		 *
		 * @return the final file name
		 */
		public String getFinalFileName() {
			return this.finalFileName;
		}

		/**
		 * Gets the temp file name.
		 *
		 * @return the temp file name
		 */
		public String getTempFileName() {
			return this.tempFileName;
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
			String stagingDir = uploadPaths.getStagingDirFullPath();
			String tempFileName = uploadPaths.getTempFileName();
			String tempFileFullPath = this.pathsHandler.joinPaths(stagingDir, tempFileName);
			long appendOffset = 0L;
			String finalFileFullPath = this.pathsHandler.joinPaths(remoteDir, finalFileName);
			if (actionIfFileExists.equals(ActionIfFileExists.APPEND)) {
				RetryableFindSizeOnRemote findSizeAction = new RetryableFindSizeOnRemote(connection, remoteDir,
						finalFileName,input);
				findSizeAction.execute();
				appendOffset = findSizeAction.getFileSize();
			}
			if (!UploadHandler.mustRenameFile(tempFileFullPath, finalFileFullPath)
					&& actionIfFileExists.equals(ActionIfFileExists.OVERWRITE)) {
				RetryableVerifyFileExistsAction fileExistsAction = new RetryableVerifyFileExistsAction(connection,
						finalFileFullPath,input);
				fileExistsAction.execute();
				if (fileExistsAction.getFileExists()) {
					RetryableDeleteFileAtPathAction retrydeleteAtPath = new RetryableDeleteFileAtPathAction(
							this.connection, finalFileFullPath,input);
					retrydeleteAtPath.execute();
				}
			}
			uploadAction = new RetryableUploadFileAction(this.connection, remoteDir, this.uploadRetryFactory,
					tempFileFullPath, input, appendOffset);
			uploadAction.execute();
			executeRetryRenameFile(input, tempFileFullPath, finalFileFullPath);
			SFTPFileMetadata fileMetadata = this.getSFTPFileMetadata(remoteDir, finalFileName);
			operationResponse.addResult((TrackedData) input, OperationStatus.SUCCESS, "0", SFTPConstants.FILE_CREATED,
					fileMetadata.toJsonPayload());
		} catch (Exception ex) {
			input.getLogger().info(ex.getMessage());
			throw ex;
		} finally {
			if (uploadAction != null) {
				uploadAction.close();
			}

		}
	}

	/**
	 * @param input the input
	 * @param tempFileFullPath full path to temp file
	 * @param finalFileFullPath full path to final file
	 */
	private void executeRetryRenameFile(ObjectData input, String tempFileFullPath, String finalFileFullPath) {
		if (UploadHandler.mustRenameFile(tempFileFullPath, finalFileFullPath)) {
			if (actionIfFileExists.equals(ActionIfFileExists.OVERWRITE)) {
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
}
