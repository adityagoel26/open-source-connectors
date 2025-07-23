//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.actions.RetryableGetFileMetadataAction;
import com.boomi.connector.sftp.common.ExtendedSFTPFileMetadata;
import com.boomi.connector.sftp.common.MeteredTempOutputStream;
import com.boomi.connector.sftp.common.PathsHandler;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.common.SFTPFileMetadata;
import com.boomi.connector.sftp.common.UnixPathsHandler;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.results.EmptySuccess;
import com.boomi.connector.sftp.results.Result;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.SftpATTRS;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static com.boomi.connector.sftp.constants.SFTPConstants.CONNECTION_PARAM_CONN_TIMEOUT;
import static com.boomi.connector.sftp.constants.SFTPConstants.CONNECTION_PARAM_READ_TIMEOUT;
import static com.boomi.connector.sftp.constants.SFTPConstants.DEFAULT_CONNECTION_TIMEOUT;
import static com.boomi.connector.sftp.constants.SFTPConstants.DEFAULT_READ_TIMEOUT;

/**
 * The Class SFTPConnection.
 *
 * @author Omesh Deoli
 * 
 * 
 */
@SuppressWarnings("rawtypes")
public class SFTPConnection extends BaseConnection {

	/** The paths handler. */
	private final PathsHandler pathsHandler;

	/** The client. */
	private SFTPClient client;

	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public SFTPClient getClient() {
		return client;
	}

	/** The remote directory. */
	private final String remoteDirectory;

	/**
	 * Gets the paths handler.
	 *
	 * @return the paths handler
	 */
	public PathsHandler getPathsHandler() {
		return pathsHandler;
	}

	/**
	 * Instantiates a new SFTP connection.
	 *
	 * @param context the context
	 */
	@SuppressWarnings("unchecked")
	public SFTPConnection(BrowseContext context) {
		super(context);
		this.pathsHandler = new UnixPathsHandler();
		this.remoteDirectory = StringUtil
				.trim(context.getConnectionProperties().getProperty(SFTPConstants.REMOTE_DIRECTORY));

	}

	/**
	 * Open connection.
	 */
	public void openConnection() {
		this.client = createSftpClient(new PropertiesUtil(this.getContext().getConnectionProperties()));
		this.client.openConnection();
	}

	/**
	 * Creates the sftp client.
	 *
	 * @param propertiesUtil the properties util
	 * @return the SFTP client
	 */
	public SFTPClient createSftpClient(PropertiesUtil propertiesUtil) {
		int connectionTimeout = getContext().getConnectionProperties()
				.getLongProperty(CONNECTION_PARAM_CONN_TIMEOUT, Long.valueOf(DEFAULT_CONNECTION_TIMEOUT)).intValue();
		int readTimeout = getContext().getConnectionProperties()
				.getLongProperty(CONNECTION_PARAM_READ_TIMEOUT, Long.valueOf(DEFAULT_READ_TIMEOUT)).intValue();
		if (propertiesUtil.getAuthType().equals(AuthType.PUBLIC_KEY.getAuthType())) {
			PublicKeyParam param;
			if (!propertiesUtil.isUseKeyContentEnabled()) {
				param = new PublicKeyParam(propertiesUtil.getpassphrase(), propertiesUtil.getprvtkeyPath(),
						propertiesUtil.getUsername(), propertiesUtil.isUseKeyContentEnabled());
			} else {
				param = new PublicKeyParam(propertiesUtil.getUsername(), propertiesUtil.getpassphrase(),
						propertiesUtil.getPrivateKeyContent(), propertiesUtil.getPublicKeyContent(),
						propertiesUtil.getKeyPairName(), propertiesUtil.isUseKeyContentEnabled());
			}
			return new SFTPClient(propertiesUtil.getHostname(), propertiesUtil.getPort(), AuthType.PUBLIC_KEY, param,
					propertiesUtil, connectionTimeout, readTimeout, getContext());
		} else {
			PasswordParam passwordParam = new PasswordParam(propertiesUtil.getUsername(), propertiesUtil.getPassword());
			return new SFTPClient(propertiesUtil.getHostname(), propertiesUtil.getPort(), AuthType.PASSWORD,
					passwordParam, propertiesUtil, connectionTimeout, readTimeout, getContext());
		}
	}

	/**
	 * Checks if is connected.
	 *
	 * @return true, if is connected
	 */
	public boolean isConnected() {
		return this.client.isConnected();
	}

	/**
	 * Close connection.
	 */
	public void closeConnection() {
		client.closeConnection();
	}

	/**
	 * Gets the doc or operation property.
	 *
	 * @param input    the input
	 * @param propName the prop name
	 * @return the doc or operation property
	 */
	@SuppressWarnings("deprecation")
	public String getDocOrOperationProperty(TrackedData input, String propName) {
		String propertyValue = SFTPUtil.getDocProperty(input, propName);
		if (StringUtil.isNotBlank(propertyValue)) {
			return propertyValue;
		}
		return StringUtil.trim(this.getOperationContext().getOperationProperties().getProperty(propName));
	}

	/**
	 * Rename file.
	 *
	 * @param fullPath    the full path
	 * @param newFullPath the new full path
	 */
	public void renameFile(String fullPath, String newFullPath) {

		client.renameFile(fullPath, newFullPath);

	}

	/**
	 * Put file.
	 *
	 * @param content  the content
	 * @param filePath the file path
	 * @param mode     the mode
	 */
	public void putFile(InputStream content, String filePath, int mode) {

		client.putFile(content, filePath, mode);

	}

	/**
	 * Checks if is file present.
	 *
	 * @param filePath the file path
	 * @return true, if is file present
	 */
	public boolean isFilePresent(String filePath) {

		return client.isFilePresent(filePath);
	}

	/**
	 * Delete file.
	 *
	 * @param filePath the file path
	 */
	public void deleteFile(String filePath) {

		client.deleteFile(filePath);

	}

	/**
	 * Delete file.
	 *
	 * @param input the input
	 * @return the result
	 */
	public Result deleteFile(ObjectIdData input) {
		String fileFullPath = getFileFullPath(input);
		deleteFile(fileFullPath);
		return new EmptySuccess();
	}

	/**
	 * Gets the file full path.
	 *
	 * @param input the input
	 * @return the file full path
	 */
	private String getFileFullPath(ObjectIdData input) {

		String enteredFilePath = input.getObjectId();
		if (StringUtil.isBlank(enteredFilePath)) {
			throw new ConnectorException(SFTPConstants.ERROR_MISSING_INPUT_FILENAME);
		}

		String enteredRemoteDir = getEnteredRemoteDirectory(input);
		String remoteDir;
		if (StringUtil.isBlank(enteredRemoteDir) || !pathsHandler.isFullPath(enteredRemoteDir)) {
			input.getLogger().log(Level.INFO,
					"Entered remote directory path is blank or not a absolute full path.Setting home directory of user as default working path");
			remoteDir = pathsHandler.resolvePaths(client.getHomeDirectory(), enteredRemoteDir);
		} else {
			remoteDir = enteredRemoteDir;
		}

		return pathsHandler.joinPaths(remoteDir, enteredFilePath);

	}

	/**
	 * Gets the file stream.
	 *
	 * @param fullFilePath the full file path
	 * @return the file stream
	 */
	public InputStream getFileStream(String fullFilePath) {

		return client.getFileStream(fullFilePath);

	}


	/**
	 * Creates the directory.
	 *
	 * @param filePath the file path
	 */
	public void createDirectory(String filePath) {
		client.createDirectory(filePath);

	}

	/**
	 * Creates the nested directories.
	 *
	 * @param fullPath the full path
	 */
	public void createNestedDirectories(String fullPath) {
		client.createNestedDirectory(fullPath);

	}

	/**
	 * Gets the current directory.
	 *
	 * @return the current directory
	 */
	public String getCurrentDirectory() {

		return client.getCurrentDirectory();

	}

	/**
	 * Gets the entered remote directory.
	 *
	 * @param input the input
	 * @return the entered remote directory
	 */
	public String getEnteredRemoteDirectory(TrackedData input) {

		String dirPath = SFTPUtil.getDocProperty(input, SFTPConstants.REMOTE_DIRECTORY);
		Map<String, String> dynamicProperties = input.getDynamicProperties();
		String propertyValue = dynamicProperties.get(SFTPConstants.REMOTE_DIRECTORY);
		String remotedir = StringUtil.isBlank(remoteDirectory) ? propertyValue: this.remoteDirectory;
		return StringUtil.defaultIfBlank(dirPath, remotedir);
	}

	/**
	 * Change current directory.
	 *
	 * @param remoteDir the remote dir
	 */
	public void changeCurrentDirectory(String remoteDir) {
		client.changeCurrentDirectory(remoteDir);

	}

	/**
	 * Find unique file name.
	 *
	 * @param enteredFileName the entered file name
	 * @param remoteDir       the remote dir
	 * @return the string
	 */
	public String findUniqueFileName(String enteredFileName, String remoteDir) {
		return client.getUniqueFileName(remoteDir, enteredFileName);
	}

	/**
	 * File exists.
	 *
	 * @param enteredFileName the entered file name
	 * @param remoteDir       the remote dir
	 * @return true, if successful
	 */
	public boolean fileExists(String enteredFileName, String remoteDir) {

		return client.isFilePresent(pathsHandler.joinPaths(remoteDir, enteredFileName));
	}

	/**
	 * File exists.
	 *
	 * @param fullPath the full path
	 * @return true, if successful
	 */
	public boolean fileExists(String fullPath) {

		return client.isFilePresent(fullPath);
	}

	/**
	 * Gets the file metadata.
	 *
	 * @param dirFullPath the dir full path
	 * @param fileName    the file name
	 * @return the file metadata
	 */
	public ExtendedSFTPFileMetadata getFileMetadata(String dirFullPath, String fileName) {

		RetryableGetFileMetadataAction retryableMetadataaction = new RetryableGetFileMetadataAction(this, dirFullPath,
				fileName);
		retryableMetadataaction.execute();
		String formattedDate = SFTPUtil
				.formatDate(SFTPUtil.parseDate(retryableMetadataaction.getFileMetaData().getMTime()));

		return new ExtendedSFTPFileMetadata(dirFullPath, fileName, formattedDate);
	}

	/**
	 * Make filter.
	 *
	 * @param input       the input
	 * @param filesOnly   the files only
	 * @param dirFullPath the dir full path
	 * @param dirPath     the dir path
	 * @return the file query filter
	 */
	public FileQueryFilter makeFilter(FilterData input, boolean filesOnly, File dirFullPath, String dirPath) {
		if (filesOnly) {
			return new FileExclusiveQueryFilter(dirFullPath, input, dirPath);
		}
		return new FileQueryFilter(dirFullPath, input, dirPath);
	}

	/**
	 * Gets the limit.
	 *
	 * @return the limit
	 */
	public long getLimit() {
		@SuppressWarnings("deprecation")
		long limit = this.getOperationContext().getOperationProperties().getLongProperty(SFTPConstants.COUNT);
		if (limit == -1L) {
			return Long.MAX_VALUE;
		}
		if (limit <= 0L) {
			throw new IllegalArgumentException(String.format(SFTPConstants.LIMIT_MUST_BE_POSITIVE, limit));
		}
		return limit;
	}

	/**
	 * Gets the home directory.
	 *
	 * @return the home directory
	 */
	public String getHomeDirectory() {

		return client.getHomeDirectory();
	}

	/**
	 * Reconnect.
	 */
	public void reconnect() {
		closeConnection();
		this.openConnection();
	}

	/**
	 * Upload file.
	 *
	 * @param filePath     the file path
	 * @param inputStream  the input stream
	 * @param appendOffset the append offset
	 */
	public void uploadFile(String filePath, InputStream inputStream, long appendOffset) {
		long actualTransferredBytes;
		SFTPFileMetadata ftpFileMetadata = this.pathsHandler.splitIntoDirAndFileName(filePath);
		actualTransferredBytes = this.getSizeOnRemote(ftpFileMetadata.getDirectory(), ftpFileMetadata.getName());
		if (appendOffset > 0L) {
			actualTransferredBytes -= appendOffset;
		}

		SFTPConnection.adjustInputStreamToResumeUpload(inputStream, actualTransferredBytes);

		this.client.putFile(inputStream, filePath, ChannelSftp.APPEND);

	}

	/**
	 * Adjust input stream to resume upload.
	 *
	 * @param inputStream      the input stream
	 * @param transferredBytes the transferred bytes
	 */
	private static void adjustInputStreamToResumeUpload(InputStream inputStream, long transferredBytes) {
		try {
			inputStream.reset();
			StreamUtil.skipFully(inputStream, transferredBytes);
		} catch (IOException e) {
			throw new ConnectorException(SFTPConstants.ERROR_RESUMING_UPLOAD, e);
		}
	}

	/**
	 * Gets the size on remote.
	 *
	 * @param directory the directory
	 * @param name      the name
	 * @return the size on remote
	 */
	public long getSizeOnRemote(String directory, String name) {
		boolean filePresent = client.isFilePresent(pathsHandler.joinPaths(directory, name));
		if (filePresent) {
			return client.getFileMetadata(pathsHandler.joinPaths(directory, name)).getSize();
		} else {
			return 0L;
		}
	}

	/**
	 * Gets the file.
	 *
	 * @param filePath     the file name
	 * @param outputStream the output stream
	 * @return the file
	 */
	public void getFile(String filePath, MeteredTempOutputStream outputStream) {
		long noOfBytestoskip = outputStream.getCount();
		this.client.retrieveFile(filePath, outputStream, noOfBytestoskip);

	}

	/**
	 * Gets the file.
	 *
	 * @param filePath     the file name
	 * @param outputStream the output stream
	 * @param client       the client
	 * @param fileName 
	 * @return the file
	 */
	public void getFile(String filePath, MeteredTempOutputStream outputStream, SFTPClient client) {
		long noOfBytestoskip = outputStream.getCount();
		client.retrieveFile(filePath, outputStream, noOfBytestoskip);

	}

	/**
	 * Gets the single file attributes.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName  the file name
	 * @return the single file attributes
	 */
	public SftpATTRS getSingleFileAttributes(String remoteDir, String fileName) {
		return client.getFileMetadata(pathsHandler.joinPaths(remoteDir, fileName));

	}

	/**
	 * Delete file.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName  the file name
	 */
	public void deleteFile(String remoteDir, String fileName) {
		client.deleteFile(pathsHandler.joinPaths(remoteDir, fileName));

	}

	/**
	 * List directory content with selector.
	 * @param dirfullPath the dirfull path
	 * @param selector    the selector
	 */
	public void listDirectoryContentWithSelector(String dirfullPath, LsEntrySelector selector) {
		client.listDirectoryContentWithSelector(dirfullPath, selector);
	}
	
	/**
	 * List directory content.
	 *
	 * @param directoryFullPath the directory full path
	 * @return the list
	 */
	public List<LsEntry> listDirectoryContent(String directoryFullPath) {
		return client.listDirectoryContent(directoryFullPath);
	}

	/**
	 * Test connection.
	 */
	public void testConnection() {
		openConnection();
		closeConnection();
	}
}
