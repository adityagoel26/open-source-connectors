//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.common.MeteredTempOutputStream;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.FileNameNotSupportedException;
import com.boomi.connector.sftp.exception.NoSuchFileFoundException;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.util.StringUtil;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import java.io.Closeable;
import java.io.InputStream;
import java.nio.file.InvalidPathException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Class SFTPClient.
 *
 */
public class SFTPClient implements Closeable {

	private static final String SPECIAL_CHARACTERS = "[<>:\"/'\\[\\];,?*|!]";

	private static final String DIGIT_REGEX = "(\\d*)";

	private static final Pattern PATTERN = Pattern.compile(SPECIAL_CHARACTERS);

	/** The sftp channel. */
	private ChannelSftp sftpChannel;

	/** The con prop. */
	private ConnectionProperties conProp;

	/** The session. */
	private Session session;

	/** The properties. */
	private PropertiesUtil properties;

	/** The logger. */
	private Logger logger = Logger.getLogger(SFTPClient.class.getName());

	/**
	 * Instantiates a new SFTP client.
	 *
	 * @param host              the host
	 * @param port              the port
	 * @param authType          the auth type
	 * @param param             the public keyparam
	 * @param properties        the properties
	 * @param connectionTimeout the connection timeout
	 * @param readTimeout       the read timeout
	 * @param connectorContext  the connector context
	 */
	public SFTPClient(String host, int port, AuthType authType, PublicKeyParam param, PropertiesUtil properties,
			int connectionTimeout, int readTimeout, ConnectorContext connectorContext) {
		if (connectorContext.getConnectionProperties().getBooleanProperty(SFTPConstants.ENABLE_POOLING,false)) {
			PropertyMap propertyMap = connectorContext.getConnectionProperties();
			String key = host;
			for (Entry<String, Object> pair : propertyMap.entrySet()) {
				if (pair.getValue() != null && !pair.getKey().equalsIgnoreCase(SFTPConstants.PROPERTY_HOST)) {
					key += pair.getValue().toString();
				}
			}
			conProp = SFTPConnectionPool.getConnectionPropeties(new ConnectionProperties(host, port, authType, param,
					properties, connectionTimeout, readTimeout, connectorContext), key);
		} else {
			conProp = new ConnectionProperties(host, port, authType, param, properties, connectionTimeout, readTimeout,
					connectorContext);
		}

		this.properties = properties;

	}

	/**
	 * Instantiates a new SFTP client.
	 *
	 * @param host              the host
	 * @param port              the port
	 * @param authType          the auth type
	 * @param passwordParam     the password param
	 * @param properties        the properties
	 * @param connectionTimeout the connection timeout
	 * @param readTimeout       the read timeout
	 * @param connectorContext  the connector context
	 */
	public SFTPClient(String host, int port, AuthType authType, PasswordParam passwordParam, PropertiesUtil properties,
			int connectionTimeout, int readTimeout, ConnectorContext connectorContext) {
		if (connectorContext.getConnectionProperties().getBooleanProperty(SFTPConstants.ENABLE_POOLING,false)) {
			PropertyMap propertyMap = connectorContext.getConnectionProperties();
			String key = host;
			for (Entry<String, Object> pair : propertyMap.entrySet()) {
				if (pair.getValue() != null && !pair.getKey().equalsIgnoreCase(SFTPConstants.PROPERTY_HOST)) {
					key += pair.getValue().toString();
				}
			}
			conProp = SFTPConnectionPool.getConnectionPropeties(new ConnectionProperties(host, port, authType,
					passwordParam, properties, connectionTimeout, readTimeout, connectorContext), key);
		} else {
			conProp = new ConnectionProperties(host, port, authType, passwordParam, properties, connectionTimeout,
					readTimeout, connectorContext);
		}
		this.properties = properties;
	}

	/**
	 * Open connection.
	 */
	public void openConnection() {
		if (session == null || !session.isConnected()) {
			getSession();
		}
		if (sftpChannel == null || !sftpChannel.isConnected()) {
			getChannel();
		}
	}

	/**
	 * Gets the session.
	 *
	 * Modifications in this method are done due to CONC-1400 to regain the session.
	 * @return the session
	 */
	private void getSession() {
		try {
			if (conProp.getProperties().isPoolingEnabled()) {
				session = StackSessionPool.getInstance().getPool().borrowObject(conProp);

				if (!session.isConnected()){
					returnSession();
					StackSessionPool.getInstance().getPool().clear(conProp);
					session = StackSessionPool.getInstance().getPool().borrowObject(conProp);
				}
			}else {
				session = ManageSession.getSessionWithoutConnectionPooling(conProp);
			}
		} catch (Exception e) {
			returnSession();
			validateNullFields();
			if (e.getLocalizedMessage().contains(SFTPConstants.FILE_NOT_FOUND_EXCEPTION)) {
				throw new ConnectorException(SFTPConstants.FILE_NOT_FOUND_MESSAGE);
			} else if (e.getLocalizedMessage().contains(SFTPConstants.INVALID_PRIVATEKEY)) {
				throw new ConnectorException(SFTPConstants.INVALID_PRIVATEKEY_ERROR_MESSAGE);
			} else if (e.getLocalizedMessage().contains(SFTPConstants.ACCESS_DENIED)) {
				throw new ConnectorException(e);
			} else if (e.getLocalizedMessage().contains(SFTPConstants.AUTH_FAIL)) {
				throw new ConnectorException(SFTPConstants.AUTH_FAIL_ERROR_MESSAGE);
			} else if (e.getLocalizedMessage().contains(SFTPConstants.USER_AUTH_FAIL)) {
				throw new ConnectorException(SFTPConstants.USER_AUTH_FAIL_MESSAGE);
			} else if (e.getLocalizedMessage().contains(SFTPConstants.PORT)) {
				throw new ConnectorException(SFTPConstants.PORT_ERROR_MESSAGE);
			} else if (e.getLocalizedMessage().contains(SFTPConstants.UNKNOWN_HOST)) {
				throw new ConnectorException(SFTPConstants.UNKNOWN_HOST_ERROR_MESSAGE);
			} else {
				throw new ConnectorException("Unable to get Session", e.getMessage());
			}
		}

	}

	/**
	 * Method to check if the fields are empty or null and throw customized error
	 * messages.
	 */
	private void validateNullFields() {
		throwExceptionIfStringIsBlank(properties.getHostname(), SFTPConstants.HOSTNAME_BLANK);
		throwExceptionIfStringIsBlank(properties.getUsername(), SFTPConstants.USER_BLANK);

		if (SFTPConstants.USERNAME_AND_PASSWORD.equals(conProp.getAuth().getAuthType())) {
			throwExceptionIfStringIsBlank(properties.getPassword(), SFTPConstants.PASS_BLANK);
		} else {
			if (properties.isUseKeyContentEnabled()) {
				throwExceptionIfStringIsBlank(properties.getPrivateKeyContent(), SFTPConstants.PRIVATE_KEY_BLANK);
				throwExceptionIfStringIsBlank(properties.getPublicKeyContent(), SFTPConstants.PUBLIC_KEY_BLANK);
			} else {
				throwExceptionIfStringIsBlank(properties.getprvtkeyPath(), SFTPConstants.KEY_FILE_PATH);
			}
		}
	}

	/**
	 * Throws a connector exception with exceptionText if str is blank
	 * @param str the string to be evaluated
	 * @param exceptionText exception text
	 */
	private void throwExceptionIfStringIsBlank(String str, String exceptionText){
		if(StringUtil.isBlank(str)){
			throw new ConnectorException(exceptionText);
		}
	}

	/**
	 * Return session.
	 */
	public void returnSession() {
		if (session != null) {
			try {
				StackSessionPool.getInstance().getPool().returnObject(conProp, session);
			} catch (Exception e) {
				throw new ConnectorException("Unable to return Session", e.getLocalizedMessage());
			}
		}

	}

	/**
	 * Close connection.
	 */
	public void closeConnection() {
		killChannel();
		if (conProp.getProperties().isPoolingEnabled()) {
			returnSession();
		}else {
			ManageSession.killSession(session);
		}
	}

	/**
	 * Kill channel.
	 */
	private void killChannel() {
		if (this.sftpChannel != null && this.sftpChannel.isConnected()) {
			try {
				this.logger.fine(SFTPConstants.DISCONNECTING_FROM_SFTP_SERVER);
				this.sftpChannel.disconnect();
			} catch (Exception e) {
				this.logger.log(Level.FINE, SFTPConstants.ERROR_DISCONNECTING_CHANNEL, e);
			}
		}
	}

	/**
	 * Gets the channel.
	 *
	 * @return the channel
	 */
	public void getChannel() {
		try {
			Channel channel = session.openChannel(SFTPConstants.SFTP);
			channel.connect();
			this.sftpChannel = (ChannelSftp) channel;
		} catch (JSchException e) {
			closeConnection();
			throw new ConnectorException(SFTPConstants.ERROR_FAILED_OPENING_SFTP_CHANNEL + " after "
					+ conProp.getDefaultReadTimedOut() + "ms", e);
		}
	}

	/**
	 * Put file.
	 *
	 * @param content  the content
	 * @param filePath the file path
	 * @param mode     the mode
	 */
	public void putFile(InputStream content, String filePath, int mode) {

		try {
			sftpChannel.put(content, filePath, mode);
		} catch (SftpException e) {
			throw new SFTPSdkException(MessageFormat.format(SFTPConstants.ERROR_FAILED_FILE_UPLOAD, filePath)
					+ SFTPConstants.CAUSE + e.getMessage(), e);
		}

	}

	/**
	 * Rename file.
	 *
	 * @param filePath    the file path
	 * @param newFilePath the new file path
	 */
	public void renameFile(String filePath, String newFilePath) {

		try {
			sftpChannel.rename(filePath, newFilePath);
		} catch (SftpException e) {
			throw new SFTPSdkException(MessageFormat.format(SFTPConstants.ERROR_FAILED_RENAME, filePath, newFilePath)
					+ SFTPConstants.CAUSE + e.getMessage(), e);
		}
	}

	/**
	 * Checks if is file present.
	 *
	 * @param filePath the file path
	 * @return true, if is file present
	 */
	public boolean isFilePresent(String filePath) {

		try {
			sftpChannel.stat(filePath);
			return true;
		} catch (SftpException e) {
			if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
				return false;
			} else {
				throw new SFTPSdkException(
						MessageFormat.format(SFTPConstants.ERROR_FAILED_CHECKING_FILE_EXISTENCE, filePath)
								+ SFTPConstants.CAUSE + e.getMessage(), e);
			}
		}
	}

	/**
	 * Delete file.
	 *
	 * @param filePath the file path
	 */
	public void deleteFile(String filePath) {

		try {
			sftpChannel.rm(filePath);
		} catch (SftpException e) {
			if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
				throw new NoSuchFileFoundException(SFTPConstants.FILE_NOT_FOUND, e);
			}
			throw new SFTPSdkException(MessageFormat.format(SFTPConstants.ERROR_FAILED_FILE_REMOVAL, filePath)
					+ SFTPConstants.CAUSE + e.getMessage(), e);
		}

	}

	/**
	 * Gets the file stream.
	 *
	 * @param filePath the file path
	 * @return the file stream
	 */
	public InputStream getFileStream(String filePath) {

		try {
			return sftpChannel.get(filePath);
		} catch (SftpException e) {
			if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
				throw new NoSuchFileFoundException(SFTPConstants.FILE_NOT_FOUND, e);
			}
			throw new SFTPSdkException(MessageFormat.format(SFTPConstants.ERROR_FAILED_FILE_RETRIEVAL, filePath)
					+ SFTPConstants.CAUSE + e.getMessage(), e);
		}

	}

	/**
	 * Can retrieve.
	 *
	 * @param entry the entry
	 * @return true, if successful
	 */
	static boolean canRetrieve(ChannelSftp.LsEntry entry) {
		return !entry.getAttrs().isDir() && !entry.getAttrs().isLink() && !StringUtil.isBlank(entry.getFilename());
	}

	/**
	 * Checks if is numeric.
	 *
	 * @param str the str
	 * @return true, if is numeric
	 */
	boolean isNumeric(String str) {
		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) > '9' || str.charAt(i) < '0') {
				return false;
			}
		}
		return true;
	}

	/**
	 * Gets the unique file name.
	 *
	 * @param path          the path
	 * @param inputFile the inputfile name
	 * @return the unique file name
	 */
	public String getUniqueFileName(String path, String inputFile) {
		String uniqueFileName = inputFile;
		final String inputFileName = inputFile;
		String extension = "";
		int counter = -1;

		final String fileNameWithoutExtension = getFileNameExcludeExtension(inputFileName);

		boolean hasExtension = inputFileName.length() != fileNameWithoutExtension.length();

		String regExString = fileNameWithoutExtension + DIGIT_REGEX;
		if (hasExtension) {
			extension = inputFileName.substring(inputFileName.lastIndexOf('.'));
			regExString = regExString.concat(extension);
		}
		Pattern pattern = Pattern.compile(regExString);

		String directoryPath = path + SFTPConstants.FILE_SEPARATOR + fileNameWithoutExtension + SFTPConstants.ASTERISK + extension;
		List<LsEntry> files = listDirectoryContent(directoryPath);

		counter = getHighestFileNumber(counter, pattern, files);

		if (counter >= 0) {
			uniqueFileName = fileNameWithoutExtension + (counter + 1) + extension;
		}

		return uniqueFileName;
	}

	private static int getHighestFileNumber(int counter, Pattern pattern, List<LsEntry> files) {
		for (LsEntry entry : files) {
			String fileName = entry.getFilename();
			Matcher matcher = pattern.matcher(fileName);
			if (matcher.matches()) {
				String numberValue = matcher.group(1);
				int fileNumber = StringUtil.isNotBlank(numberValue) ? Integer.parseInt(numberValue) : 0;
				if (fileNumber > counter) {
					counter = fileNumber;
				}
			}
		}
		return counter;
	}

	private static String getFileNameExcludeExtension(String inputFileName) {
		int inputFileExtensionLastIndex = inputFileName.lastIndexOf(SFTPConstants.EXTENSION_SEPARATOR);
		int inpFileNameLength = (inputFileExtensionLastIndex != -1 && inputFileName.indexOf(
				SFTPConstants.EXTENSION_SEPARATOR) != 0) ? inputFileExtensionLastIndex : inputFileName.length();

		return inputFileName.substring(0, inpFileNameLength);
	}


	/**
	 * Creates the directory.
	 *
	 * @param filePath the file path
	 */
	public void createDirectory(String filePath) {
		try {
			sftpChannel.mkdir(filePath);
		} catch (SftpException e) {
			throw new SFTPSdkException(MessageFormat.format(SFTPConstants.ERROR_FAILED_DIRECTORY_CREATE, filePath)
					+ SFTPConstants.CAUSE + e.getMessage(), e);
		}
	}

	/**
	 * Gets the current directory.
	 *
	 * @return the current directory
	 */
	public String getCurrentDirectory() {
		try {
			return sftpChannel.pwd();
		} catch (SftpException e) {

			throw new SFTPSdkException(
					SFTPConstants.ERROR_FAILED_RETRIEVING_DIRECTORY + SFTPConstants.CAUSE + e.getMessage(), e);
		}

	}

	/**
	 * Checks if is connected.
	 *
	 * @return true, if is connected
	 */
	public boolean isConnected() {

		return sftpChannel != null && sftpChannel.isConnected() && !sftpChannel.isClosed() && session != null
				&& session.isConnected();

	}

	/**
	 * Change current directory.
	 *
	 * @param fullPath the full path
	 */
	public void changeCurrentDirectory(String fullPath) {
		try {
			sftpChannel.cd(fullPath);
		} catch (SftpException e) {
			throw new SFTPSdkException(
					MessageFormat.format(SFTPConstants.ERROR_FAILED_CHANGING_CURRENT_DIRECTORY, fullPath)
							+ SFTPConstants.CAUSE + e.getMessage(),
					e);
		}
	}

	/**
	 * Gets the file metadata.
	 *
	 * @param fileFullPath the file full path
	 * @return the file metadata
	 */
	public SftpATTRS getFileMetadata(String fileFullPath) {

		try {
			return sftpChannel.lstat(fileFullPath);
		} catch (SftpException e) {
			throw new SFTPSdkException(
					MessageFormat.format(SFTPConstants.ERROR_FAILED_GETING_FILE_METADATA, fileFullPath)
							+ SFTPConstants.CAUSE + e.getMessage(),
					e);

		}
	}

	/**
	 * Creates the nested directory.
	 *
	 * @param fullPath the full path
	 */
	public void createNestedDirectory(String fullPath) {
		String[] folders = fullPath.split("/");
		changeCurrentDirectory("/");
		for (String folder : folders) {
			if (folder.length() > 0) {
				try {
					changeCurrentDirectory(folder);
				} catch (SFTPSdkException e) {
					createDirectory(folder);
					changeCurrentDirectory(folder);
				}
			}
		}
	}

	/**
	 * Gets the home directory.
	 *
	 * @return the home directory
	 */
	public String getHomeDirectory() {
		try {
			return sftpChannel.getHome();
		} catch (SftpException e) {
			throw new SFTPSdkException(SFTPConstants.ERROR_GETTING_HOME_DIR + SFTPConstants.CAUSE + e.getMessage());
		}
	}

	/**
	 * Retrieve file.
	 *
	 * @param fileName        the file name
	 * @param outputStream    the output stream
	 * @param noOfBytestoSkip the no of bytesto skip
	 */
	public void retrieveFile(String fileName, MeteredTempOutputStream outputStream, long noOfBytestoSkip) {

		try {
			if (!".".equals(fileName) && !"..".equals(fileName)) {
				sftpChannel.get(fileName, outputStream, null, ChannelSftp.RESUME, noOfBytestoSkip);
			}
		} catch (SftpException e) {
			if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
				throw new NoSuchFileFoundException(SFTPConstants.FILE_NOT_FOUND, e);
			}
			throw new SFTPSdkException(MessageFormat.format(SFTPConstants.ERROR_FAILED_FILE_RETRIEVAL, fileName)
					+ SFTPConstants.CAUSE + e.getMessage(), e);
		}

	}

	/**
	 * List directory content with selector.
	 *
	 * @param directoryFullPath the directoryFullPath path
	 * @param selector    the selector
	 */
	public void listDirectoryContentWithSelector(String directoryFullPath, LsEntrySelector selector) {

		try {
			sftpChannel.ls(directoryFullPath, selector);
		} catch (SftpException sftpException) {
			if (sftpException.getCause() instanceof InvalidPathException) {
				List<String> files = getSpecialCharacterFilenames(directoryFullPath);

				String message = MessageFormat.format(SFTPConstants.ERROR_FAILED_GETING_DIRECTORY_CONTENTS,
						directoryFullPath) + SFTPConstants.CAUSE + sftpException.getCause().getMessage() + "\n\n"
						+ "List of files with non-permissible characters in the filename: " + "\n" + files;
				throw new FileNameNotSupportedException(message, sftpException);
			}
			throw new SFTPSdkException(
					MessageFormat.format(SFTPConstants.ERROR_FAILED_GETING_DIRECTORY_CONTENTS, directoryFullPath)
							+ SFTPConstants.CAUSE + sftpException.getCause().getMessage(), sftpException);
		}
	}

	private List<String> getSpecialCharacterFilenames(String directoryFullPath) {
		List<LsEntry> lsEntries = listDirectoryContent(directoryFullPath);
		List<String> files = new ArrayList<>();
		for (LsEntry entry : lsEntries) {
			String filename = entry.getFilename();
			Matcher matcher = PATTERN.matcher(filename);
			if(matcher.find()){
				files.add(filename);
			}
		}
		return files;
	}

	/**
	 * Close.
	 */
	@Override
	public void close() {
		closeConnection();

	}

	/**
	 * Sets the default timeout.
	 *
	 * @param connectionTimeout the new default timeout
	 */
	public void setDefaultTimeout(int connectionTimeout) {
		try {
			session.setTimeout(connectionTimeout);
		} catch (JSchException e) {
			throw new ConnectorException(e);
		}

	}

	/**
	 * List directory content.
	 *
	 * @param directoryFullPath the directory full path
	 * @return the list
	 */
	public List<LsEntry> listDirectoryContent(String directoryFullPath) {
		List<LsEntry> fileList;
		try {
			fileList = sftpChannel.ls(directoryFullPath);
		} catch (SftpException e) {
			throw new SFTPSdkException(
					MessageFormat.format(SFTPConstants.ERROR_FAILED_GETING_DIRECTORY_CONTENTS, directoryFullPath)
							+ SFTPConstants.CAUSE + e.getMessage(),
					e);
		}
		return fileList;

	}
}
