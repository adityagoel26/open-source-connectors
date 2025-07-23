// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.wrappers;

import java.sql.SQLException;
import java.sql.PreparedStatement;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.snowflake.stages.StageHandler;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.StringUtil;

public class BulkLoadFiles extends SnowflakeWrapper {
	private static final String SQL_COMMAND_PURGE = " PURGE = TRUE ";
	private static final String SQL_COMMAND_COPY_INTO = "COPY INTO ";
	private static final String SQL_COMMAND_FROM = " FROM ";
	private static final String FILE_FORMAT_TYPE = " FILE_FORMAT = ( TYPE = '%s', COMPRESSION = '%s'%s)";
	private static final String FILE_FORMAT_NAME = " FILE_FORMAT = ( FORMAT_NAME = '%s'%s)";
	private String _filePath;
	private final String _fileFormat;
	private String _columnNames;
	private String _copyOptions;
	private StageHandler _stageHandler;
	private final boolean _truncate;

	/**
	 * @param properties    JDBC connection properties
	 * @param stageHandler  Stage handler that will be operated on
	 */
	public BulkLoadFiles(ConnectionProperties properties,StageHandler stageHandler) {
		super(properties.getConnectionGetter(), properties.getConnectionTimeFormat(),
				properties.getLogger(), properties.getTableName());
		_stageHandler = stageHandler;
		_filePath = properties.getFilePath();
		_truncate = properties.getTruncate();
		_processLogger = properties.getLogger();
		_columnNames = "";
		_fileFormat = getFileFormat(properties);
		_copyOptions = properties.getCopyOptions();
		if(!_copyOptions.toLowerCase().contains("purge")) {
			_copyOptions += SQL_COMMAND_PURGE;
		}
		if(properties.getColumnNames().length() != 0) {
			_columnNames = "(" + properties.getColumnNames() + ")";
		}
	}

	/**
	 * Get file format name with other format options
	 * @param properties JDBC connection properties
	 * @return file format
	 */
	private static String getFileFormat(ConnectionProperties properties) {
		if (StringUtil.isBlank(properties.getFileFormatType())) {
			return StringUtil.EMPTY_STRING;
		}
		if (StringUtil.isBlank(properties.getFileFormatName())) {
			return String.format(FILE_FORMAT_TYPE, properties.getFileFormatType(), properties.getCompression(),
					StringUtil.formatIfNotBlank(", %s", properties.getOtherFormatOptions()));
		} else {
			return String.format(FILE_FORMAT_NAME, properties.getFileFormatName(),
					StringUtil.formatIfNotBlank(", %s", properties.getOtherFormatOptions()));
		}
	}

	/**
	 * Initiates the process of uploading files to the cloud and copying them into a Snowflake table.
	 * @param inputDocument An ObjectData object containing the input data for the process.
	 * @param properties A ConnectionProperties object containing the properties required for
	 * connecting to the Snowflake database and performing the upload and copy operations.
	 */
	public void start(ObjectData inputDocument, ConnectionProperties properties) {
		String stagePath = properties.getStageTempPath(inputDocument);
		_processLogger.fine("Uploading files to cloud.");
		uploadFiles(_filePath, stagePath,inputDocument.getDynamicOperationProperties());

		_processLogger.fine("Executing Copy Into snowflake table.");
		copyIntoTable(stagePath,inputDocument.getDynamicOperationProperties());
	}

	/**
	 * Uploads files from a specified local path to a Snowflake stage.
	 *
	 * @param filePath The local file path where the files to be uploaded are located.
	 * @param stagePath The path within the Snowflake stage where the files should be uploaded.
	 * @param dynamicPropertyMap A map containing dynamic properties that may be used during the upload process.
	 *
	 * @implNote This method uses a StageHandler to perform the actual file upload operation.
	 *           If no files are found or uploaded, a warning message is logged.
	 *           Otherwise, a fine-level log message is created with the names of the uploaded files.
	 *
	 * @throws RuntimeException If there's an error during the upload process (this is implied and depends on the
	 * implementation of _stageHandler.upload()).
	 */
	private void uploadFiles(String filePath, String stagePath,DynamicPropertyMap dynamicPropertyMap) {
		String message = _stageHandler.upload(filePath, stagePath,dynamicPropertyMap);
		if (StringUtil.isBlank(message)) {
			_processLogger.warning("0 files found");
		} else {
			_processLogger.fine(() -> String.format("Uploaded files: %s", message));
		}
	}

	/**
	 * Copies files from a specified stage path into a Snowflake table.
	 * <p>If this is the first time the method is called and the `_truncate` flag is set,
	 * the table will be truncated before copying the files.</p>
	 *
	 * @param stagePath The path to the stage where the files are located.
	 * @param dynamicPropertyMap A map containing dynamic properties required for
	 * constructing the COPY INTO statement.
	 */
	private void copyIntoTable(String stagePath, DynamicPropertyMap dynamicPropertyMap) {
		String url = _stageHandler.getStageUrl(stagePath);
		String credentials = _stageHandler.getStageCredentials();
		String sql = getQuery(url,credentials);
		_processLogger.info(() -> String.format("SQL: %s", sql.replace(credentials, "")));
		try (PreparedStatement statement = _getter.getConnection(_processLogger,
				dynamicPropertyMap).prepareStatement(sql)){
			if(_truncate){
				truncateTableIfNotDone(dynamicPropertyMap);
			}
			statement.execute();
		}catch(SQLException e) {
			throw new ConnectorException(SnowflakeOverrideConstants.COPY_COMMAND_ERROR, e);
		}
	}
	/**
	 * Builds an SQL query for copying data into a table.
	 *
	 * This method constructs an SQL query string using the provided URL and credentials,
	 * along with predefined SQL command components and options.
	 *
	 * @param url The URL of the data source.
	 * @param credentials The credentials required to access the data source.
	 * @return The generated SQL query string.
	 */
	private String getQuery(String url, String credentials) {
		return SQL_COMMAND_COPY_INTO + _tableName + _columnNames + SQL_COMMAND_FROM + url + credentials
				+ _fileFormat + _copyOptions;
	}

	/**
	 * @return the file format string
	 */
	public String getFileFormat() {
		return _fileFormat;
	}

	/**
	 * sets the filePath for wrapper
	 * @param filePath Internal Source Files Path
	 */
	public void setFilePath(String filePath) {
		_filePath = filePath;
	}

	/**
	 * sets the stage handler for wrapper
	 * @param stageHandler snowflake internal stage handler
	 */
	public void setStageHandler(StageHandler stageHandler) {
		_stageHandler = stageHandler;
	}
}
