// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.wrappers;

import java.io.InputStream;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.snowflake.stages.StageHandler;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.StringUtil;

public class BulkLoadWrapper extends SnowflakeWrapper {
	private static final String SQL_COMMAND_PURGE = " PURGE=TRUE ";
	private static final String SQL_COMMAND_COPY_INTO = "COPY INTO ";
	private static final String SQL_COMMAND_FROM = " FROM ";
	private static final String RECORD_DELIMITER_REGEX = "RECORD_DELIMITER[ ]{0,}=[ ]{0,}'.'";
	
	private String _sqlConstructFields;
	private long _chunkSize;
	private StageHandler _stageHandler;
	private Boolean _truncate;
	private Boolean _autoCompress;
	private String _fileFormatOptions;
	private String _copyOptions;
	private String _fileFormat;
	private char _recordDelimiter;
	
	
	/**
	 * @param properties    Connection properties
	 * @param stageHandler  Stage handler that will be operated on
	 */
	public BulkLoadWrapper(ConnectionProperties properties, StageHandler stageHandler) {
		super(properties.getConnectionGetter(), properties.getConnectionTimeFormat(),
				properties.getLogger(), properties.getTableName());
		_stageHandler = stageHandler;
		_sqlConstructFields = "";
		_truncate = properties.getTruncate();
		_processLogger = properties.getLogger();
		_autoCompress = properties.getAutoCompress();
		_chunkSize = properties.getChunkSize();
		
		if (properties.getColumnNames().length() != 0) {
			_sqlConstructFields = "(" + properties.getColumnNames() + ")";
		}
		
		boolean alreadyCompressed;
		if (properties.getFileFormatType().length() == 0 || properties.getCompression().equals("NONE")) {
			alreadyCompressed = false;
		} else {
			alreadyCompressed = true;
		}
		String compression = alreadyCompressed ? ("." + properties.getCompression()) : StringUtil.EMPTY_STRING;
		_fileFormat = "." + properties.getFileFormatType() + (_autoCompress ? ".gz" : compression);
		_recordDelimiter = properties.getFileFormatType().equals("CSV") ? '\n' : ',';

		handleFileFormat(properties, alreadyCompressed);

		_copyOptions = properties.getCopyOptions();
		if (_copyOptions.toLowerCase().contains("purge") == false) {
			_copyOptions += SQL_COMMAND_PURGE;
		}
	}

	private void handleFileFormat(ConnectionProperties properties, boolean alreadyCompressed) {
		if (properties.getFileFormatName().length() == 0) {
			_fileFormatOptions = " FILE_FORMAT = ( ";
			_fileFormatOptions += " TYPE = '" + (properties.getFileFormatType().length() != 0 ? properties.getFileFormatType() : "CSV") + "' ";
			_fileFormatOptions += " COMPRESSION = '" + (alreadyCompressed ? properties.getCompression() : "AUTO") + "' ";
			if (properties.getFileFormatType().length() == 0) {
				_fileFormatOptions += " FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ";
			} else {
				String formatOptions = properties.getOtherFormatOptions().toUpperCase();
				_fileFormatOptions += " " + formatOptions + " ";
				Pattern pattern = Pattern.compile(RECORD_DELIMITER_REGEX);
				Matcher matcher = pattern.matcher(formatOptions);
				if (matcher.find()) {
					String match = matcher.group(0);
					_recordDelimiter = match.charAt(match.length() - 2);
				}
			}
			_fileFormatOptions += ") ";
		} else {
			_fileFormatOptions = " FILE_FORMAT = ( FORMAT_NAME = '" + properties.getFileFormatName() + "')";
		}
	}

	/**
	 * Uploads to Stage location with key name with
	 * this format: [STAGE_PATH][Timestamp][counter]
	 * @param inputFile stream containing sent file
	 * @param inputDocument the input document
	 * @param properties the connection properties
	 */
	public void uploadData(InputStream inputFile, ObjectData inputDocument, ConnectionProperties properties) {
		_processLogger.fine("Uploading data to cloud.");
		String stagePath = properties.getStageTempPath(inputDocument);
		_stageHandler.UploadHandler(stagePath, _fileFormat, inputFile, _chunkSize, _autoCompress, _recordDelimiter);
		copyIntoTable(stagePath,inputDocument.getDynamicOperationProperties());
	}

	/**
	 * Executes the Copy Into command to import data from cloud location to Snowflake table, with
	 * purge=true parameter to delete files after copying
	 * @param stagePath the stage path
	 * @param dynamicPropertyMap the dynamic property map
	 */
	private void copyIntoTable(String stagePath, DynamicPropertyMap dynamicPropertyMap) {
		String url = _stageHandler.getStageUrl(stagePath);
		String credentials = _stageHandler.getStageCredentials();
		String sql = getQueryForBulkLoad(url, credentials);
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
	 * Constructs an SQL query for copying data into a table.
	 *
	 * This method builds an SQL query string using the provided URL and credentials,
	 * along with predefined SQL command components and options.
	 *
	 * @param url The URL from which to copy data.
	 * @param credentials The credentials to access the data source.
	 * @return The constructed SQL query string.
	 */
	private String getQueryForBulkLoad(String url, String credentials) {
		return SQL_COMMAND_COPY_INTO + _tableName + _sqlConstructFields + SQL_COMMAND_FROM
				+ url + credentials + _fileFormatOptions + _copyOptions;
	}

	/**
	 * sets the stage handler for wrapper
	 * @param stageHandler AWS handler
	 */
	public void setStageHandler(StageHandler stageHandler) {
		_stageHandler = stageHandler;
	}
}
