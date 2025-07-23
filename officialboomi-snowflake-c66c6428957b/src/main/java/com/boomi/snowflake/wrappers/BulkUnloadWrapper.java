// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.wrappers;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.snowflake.stages.StageHandler;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JsonArrayStream;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;

/**
 * The Class BulkUnloadWrapper
 * @author s.vanangudi
 *
 */
public class BulkUnloadWrapper extends SnowflakeWrapper {
	
	/** The Constant SQL_COMMAND_COPY_INTO. */
	private static final String SQL_COMMAND_COPY_INTO = "COPY INTO ";
	/** The Constant SQL_COMMAND_FROM. */
	private static final String SQL_COMMAND_FROM = " FROM ";
	/** The File Format. */
	private String _fileFormat = "FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP')";
	/** The Copy Options. */
	private String _copyOptions;
	/** The Header. */
	private String _header;
	/** The SQL Select Command. */
	private String _sqlCommandSelect = "SELECT OBJECT_CONSTRUCT(*) ";
	/** The Stage Handler. */
	private StageHandler _stageHandler;
	/** The Filter Objects map. */
	private SortedMap<String, String> _filterObj;
	/** The Files path. */
	private List<String> _filesPath;
	/** The File index. */
	private int _fileIndex;
	/** The Input Stream. */
	private InputStream _inputStream;
	/** The StagePath. */
	private String _stagePath;
	/** The CSV File Format. */
	private boolean _csvFileForamtEntered;
	/** The GZIP Compression. */
	private boolean _gzipCompressionEntered;

	/**
	 * @param properties    contains the JDBC connection properties
	 * @param stageHandler  Stage handler that will be operated on
	 * @param filterObj     JSON object represents the WHERE conditions to the select
	 *                      statement
	 * @param inputDocument input document that will be operated on              
	 */
	public BulkUnloadWrapper(ConnectionProperties properties, StageHandler stageHandler,
			SortedMap<String, String> filterObj, ObjectData inputDocument) {
		super(properties.getConnectionGetter(), properties.getConnectionTimeFormat(), properties.getLogger(),
				properties.getTableName());
		_stageHandler = stageHandler;
		_filterObj = filterObj;
		_fileIndex = 0;
		_stagePath = properties.getStageTempPath(inputDocument);
		_gzipCompressionEntered = "GZIP".equals(properties.getCompression());
		if (properties.getFileFormatType().length() == 0) {
			_fileFormat = "FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP')";
			_copyOptions = _header = "";
			initialize(inputDocument.getDynamicOperationProperties());
			prepareInputStream();
		} else {
			_csvFileForamtEntered = "CSV".equals(properties.getFileFormatType());
			if (_csvFileForamtEntered) {
				_sqlCommandSelect = _sqlCommandSelect.replace("OBJECT_CONSTRUCT(*)", "*");
			}
			if (properties.getFileFormatName().length() == 0) {
				_fileFormat = " FILE_FORMAT = (";
				_fileFormat += " TYPE = '" + properties.getFileFormatType() + "' ";
				_fileFormat += " COMPRESSION = '" + properties.getCompression() + "' ";
				_fileFormat += " " + properties.getOtherFormatOptions() + ") ";
			} else {
				_fileFormat = " FILE_FORMAT = ( FORMAT_NAME = '" + properties.getFileFormatName() + "')";
			}
			_copyOptions = properties.getCopyOptions();
			_header = (properties.getHeader() ? " HEADER = TRUE " : "");
			initialize(inputDocument.getDynamicOperationProperties());
		}

	}

	/**
	 * Initialize the first steps of Bulk Unloading; export files into cloud
	 * location then download files from cloud location
	 *
	 * @param dynamicOperationProperties Map containing dynamic properties for the operation
	 */
	private void initialize(DynamicPropertyMap dynamicOperationProperties) {
		_processLogger.fine("Executing Copy Into cloud.");
		copyIntoLocation(dynamicOperationProperties);
		_filesPath = _stageHandler.getListObjects(_stagePath);

		if (_filesPath.isEmpty()) {
			_processLogger.warning("Copy executed with 0 files processed.");
		} else {
			_processLogger.fine(() -> "Copy executed with " + _filesPath.size() + " files processed.");
		}
	}

	/**
	 * Finalize the last steps of Bulk Unloading; delete all exported files on cloud
	 * location after all data is read
	 * 
	 * @return
	 */
	public void closeResources() {
		_processLogger.fine("Deleting data from cloud.");
		try {
			for (String keyName : _filesPath) {
				_stageHandler.delete(keyName);
			}

		} catch (Exception e) {
			throw e;
		} finally {
			closeQuitely();
		}
	}

	/**
	 * Returns next file.
	 * @return Input Stream
	 */
	public InputStream getNextFile() {
		if (prepareInputStream() == false) {
			return null;
		}

		return _inputStream;
	}

	/**
	 * Retrieves the table as a file
	 * 
	 * @return Input Stream
	 */
	private InputStream getFile() {
		_processLogger.fine("Downloading and perparing data from cloud location.");
		return CollectionUtil.isEmpty(_filesPath) ? null : _stageHandler.download(_filesPath.get(_fileIndex));
	}

	/**
	 * Retrieves next file from cloud location and prepare an input stream of the
	 * data after decompressing it
	 * 
	 * @return <true> if exists a next input stream <false> otherwise
	 */
	private boolean prepareInputStream() {
		closeQuitely();
		if (_fileIndex == _filesPath.size()) {
			return false;
		}
		_processLogger.fine("Downloading and perparing data from cloud location.");
		_inputStream = getFile();
		if (_gzipCompressionEntered) {
			try {
				_inputStream = new GZIPInputStream(_inputStream);
			} catch (IOException e) {
				throw new ConnectorException("Cannot convert InputStream to GZIP stream", e);
			}
		}
		if (!_csvFileForamtEntered) {
			_inputStream = new JsonArrayStream(_inputStream);
		}
		++_fileIndex;
		return true;
	}

	/**
	 * Executes the Copy Into command to export data from Snowflake table to cloud
	 * location
	 *
	 * @param dynamicOperationProperties Map containing dynamic properties for the operation
	 */
	private void copyIntoLocation(DynamicPropertyMap dynamicOperationProperties) {
		String url = _stageHandler.getStageUrl(_stagePath);
		String credentials = _stageHandler.getStageCredentials();
		String selectStatement = "(" + _sqlCommandSelect + SQL_COMMAND_FROM + _tableName
				+ sqlConstructWhereClause(_filterObj) + ")\n";
		String sql = SQL_COMMAND_COPY_INTO + url + SQL_COMMAND_FROM + selectStatement + credentials + _fileFormat
				+ _copyOptions + _header;
		_processLogger.info(() -> "SQL: " + sql.replace(credentials, ""));
		SortedMap<String, String> metadata = null;
		if(_tableName != null) {
			metadata = SnowflakeOperationUtil.getTableMetadata(_tableName, _getter.getConnection(_processLogger),
					dynamicOperationProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
					dynamicOperationProperties.getProperty(SnowflakeOverrideConstants.SCHEMA));
		}
		try (PreparedStatement preparedStatement = _getter.getConnection(_processLogger,dynamicOperationProperties).
				prepareStatement(sql)) {
			fillObjectDefination(filterObjectDefinition, _filterObj);
			fillStatementValuesWithDataType(preparedStatement, new TreeMap<>(), _filterObj, metadata);
			preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new ConnectorException("Failed to execute Copy Into cloud location", e);
		}
	}

	/**
	 * Closes Streams
	 */
	private void closeQuitely() {
		IOUtil.closeQuietly(_inputStream);
	}
}
