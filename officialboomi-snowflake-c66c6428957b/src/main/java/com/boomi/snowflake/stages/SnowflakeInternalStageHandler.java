// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.stages;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.snowflake.util.ConnectionProperties.ConnectionGetter;
import com.boomi.util.IOUtil;

import net.snowflake.client.jdbc.SnowflakeConnection;

public class SnowflakeInternalStageHandler implements StageHandler {
	private static final String SQL_COMMAND_PARALLEL = " PARALLEL = ";
	private static final String SQL_COMMAND_AUTO_COMPRESS = " AUTO_COMPRESS = ";
	private static final String SQL_COMMAND_SOURCE_COMPRESSION = " SOURCE_COMPRESSION = ";
	private static final String SQL_COMMAND_OVERWRITE = " OVERWRITE = ";
	private String _stageName;
	private ConnectionGetter _getter;
	private Long _parallel;
	private boolean _autoCompress;
	private String _sourceCompression;
	private boolean _overwrite;

	public SnowflakeInternalStageHandler(ConnectionGetter getter, String stageName, 
			Long parallel, Boolean autoCompress, String sourceCompression, Boolean overwrite) {
		_getter = getter;
		_stageName = stageName;
		_parallel = parallel;
		_autoCompress = autoCompress;
		_sourceCompression = sourceCompression;
		if(_sourceCompression.equals("AUTO")) {
			_sourceCompression += "_DETECT";
		}
		_overwrite = overwrite;
	}

	@Override
	public ArrayList<String> getListObjects(String prefix) {
		ArrayList<String> ret = new ArrayList<String>();
		String sql = "list @" + _stageName + "/" + prefix;
		PreparedStatement preparedStatement = null;
		ResultSet resultset = null;
		try {
			preparedStatement = _getter.getConnection(null).prepareStatement(sql);
			resultset = preparedStatement.executeQuery();
			while (resultset.next()) {
				String keyName = resultset.getString(1);
				int idx = keyName.indexOf('/') + 1;
				keyName = keyName.substring(idx);
				ret.add(keyName);
			}
		}catch(SQLException e) {
			throw new ConnectorException("Cannot list objects in stage", e);
		}finally {
			IOUtil.closeQuietly(resultset, preparedStatement);
		}

		return ret;
	}

	@Override
	public InputStream download(String keyName) {
		InputStream out = null;
		try {
			out =  _getter.getConnection(null).unwrap(SnowflakeConnection.class).downloadStream(_stageName, keyName, false);
		}catch(SQLException e) {
			throw new ConnectorException("Cannot download data from snowflake", e);
		}
		return out;
	}


	@Override
	public void delete(String keyName) {
		String sql = "Remove @" + _stageName + "/" + keyName;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			preparedStatement =  _getter.getConnection(null).prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
		}catch (SQLException e) {
			throw new ConnectorException("Cannot download delete from snowflake", e);
		}finally {
			IOUtil.closeQuietly(resultSet, preparedStatement);
		}
	}

	@Override
	public String getStageUrl(String prefixPath) {
		return " @" + _stageName + "/" + prefixPath + "";
	}

	/**
	 * Uploads a file to a Snowflake internal stage.
	 *
	 * @param filePath The local path of the file to be uploaded.
	 * @param stagePrefix The prefix within the stage where the file should be uploaded.
	 * @param dynamicPropertyMap A map of dynamic properties that may be used during the connection process.
	 * @return A string containing the names of the successfully uploaded files, separated by commas if multiple files
	 * are uploaded.
	 * @throws ConnectorException If the upload fails due to SQL errors or other issues.
	 *
	 * @implNote This method constructs a SQL PUT command to upload the file to Snowflake.
	 *           It uses class fields like _stageName, _parallel, _sourceCompression, _autoCompress, and _overwrite
	 *           to configure the upload process. The method executes the PUT command and processes the result set
	 *           to compile a list of uploaded files.
	 *
	 * @implSpec The SQL command includes options for parallel processing, source compression,
	 *           auto-compression, and overwrite behavior. These are controlled by the respective
	 *           class fields (_parallel, _sourceCompression, _autoCompress, _overwrite).
	 *
	 * @see SnowflakeInternalStageHandler
	 */
	@Override
	public String upload(String filePath, String stagePrefix, DynamicPropertyMap dynamicPropertyMap) {
		String sql = "put file://" + filePath + " @" + _stageName + "/" + stagePrefix +
					SQL_COMMAND_PARALLEL + _parallel +
					SQL_COMMAND_SOURCE_COMPRESSION + _sourceCompression +
					SQL_COMMAND_AUTO_COMPRESS + _autoCompress +
					SQL_COMMAND_OVERWRITE + _overwrite;
		StringBuilder uploadedFiles = new StringBuilder();
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			preparedStatement =  _getter.getConnection(null,
					dynamicPropertyMap).prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				if (uploadedFiles.length() != 0) {
					uploadedFiles.append(", ");
				}

				uploadedFiles.append(resultSet.getString(1));
			}
		}catch(SQLException e) {
			throw new ConnectorException("Failed to upload files to Snowflake internal stage", e);
		}finally {
			IOUtil.closeQuietly(resultSet, preparedStatement);
		}
		return uploadedFiles.toString();
	}

	/**
	 * Return empty string because internal stage doesn't use credentials to be used in Snowflake
	 * COPY INTO parameter
	 * 
	 * @return String empty string
	 */
	@Override
	public String getStageCredentials() {
		return "";
	}

	/**
	 * Tests internal stage connection by making a listObjects request
	 * 
	 */
	@Override
	public void testConnection() {
		getListObjects("Random/value");
	}

	@Override
	public void upload(String path, String fileFormat, InputStream data, long dataLength) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void UploadHandler(String path, String fileFormat, InputStream data, long chunkSize,
			boolean compressionActivated, char recordDelimiter) {
		// TODO Auto-generated method stub
		
	}

}
