// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.controllers;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.SortedMap;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.snowflake.stages.AmazonWebServicesHandler;
import com.boomi.snowflake.stages.SnowflakeInternalStageHandler;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.wrappers.BulkUnloadWrapper;
import com.boomi.snowflake.wrappers.SnowflakeTableStream;

public class SnowflakeGetController {
	private BulkUnloadWrapper _bulkUnloadWrapper = null;
	private SnowflakeTableStream _reader = null;

	/**
	 * @param properties connection properties set for this operation
	 * @param filterObj containing filtering logic
	 */
	public SnowflakeGetController(ConnectionProperties properties, SortedMap<String, String> filterObj, ObjectData inputDocument){

		if (properties.getBucketName() != null && properties.getBucketName().length() > 0) {
			properties.getLogger().fine(
					"Loading Amazon Web Services Connection and Operation settings and establishing connection.");
			if(properties.getAWSRegion().isEmpty()) {
				throw new ConnectorException("Select a valid AWS Region");
			}
			AmazonWebServicesHandler awsHandler = new AmazonWebServicesHandler(
					properties.getBucketName(), properties.getAccessKey(),
					properties.getSecret(), properties.getAWSRegion());

			awsHandler.testConnection();

			properties.getLogger().fine(
					"Loading Database Connection and Operation settings then establishing connection.");
			_bulkUnloadWrapper = new BulkUnloadWrapper(properties, awsHandler, filterObj, inputDocument);
		} else if (properties.getStageName() != null && properties.getStageName().length() > 0) {
			properties.getLogger().fine("Preparing to connect to Snowflake internal stage.");
			
			SnowflakeInternalStageHandler internalStage = new SnowflakeInternalStageHandler(
					properties.getConnectionGetter(), 
					properties.getStageName(),
					properties.getParallelism(),
					properties.getAutoCompress(),
					properties.getSourceCompression(),
					properties.getOverwrite());
			internalStage.testConnection();

			_bulkUnloadWrapper = new BulkUnloadWrapper(properties, internalStage , filterObj, inputDocument);
		} else {
			if (properties.getBatchSize() == null || properties.getBatchSize() < 1) {
				throw new ConnectorException("Batch Size must be a positive integer");
			}
			
			properties.getLogger().fine(
					"Loading Database Connection and Operation settings and establishing connection.");

			_reader = new SnowflakeTableStream(properties, new ArrayList<>(), filterObj, new ArrayList<>(), null,
					inputDocument.getDynamicOperationProperties());
		}
	}

	/**
	 * Retrieves the next JSON object from the correct stream
	 * 
	 * @return stream containing JSON string represents the next row data
	 */
	public InputStream getNext()  {
		if(_reader != null) {
			return _reader.getBatchedRow();
		}
		
		return null;
	}
	
	/**
	 * Retrieves the next JSON object from the correct stream
	 *
	 * @return stream containing JSON string represents the next row data
	 */
	public boolean next()  {
		return _reader.next();
	}

	/**
	 * Retrieves the table as a file
	 * @return
	 */
	public InputStream getNextFile() {
		return _bulkUnloadWrapper.getNextFile();
	}
	
	/**
	 * Finalize the last steps of Get and closes connection
	 */
	public void close() {
		if (_reader != null) {
			_reader.closeReader();
		} else {
			try {
				_bulkUnloadWrapper.closeResources();
			}catch(Exception e) {
				throw e;
			}finally {
				_bulkUnloadWrapper.close();
			}
		}
	}
}
