//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.actions.RetryableQueryAction;
import com.boomi.connector.sftp.common.FileMetadata;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.common.UnixPathsHandler;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.results.BaseResult;
import com.boomi.connector.sftp.results.ErrorResult;
import com.boomi.connector.sftp.results.MultiResult;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;

import java.io.File;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class CustomLsEntrySelector.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class CustomLsEntrySelector implements LsEntrySelector {

	/** The partial result. */
	BaseResult partialResult;

	/** The result. */
	MultiResult result;

	/** The filter. */
	FileQueryFilter filter;

	/** The limit. */
	long limit;

	/** The logger. */
	Logger logger;

	/** The result builder. */
	ResultBuilder resultBuilder;

	/** The dir full path. */
	String dirFullPath;

	/** The conn. */
	SFTPConnection conn;

	/** The pathshandler. */
	UnixPathsHandler pathshandler = new UnixPathsHandler();

	/** The new client. */
	SFTPClient newClient;

	/** The number of entries processed. */
	int numberOfEntriesProcessed;

	/** The level. */
	int level;

	/** The property map. */
	PropertyMap propertyMap;

	/** The reconnect failed. */
	private boolean reconnectFailed;

	OperationResponse operationResponse;

	/**
	 * Checks if is reconnect failed.
	 *
	 * @return true, if is reconnect failed
	 */
	public boolean isReconnectFailed() {
		return reconnectFailed;
	}

	/**
	 * Sets the reconnect failed.
	 *
	 * @param reconnectFailed the new reconnect failed
	 */
	public void setReconnectFailed(boolean reconnectFailed) {
		this.reconnectFailed = reconnectFailed;
	}

	/**
	 * Instantiates a new custom ls entry selector.
	 *
	 * @param result            the result
	 * @param filter            the filter
	 * @param limit             the limit
	 * @param logger            the logger
	 * @param resultBuilder     the result builder
	 * @param dirFullPath       the dir full path
	 * @param conn              the conn
	 * @param newClient         the new client
	 * @param level             the level
	 * @param propertyMap       the property map
	 * @param operationResponse
	 * @param sort
	 */
	public CustomLsEntrySelector(MultiResult result, FileQueryFilter filter, long limit, Logger logger,
			ResultBuilder resultBuilder, String dirFullPath, SFTPConnection conn, SFTPClient newClient, int level,
			PropertyMap propertyMap, OperationResponse operationResponse) {
		this.result = result;
		this.filter = filter;
		this.limit = limit;
		this.logger = logger;
		this.resultBuilder = resultBuilder;
		this.newClient = newClient;
		this.dirFullPath = dirFullPath;
		this.conn = conn;
		this.level = level;
		this.propertyMap = propertyMap;
		this.operationResponse = operationResponse;
	}

	/**
	 * Select.
	 *
	 * @param entry the entry
	 * @return the int
	 */
	@Override
	public int select(LsEntry entry) {
		try {
			String filename = entry.getFilename();
			if (filename.equals(".") || filename.equals("..")) {
				numberOfEntriesProcessed++;
				return CONTINUE;
			}
			if (!entry.getAttrs().isLink()) {
				String fullFilePath = pathshandler.joinPaths(dirFullPath, filename);
				File fullFilePathObj = new File(fullFilePath);
				partialResult = getPartialResult(filter, logger, fullFilePathObj.toPath(), resultBuilder, entry, propertyMap);
				numberOfEntriesProcessed++;
				addPartialResultAndListContent(entry, filename, fullFilePath);


				if ((long) result.getSize() == limit) {
					return BREAK;
				}
			}
			return CONTINUE;
		} finally {
			if (partialResult != null && resultBuilder instanceof QueryResultBuilder) {
				QueryResultBuilder queryResult = (QueryResultBuilder) resultBuilder;
				IOUtil.closeQuietly(queryResult.getFilecontent());
			}
		}
	}

	/**
	 * Adds partial results and lists directory contents
	 * @param entry        	the string to be checked
	 * @param filename		the file name
	 * @param fullFilePath	full path for the file
	 */
	private void addPartialResultAndListContent(LsEntry entry, String filename, String fullFilePath) {
		if (partialResult != null && !(partialResult instanceof ErrorResult)) {
			if (logger.isLoggable(Level.FINE)) {
				LogUtil.fine(logger, "Adding %s", fullFilePath);
			}
			result.addPartialResult(dirFullPath, filename, partialResult);
			if (level == 0 && entry.getAttrs().isDir()
					&& propertyMap.getBooleanProperty("recursiveList", false)) {
				CustomLsEntrySelector subFolderSelector = null;
				SFTPClient subFolderClient = null;
				try {
					subFolderClient = conn
							.createSftpClient(new PropertiesUtil(conn.getContext().getConnectionProperties()));
					subFolderSelector = new CustomLsEntrySelector(this.result, this.filter, this.limit,
							this.logger, this.resultBuilder, fullFilePath, this.conn, this.newClient, 1,
							propertyMap, operationResponse);
					subFolderClient.openConnection();
					subFolderClient.listDirectoryContentWithSelector(fullFilePath, subFolderSelector);
				} finally {
					IOUtil.closeQuietly(subFolderClient);
				}
			}
		}
	}


	/**
	 * Gets the partial result.
	 *
	 * @param filter        the filter
	 * @param logger        the logger
	 * @param path          the path
	 * @param resultBuilder the result builder
	 * @param entry         the entry
	 * @param propertyMap
	 * @return the partial result
	 */
	private BaseResult getPartialResult(FileQueryFilter filter, Logger logger, Path path, ResultBuilder resultBuilder,
			LsEntry entry, PropertyMap propertyMap) {
		RetryableQueryAction rQuery = null;
		try {
			if (filter.accept(entry)) {
				rQuery = new RetryableQueryAction(conn, filter.getInput(),
						RetryStrategyFactory.createFactory(1), newClient,
						FileMetadata.joinPaths(dirFullPath, entry.getFilename()));
				if (resultBuilder instanceof QueryResultBuilder) {
					if (!reconnectFailed) {
						return resultBuilder.makeResult(entry, dirFullPath, rQuery, propertyMap, operationResponse);
					} else {
						LogUtil.warning(logger,
								"Error fetching file from " + FileMetadata.joinPaths(dirFullPath, entry.getFilename()),
								new ConnectorException("Lost Connectivity to Remote System"));
						return new ErrorResult(OperationStatus.APPLICATION_ERROR,
								new ConnectorException("Lost Connectivity to Remote System"));
					}
				} else {
					return resultBuilder.makeResult(entry, dirFullPath, rQuery, propertyMap, operationResponse);
				}
			}
			return null;
		} catch (Exception e) {
			if (rQuery != null) {
				reconnectFailed = rQuery.isReconnectFailed();
				if (reconnectFailed) {
					LogUtil.warning(logger,
							"Error fetching file from " + FileMetadata.joinPaths(dirFullPath, entry.getFilename()), e);
					return new ErrorResult(OperationStatus.APPLICATION_ERROR, e);
				}
			}
			LogUtil.warning(logger, e, SFTPConstants.UNEXPECTED_ERROR_OCCURED, path);
			return new ErrorResult(OperationStatus.APPLICATION_ERROR, e);
		} finally {
			if (rQuery != null) {
				rQuery.close();
			}
		}
	}

}
