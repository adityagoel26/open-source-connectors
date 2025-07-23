//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.handlers;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.CustomLsEntrySelector;
import com.boomi.connector.sftp.FileQueryFilter;
import com.boomi.connector.sftp.ResultBuilder;
import com.boomi.connector.sftp.SFTPClient;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.actions.RetryableLsEntrySelector;
import com.boomi.connector.sftp.common.FileMetadata;
import com.boomi.connector.sftp.common.PathsHandler;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.sftp.results.MultiResult;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;

/**
 * The Class QueryHandler.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class QueryHandler {

	/** The conn. */
	SFTPConnection conn;

	/** The paths handler. */
	final PathsHandler pathsHandler;

	final OperationResponse operationResponse;

	/**
	 * Make json payload.
	 *
	 * @param meta        the meta
	 * @param dirFullPath the dir full path
	 * @return the payload
	 */
	public static Payload makeJsonPayload(LsEntry meta, String dirFullPath) {
		return QueryHandler.makeJsonPayload(meta, dirFullPath, true);
	}

	/**
	 * Make json payload.
	 *
	 * @param meta        the meta
	 * @param dirFullPath the dir full path
	 * @param includeAll  the include all
	 * @return the payload
	 */
	private static Payload makeJsonPayload(LsEntry meta, String dirFullPath, boolean includeAll) {
		ObjectNode obj = JSONUtil.newObjectNode();
		obj.put(SFTPConstants.PROPERTY_FILENAME, meta.getFilename());
		obj.put(SFTPConstants.PROPERTY_REMOTE_DIRECTORY, dirFullPath);
		if (includeAll) {
			obj.put(SFTPConstants.MODIFIED_DATE,
					FileMetadata.formatDate(FileMetadata.parseDate(meta.getAttrs().getMTime() * 1000L)));
			obj.put(SFTPConstants.FILESIZE, meta.getAttrs().getSize());
			obj.put(SFTPConstants.IS_DIRECTORY, meta.getAttrs().isDir());
		}
		return JsonPayloadUtil.toPayload((TreeNode) obj);
	}

	/**
	 * Instantiates a new query handler.
	 *
	 * @param conn              the conn
	 * @param operationResponse
	 */
	public QueryHandler(SFTPConnection conn, OperationResponse operationResponse) {

		this.conn = conn;
		pathsHandler = conn.getPathsHandler();
		this.operationResponse = operationResponse;

	}

	/**
	 * To full path.
	 *
	 * @param childPath the child path
	 * @return the string
	 */
	String toFullPath(String childPath) {
		return this.pathsHandler.resolvePaths(conn.getHomeDirectory(), childPath);
	}

	/**
	 * Do query.
	 *
	 * @param result        the result
	 * @param filesOnly     the files only
	 * @param resultBuilder the result builder
	 * @param propertyMap   the property map
	 */
	public void doQuery(MultiResult result, boolean filesOnly, ResultBuilder resultBuilder, PropertyMap propertyMap) {
		FilterData input = result.getInput();

		String enteredRemoteDir = conn.getEnteredRemoteDirectory(input);
		String remoteDir;
		if (StringUtil.isBlank(enteredRemoteDir) || !pathsHandler.isFullPath(enteredRemoteDir)) {
			input.getLogger().log(Level.INFO,
					"Entered remote directory path is blank or not a absolute full path.Setting home directory of user as default working path");
			remoteDir = this.toFullPath(enteredRemoteDir);
		} else {
			remoteDir = enteredRemoteDir;
		}
		if (remoteDir == null) {
			throw new ConnectorException(SFTPConstants.ERROR_MISSING_DIRECTORY);
		}
		boolean fileExists = conn.fileExists(remoteDir);
		if (!fileExists) {
			throw new SFTPSdkException(SFTPConstants.ERROR_REMOTE_DIRECTORY_NOT_FOUND);
		}
		Logger logger = input.getLogger();
		File directory = new File(remoteDir);

		FileQueryFilter filter = conn.makeFilter(input, filesOnly, directory, remoteDir);
		SFTPClient newClient = null;
		LsEntrySelector selector = null;
		RetryableLsEntrySelector selectAction = null;
		try {

			long limit = conn.getLimit();
			newClient = conn.createSftpClient(new PropertiesUtil(conn.getContext().getConnectionProperties()));
			selector = new CustomLsEntrySelector(result, filter, limit, logger, resultBuilder, remoteDir, conn,
					newClient, 0, propertyMap, operationResponse);
			selectAction = new RetryableLsEntrySelector(conn, remoteDir, input, selector);
			selectAction.execute();

		} finally {
			IOUtil.closeQuietly(newClient);
		}
	}

}