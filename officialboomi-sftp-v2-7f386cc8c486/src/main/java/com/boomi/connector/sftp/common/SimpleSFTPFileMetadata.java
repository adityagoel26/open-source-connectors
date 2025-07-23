//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadMetadataFactory;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.json.JSONUtil;
import com.boomi.util.json.JsonPayloadUtil;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The Class SimpleSFTPFileMetadata.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SimpleSFTPFileMetadata implements SFTPFileMetadata {
	
	/** The remote dir. */
	private final String remoteDir;
	
	/** The name. */
	private final String name;

	/**
	 * Instantiates a new simple SFTP file metadata.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 */
	public SimpleSFTPFileMetadata(String remoteDir, String fileName) {
		this.remoteDir = remoteDir;
		this.name = fileName;
	}

	/**
	 * Gets the directory.
	 *
	 * @return the directory
	 */
	@Override
	public String getDirectory() {
		return this.remoteDir;
	}

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	@Override
	public String getName() {
		return this.name;
	}

	/**
	 * To payload metadata.
	 *
	 * @param payloadMetadataFactory the payload metadata factory
	 * @return the payload metadata
	 */
	@Override
	public PayloadMetadata toPayloadMetadata(PayloadMetadataFactory payloadMetadataFactory) {
		PayloadMetadata metadata = payloadMetadataFactory.createMetadata();
		metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, this.name);
		metadata.setTrackedProperty(SFTPConstants.REMOTE_DIRECTORY, this.remoteDir);
		return metadata;
	}

	/**
	 * To json payload.
	 *
	 * @return the payload
	 */
	@Override
	public Payload toJsonPayload() {
		return JsonPayloadUtil.toPayload((TreeNode) this.toJson());
	}

	/**
	 * To json.
	 *
	 * @return the object node
	 */
	protected ObjectNode toJson() {
		return JSONUtil.newObjectNode().put(SFTPConstants.PROPERTY_FILENAME, this.name)
				.put(SFTPConstants.REMOTE_DIRECTORY, this.remoteDir);
	}
	

}
