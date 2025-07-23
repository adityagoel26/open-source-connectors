//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadMetadataFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The Class ExtendedSFTPFileMetadata.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class ExtendedSFTPFileMetadata extends SimpleSFTPFileMetadata {
	
	/** The timestamp. */
	private final String timestamp;

	/**
	 * Instantiates a new extended SFTP file metadata.
	 *
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 * @param timeStamp the time stamp
	 */
	public ExtendedSFTPFileMetadata(String remoteDir, String fileName, String timeStamp) {
		super(remoteDir, fileName);
		this.timestamp = timeStamp;
	}

	/**
	 * To payload metadata.
	 *
	 * @param payloadMetadataFactory the payload metadata factory
	 * @return the payload metadata
	 */
	@Override
	public PayloadMetadata toPayloadMetadata(PayloadMetadataFactory payloadMetadataFactory) {
		PayloadMetadata metadata = super.toPayloadMetadata(payloadMetadataFactory);
		metadata.setTrackedProperty("timestamp", this.timestamp);
		return metadata;
	}

	/**
	 * To json.
	 *
	 * @return the object node
	 */
	@Override
	protected ObjectNode toJson() {
		return super.toJson().put("timestamp", this.timestamp);
	}

	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public Path getPath() {
		UnixPathsHandler pathHandler = new UnixPathsHandler();
		return Paths.get(pathHandler.joinPaths(super.getDirectory(), super.getName()));

	}
}
