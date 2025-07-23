//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

 /**
  * @author Omesh Deoli
  *
  * 
  */
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadMetadataFactory;

/**
 * The Interface SFTPFileMetadata.
 */
public interface SFTPFileMetadata {
	
	/**
	 * Gets the directory.
	 *
	 * @return the directory
	 */
	public String getDirectory();

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getName();

	/**
	 * To payload metadata.
	 *
	 * @param var1 the var 1
	 * @return the payload metadata
	 */
	public PayloadMetadata toPayloadMetadata(PayloadMetadataFactory var1);

	/**
	 * To json payload.
	 *
	 * @return the payload
	 */
	public Payload toJsonPayload();
}
