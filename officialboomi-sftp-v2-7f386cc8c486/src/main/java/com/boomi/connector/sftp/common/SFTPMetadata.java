//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.common;

import java.io.InputStream;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadMetadataFactory;


/**
 * The Interface SFTPMetadata.
 * @author sweta.b.das
 */
public interface SFTPMetadata {
	
	/**
	 * Gets the input stream.
	 *
	 * @return the input stream
	 */
	public InputStream getInputStream();

	/**
	 * Gets the name.
	 *
	 * @return the name
	 */
	public String getTimestamp();

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
