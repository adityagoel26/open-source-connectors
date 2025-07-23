//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadMetadataFactory;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The Class GetSFTPFileMetadata.
 * @author sweta.b.das
 */
public class GetSFTPFileMetadata implements PayloadMetadata, SFTPMetadata {

	/** The input stream. */
	private final InputStream inputStream;

	/** The name. */
	private final String fileName;

	/** The time stamp. */
	private final String timeStamp;

	/**
	 * Instantiates a new gets the SFTP file metadata.
	 *
	 * @param inputStream the input stream
	 * @param fileName the file name
	 * @param timeStamp the time stamp
	 */
	public GetSFTPFileMetadata(InputStream inputStream, String fileName, String timeStamp) {
		this.inputStream = inputStream;
		this.fileName = fileName;
		this.timeStamp = timeStamp;
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
		metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, this.fileName);
		return metadata;
	}

	/**
	 * To json payload.
	 *
	 * @return the payload
	 */
	@Override
	public Payload toJsonPayload() {
		return null;
	}

	/**
	 * Gets the input stream.
	 *
	 * @return the input stream
	 */
	@Override
	public InputStream getInputStream() {
		return this.inputStream;
	}

	/**
	 * Gets the timestamp.
	 *
	 * @return the timestamp
	 */
	@Override
	public String getTimestamp() {
		return this.timeStamp;
	}

	/**
	 * To json.
	 *
	 * @return the object node
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String toJson() throws IOException {
		ObjectNode jsonNode;
		try(InputStreamReader input = new InputStreamReader(inputStream, StandardCharsets.UTF_8); BufferedReader reader = new BufferedReader(input)){
		String text = reader.lines()
				.collect(Collectors.joining("\n"));
		
		jsonNode = (JSONUtil.newObjectNode().put(SFTPConstants.PROPERTY_FILE_CONTENT, text))
				.put(SFTPConstants.MODIFIED_DATE, this.timeStamp);
		}
		return JSONUtil.getDefaultObjectMapper().writeValueAsString(jsonNode);
	}

	/**
	 * Sets the tracked property.
	 *
	 * @param name the name
	 * @param value the value
	 */
	@Override
	public void setTrackedProperty(String name, String value) {
		// Do nothing.
	}

	/**
	 * Sets the user defined property.
	 *
	 * @param name the name
	 * @param value the value
	 */
	@Override
	public void setUserDefinedProperty(String name, String value) {
		// Do nothing.
	}

	

}
