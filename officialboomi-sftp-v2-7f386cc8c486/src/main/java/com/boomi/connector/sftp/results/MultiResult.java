//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.results;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.constants.SFTPConstants;

/**
 * The Class MultiResult.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class MultiResult {

	/** The response. */
	private final OperationResponse response;
	
	/** The input. */
	private final FilterData input;
	
	/** The size. */
	private int size;

	/**
	 * Instantiates a new multi result.
	 *
	 * @param response the response
	 * @param input the input
	 */
	public MultiResult(OperationResponse response, FilterData input) {
		this.response = response;
		this.input = input;
	}

	/**
	 * Adds the partial result.
	 *
	 * @param dirFullPath the dir full path
	 * @param fileName the file name
	 * @param result the result
	 */
	public void addPartialResult(String dirFullPath, String fileName, BaseResult result) {
		if (result.getStatus() == OperationStatus.APPLICATION_ERROR || result.getStatus() == OperationStatus.FAILURE) {

			this.addPartialErrorResult(dirFullPath, fileName, result);
		} else {
			this.addPartialSuccessResult(dirFullPath, fileName, result);
		}
	}

	/**
	 * Adds the partial result.
	 *
	 * @param e the e
	 */
	public void addPartialResult(ErrorResult e) {
		this.response.addPartialResult((TrackedData) this.input, e.getStatus(), e.getStatusCode(),
				e.getStatusMessage(), null);
		++this.size;
	}
	
	/**
	 * Adds the partial error result.
	 *
	 * @param dirFullPath the dir full path
	 * @param fileName the file name
	 * @param result the result
	 */
	private void addPartialErrorResult(String dirFullPath, String fileName, BaseResult result) {
		PayloadMetadata metadata = response.createMetadata();
		metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, fileName);
		metadata.setTrackedProperty(SFTPConstants.REMOTE_DIRECTORY, dirFullPath);
		this.response.addPartialResult((TrackedData) this.input, result.getStatus(), result.getStatusCode(),
				"Error getting file " + fileName, null);
		++this.size;

	}

	/**
	 * Adds the partial success result.
	 *
	 * @param dirFullPath the dir full path
	 * @param filename the filename
	 * @param result the result
	 */
	public void addPartialSuccessResult(String dirFullPath, String filename, BaseResult result) {
		PayloadMetadata metadata = response.createMetadata();
		metadata.setTrackedProperty(SFTPConstants.PROPERTY_FILENAME, filename);
		metadata.setTrackedProperty(SFTPConstants.REMOTE_DIRECTORY, dirFullPath);
		this.addPartial(result);
		++this.size;
	}

	/**
	 * Adds the partial.
	 *
	 * @param result the result
	 */
	private void addPartial(BaseResult result) {
		this.response.addPartialResult((TrackedData) this.input, result.getStatus(), result.getStatusCode(),
				result.getStatusMessage(), result.getPayload());
	}

	/**
	 * Finish.
	 */
	public void finish() {
		this.response.finishPartialResult((TrackedData) this.input);

	}

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public FilterData getInput() {
		return this.input;
	}

	/**
	 * Gets the size.
	 *
	 * @return the size
	 */
	public int getSize() {
		return this.size;
	}

	/**
	 * Checks if is empty.
	 *
	 * @return true, if is empty
	 */
	public boolean isEmpty() {
		return this.size == 0;
	}
}
