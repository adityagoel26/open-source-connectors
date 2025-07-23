// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.bean;

import java.util.ArrayList;
import java.util.List;

import com.boomi.connector.mongodb.TrackedDataWrapper;
/**
 * The Class BatchResult.
 * 
 */
public class BatchResult {

	/** The mark batch as failed. */
	boolean markBatchAsFailed;
	
	/** The failed rec indexes. */
	List<Integer> failedRecIndexes;
	
	/** The failed records. */
	List<TrackedDataWrapper> failedRecords;

	/**
	 * Checks if is mark batch as failed.
	 *
	 * @return true, if is mark batch as failed
	 */
	public boolean isMarkBatchAsFailed() {
		return markBatchAsFailed;
	}

	/**
	 * Sets the mark batch as failed.
	 *
	 * @param markBatchAsFailed the new mark batch as failed
	 */
	public void setMarkBatchAsFailed(boolean markBatchAsFailed) {
		this.markBatchAsFailed = markBatchAsFailed;
	}

	/**
	 * Gets the failed record indexes.
	 *
	 * @return the failed rec indexes
	 */
	public List<Integer> getFailedRecIndexes() {
		return failedRecIndexes;
	}

	/**
	 * Sets the failed record indexes.
	 *
	 * @param failedRecIndexes the new failed rec indexes
	 */
	public void setFailedRecIndexes(List<Integer> failedRecIndexes) {
		this.failedRecIndexes = failedRecIndexes;
	}

	/**
	 * Gets the failed records.
	 *
	 * @return the failed records
	 */
	public List<TrackedDataWrapper> getFailedRecords() {
		return failedRecords;
	}

	/**
	 * Sets the failed records.
	 *
	 * @param failedRecords the new failed records
	 */
	public void setFailedRecords(List<TrackedDataWrapper> failedRecords) {
		this.failedRecords = failedRecords;
	}

	/**
	 * Reset the failed records.
	 */
	public void reset() {
		setMarkBatchAsFailed(false);
		getFailedRecIndexes().clear();
		getFailedRecords().clear();
	}

	/**
	 * Instantiates a new batch result.
	 */
	public BatchResult() {
		setMarkBatchAsFailed(false);
		setFailedRecIndexes(new ArrayList<Integer>());
		setFailedRecords(new ArrayList<TrackedDataWrapper>());
	}

}
