// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import java.util.ArrayList;
import java.util.List;

import com.boomi.connector.api.TrackedData;

/**
 * The Class InputWrapper contains logic to batch input data provided in
 * request.
 *
 */
public abstract class InputWrapper {

	/** The succeeded records. */
	List<TrackedData> succeededRecords = new ArrayList<>();

	/** The app error records. */
	List<TrackedDataWrapper> appErrorRecords = new ArrayList<>();

	/** The failed records. */
	List<TrackedData> failedRecords = new ArrayList<>();

	/** The batch counter. */
	int batchCounter = 0;

	/**
	 * Gets the failed records.
	 *
	 * @return the failed records
	 */
	public List<TrackedDataWrapper> getAppErrorRecords() {
		return appErrorRecords;
	}

	/**
	 * Gets the succeeded records.
	 *
	 * @return the succeeded records
	 */
	public List<TrackedData> getSucceededRecords() {
		return succeededRecords;
	}

	/**
	 * Gets the failed records.
	 *
	 * @return the failed records
	 */
	public List<TrackedData> getFailedRecords() {
		return failedRecords;
	}

	/**
	 * Gets the batch counter.
	 *
	 * @return the batch counter
	 */
	public int getBatchCounter() {
		return batchCounter;
	}

	/**
	 * Checks if next input batch is available.
	 *
	 * @return true, if successful
	 */
	abstract boolean hasNext();

	/**
	 * Prepares the next batch of input data to be processed in the operation by
	 * parsing the input .
	 *
	 * @return the list
	 */
	public abstract List<TrackedDataWrapper> next();

	/**
	 * Gets the total input records count.
	 *
	 * @return the total input records count
	 */
	public int getTotalInputRecordsCount() {
		return getSucceededRecords().size() + getAppErrorRecords().size() + getFailedRecords().size();
	}

}
