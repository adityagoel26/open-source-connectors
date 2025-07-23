// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


/**
 * The Class BatchResponseWithId.
 * @author sweta.b.das
 */
public class BatchResponseWithId extends BatchResponse {

	/** The ids. */
	@JsonProperty("Inserted Ids ")
	public List<Integer> ids;
	
	/**
	 * Instantiates a new batch response.
	 *
	 * @param status       the status
	 * @param batchNumber  the batch number
	 * @param ids the ids
	 * @param recordNumber the record number
	 */
	public BatchResponseWithId(String status, int batchNumber, List<Integer> ids, int recordNumber) {
		super(status, batchNumber, recordNumber);
		this.ids = ids;
			
	}

}
