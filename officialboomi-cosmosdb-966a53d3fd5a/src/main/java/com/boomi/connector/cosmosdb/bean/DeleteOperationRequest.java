//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.bean;

import com.boomi.connector.cosmosdb.util.CosmosDbConstants;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class DeleteOperationRequest {
	
	/** The id property. */
	@JsonProperty(CosmosDbConstants.DELETE_ID)
	private String id;
	
	/** The id partitionKey. */
	@JsonProperty(CosmosDbConstants.DELETE_PART_KEY)
	private String partitionKey;

	/**
	 * Gets the id
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets the id
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Gets the partitionKey
	 * @return the partitionKey
	 */
	public String getPartitionKey() {
		return partitionKey;
	}

	/**
	 * Sets the partitionKey
	 * @param partitionKey
	 */
	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

}
