//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.bean;

import com.boomi.connector.cosmosdb.util.CosmosDbConstants;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class GetOperationRequest {

	/** The id property. */
	@JsonProperty(CosmosDbConstants.ID)
	private String id;
	
	/** The partitionKey property. */
	@JsonProperty(CosmosDbConstants.PART_KEY)
	private String partitionKey;

	/**
	 * gets the id
	 * @return id
	 */
	public String getId() {
		return id;
	}

	/**
	 * sets the id
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * gets the partitionKey
	 * @return partitionKey
	 */
	public String getPartitionKey() {
		return partitionKey;
	}

	/**
	 * sets the partitionKey
	 * @param partitionKey
	 */
	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

}
