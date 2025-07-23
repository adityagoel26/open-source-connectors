//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.bean;

/**
 * @author abhijit.d.mishra
 **/
public class UpdateOperationRequest {
	
	/** The id property. */
	private String id;
	
	/** The partitionKey property. */
	private String partitionKey;
	
	/** The entityData property. */
	private String entityData;
	
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
	
	/**
	 * Gets the entityData
	 * @return the entityData
	 */
	public String getEntityData() {
		return entityData;
	}
	
	/**
	 * Sets the entityData
	 * @param entityData
	 */
	public void setEntityData(String entityData) {
		this.entityData = entityData;
	}
}
