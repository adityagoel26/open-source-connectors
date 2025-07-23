//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.bean;


/**
 * @author swastik.vn
 *
 */
public class CreateOperationRequest {
	
	/** The id requestId. */
	private String requestId;
	
	/** The id partitionValue. */
	private String partitionValue;
	
	/** The id entity. */
	private String entity;
	
	/** The id statusCode. */
	private int statusCode;
	
	/**
	 * Gets the statusCode
	 * @return statusCode
	 */
	public int getStatusCode() {
		return statusCode;
	}
	
	/**
	 * Sets the statusCode
	 * @param statusCode
	 */
	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
	
	/**
	 * Gets the requestId
	 * @return requestId
	 */
	public String getRequestId() {
		return requestId;
	}
	
	/**
	 * Sets the requestId
	 * @param requestId
	 */
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	
	/**
	 * Gets the partitionValue
	 * @return partitionValue
	 */
	public String getPartitionValue() {
		return partitionValue;
	}
	
	/**
	 * Sets the partitionValue
	 * @param partitionValue
	 */
	public void setPartitionValue(String partitionValue) {
		this.partitionValue = partitionValue;
	}
	
	/**
	 * Gets the entity
	 * @return entity
	 */
	public String getEntity() {
		return entity;
	}
	
	/**
	 * Sets the entity
	 * @param entity
	 */
	public void setEntity(String entity) {
		this.entity = entity;
	}
	
}
