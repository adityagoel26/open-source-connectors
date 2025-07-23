//Copyright (c) 2020 Boomi, Inc.

/*
 * Decompiled with CFR 0.137.
 * 
 * Could not load the following classes:
 *  com.boomi.connector.api.OperationStatus
 */
package com.boomi.connector.liveoptics.utils;

import com.boomi.connector.api.OperationStatus;

/**
 * @author Naveen Ganachari
 *
 *         ${tags}
 */

public class LiveOpticsResponse {
	
	private int responseCode;
	private String responseMsg;
	private String sessionToken;

	public LiveOpticsResponse() {
		//Empty constructor.
	}
	
	
	/**
	 * This method is used to achieve ResponseCode
	 * 
	 * @return int The response code
	 */
	public int getResponseCode() {
		return this.responseCode;
	}
	
		
	/**
	 * This method is used to set ResponseCode
	 * 
	 * @param responseCode The response code
	 */
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	
	/**
	 * This method is used to achieve ResponseCode as String
	 * 
	 * @return String The response code as type String
	 */
	public String getResponseCodeAsString() {
		return String.valueOf(this.responseCode);
	}

		
	/**
	 * This method is used to achieve ResponseMessage
	 * 
	 * @return String The response message
	 */
	public String getResponseMessage() {
		return this.responseMsg;
	}
	
	/**
	 * This method is used to set ResponseMessage
	 * 
	 * @param responseMsg The response message to set
	 */
	public void setResponseMessage(String responseMsg) {
		this.responseMsg = responseMsg;
	}

	/**
	 * This method is used to achieve Operation Status
	 * 
	 * @return OperationStatus The Operation Status
	 */
	public OperationStatus getStatus() {
		if (this.responseCode >= 200 && this.responseCode < 300) {
			return OperationStatus.SUCCESS;
		}
		if ((this.responseCode >= 300 && this.responseCode < 600)) {
			return OperationStatus.APPLICATION_ERROR;
		}
		return OperationStatus.FAILURE;
	}
	
		
	/**
	 * This method is used to get sessionToken
	 * 
	 * @return String The session token
	 */
	public String getSessionToken() {
		return sessionToken;
	}
	
	
	/**
	 * This method is used to set sessionToken
	 * 
	 * @param sessionToken Session token to set
	 */
	public void setSessionToken(String sessionToken) {
		this.sessionToken = sessionToken;
	}
}
