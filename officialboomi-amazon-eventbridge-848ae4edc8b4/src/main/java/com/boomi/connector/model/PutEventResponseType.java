/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.boomi.connector.util.AWSEventBridgeJsonConstants;
/**
 * @author a.kumar.samantaray
 *
 */
public class PutEventResponseType {
	private List<PutEventResponse> response;

	public List<PutEventResponse> getResponse() {
		return response;
	}
	@JsonProperty(AWSEventBridgeJsonConstants.ENTRIES)
	public void setResponse(List<PutEventResponse> response) {
		this.response = response;
	}

}
