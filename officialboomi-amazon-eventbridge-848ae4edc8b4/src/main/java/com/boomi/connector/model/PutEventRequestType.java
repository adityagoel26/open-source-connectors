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
public class PutEventRequestType {

	private List<PutEventRequest> request;

	public List<PutEventRequest> getRequest() {
		return request;
	}

	@JsonProperty(AWSEventBridgeJsonConstants.ENTRIES)
	public void setRequest(List<PutEventRequest> request) {
		this.request = request;
	}

}
