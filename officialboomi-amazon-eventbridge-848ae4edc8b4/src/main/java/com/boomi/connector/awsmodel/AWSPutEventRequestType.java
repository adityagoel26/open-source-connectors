/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.awsmodel;

import java.util.List;

import com.boomi.connector.util.AWSEventBridgeJsonConstants;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * @author swastik.vn
 *
 */
public class AWSPutEventRequestType {
	private List<AWSPutEventRequest> request;

	public List<AWSPutEventRequest> getRequest() {
		return request;
	}

	@JsonProperty(AWSEventBridgeJsonConstants.ENTRIES)
	public void setRequest(List<AWSPutEventRequest> request) {
		this.request = request;
	}
}
