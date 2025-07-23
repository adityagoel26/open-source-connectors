/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.model;

import java.util.List;

import com.boomi.connector.util.AWSEventBridgeJsonConstants;
import com.fasterxml.jackson.annotation.JsonProperty;
/**
 * @author swastik.vn
 *
 */
public class PutEventSchemaType {
	
	private List<PutEventSchema> request;

	public List<PutEventSchema> getRequest() {
		return request;
	}

	@JsonProperty(AWSEventBridgeJsonConstants.ENTRIES)
	public void setRequest(List<PutEventSchema> request) {
		this.request = request;
	}

}
