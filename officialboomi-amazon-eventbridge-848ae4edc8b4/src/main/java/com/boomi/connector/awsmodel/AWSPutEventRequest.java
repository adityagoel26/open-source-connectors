/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.awsmodel;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.boomi.connector.util.AWSEventBridgeJsonConstants;

/**
 * @author swastik.vn
 *
 */
public class AWSPutEventRequest {
	
	private String eventbusName;
	private String source;
	private String detail;
	private String detailType;
	private List<String> resources;
	

	public String getEventbusName() {
		return eventbusName;
	}

	@JsonProperty(AWSEventBridgeJsonConstants.EVENTBUSNAME)
	public void setEventbusName(String eventbusName) {
		this.eventbusName = eventbusName;
	}
	
	public String getSource() {
		return source;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.SOURCE)
	public void setSource(String source) {
		this.source = source;
	}
	

	public String getDetail() {
		return detail;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.DETAIL)
	public void setDetail(String detail) {
		this.detail = detail;
	}
	
	public String getDetailType() {
		return detailType;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.DETAILTYPE)
	public void setDetailType(String detailType) {
		this.detailType = detailType;
	}
	
	public List<String> getResources() {
		return resources;
	}
	@JsonProperty(AWSEventBridgeJsonConstants.RESOURCES)
	public void setResources(List<String> resources) {
		this.resources = resources;
	}

}
