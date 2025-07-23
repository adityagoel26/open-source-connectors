/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.model;

import com.boomi.connector.util.AWSEventBridgeJsonConstants;
import com.fasterxml.jackson.annotation.JsonProperty;
/**
 * @author a.kumar.samantaray
 *
 */
public class PutEventRequest {
	
	private String eventbusName;
	private String source;
	private String detailType;
	private Object detail;
	

	public String getSource() {
		return source;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.SOURCE)
	public void setSource(String source) {
		this.source = source;
	}
	
	public Object getDetail() {
		return detail;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.DETAIL)
	public void setDetail(Object detail) {
		this.detail = detail;
	}
	
	public String getDetailType() {
		return detailType;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.DETAILTYPE)
	public void setDetailType(String detailType) {
		this.detailType = detailType;
	}
	
	public String getEventbusName() {
		return eventbusName;
	}

	@JsonProperty(AWSEventBridgeJsonConstants.EVENTBUSNAME)
	public void setEventbusName(String eventbusName) {
		this.eventbusName = eventbusName;
	}
}