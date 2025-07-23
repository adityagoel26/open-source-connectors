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
public class PutEventResponse {
	
	private String eventId;

	public String getEventId() {
		return eventId;
	}
	
	@JsonProperty(AWSEventBridgeJsonConstants.EVENTID)
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	
	

}