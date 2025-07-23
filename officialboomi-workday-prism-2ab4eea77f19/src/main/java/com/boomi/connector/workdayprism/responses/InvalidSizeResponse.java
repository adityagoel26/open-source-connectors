//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.responses;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InvalidSizeResponse implements Serializable {
    private static final long serialVersionUID = -201808289854L;

    private String error;
    private long size;
    
    @JsonCreator
	public InvalidSizeResponse(@JsonProperty("error") String error, @JsonProperty("size") long size) {
		super();
		this.error = error;
		this.size = size;
	}

	public String getError() {
		return error;
	}

	public long getSize() {
		return size;
	}
    
    

}
