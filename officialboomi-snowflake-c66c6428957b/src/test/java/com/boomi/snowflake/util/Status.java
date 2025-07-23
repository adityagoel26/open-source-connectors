// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.util;

import com.boomi.connector.api.OperationStatus;

public class Status {
	public OperationStatus operationStatus;
	public String statusCode;
	public String statusMessage;
	public Throwable thrown;
	
	public Status (Object obj) {
		
	}
}
