// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import com.boomi.connector.testutil.SimpleGetRequest;
import com.boomi.connector.testutil.SimpleTrackedData;

public class ModifiedGetRequest extends SimpleGetRequest {

	public ModifiedGetRequest(String objectId, ModifiedSimpleOperationResponse response) {
		super(new SimpleTrackedData(0, objectId));
		response.addTrackedData((SimpleTrackedData) getObjectId());
	}
}
