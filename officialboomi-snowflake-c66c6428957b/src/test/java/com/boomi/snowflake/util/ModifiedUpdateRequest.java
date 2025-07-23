// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import java.io.InputStream;
import java.util.List;

import com.boomi.connector.testutil.SimpleUpdateRequest;

public class ModifiedUpdateRequest extends SimpleUpdateRequest {

	public ModifiedUpdateRequest(List<InputStream> inputs, ModifiedSimpleOperationResponse response) {
		super(ModifiedRequestUtil.getTrackedDataIter(response, ModifiedRequestUtil.getData(inputs)));
	}
}
