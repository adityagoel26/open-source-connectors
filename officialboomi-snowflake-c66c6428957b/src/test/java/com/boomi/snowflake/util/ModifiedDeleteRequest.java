// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import java.util.List;
import com.boomi.connector.testutil.SimpleDeleteRequest;

public class ModifiedDeleteRequest extends SimpleDeleteRequest {

	public ModifiedDeleteRequest(List<String> inputs, ModifiedSimpleOperationResponse response) {
		super(ModifiedRequestUtil.getTrackedDataIter(response, ModifiedRequestUtil.getData(inputs)));
	}
}
