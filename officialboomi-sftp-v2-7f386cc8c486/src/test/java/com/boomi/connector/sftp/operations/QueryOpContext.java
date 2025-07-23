//Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.Connector;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleOperationContext;

import java.util.Map;

public class QueryOpContext extends SimpleOperationContext{
	
	private String _customOperationType;
	
	@Override
	public String getCustomOperationType() {
		return _customOperationType;
	}


	public void set_customOperationType(String _customOperationType) {
		this._customOperationType = _customOperationType;
	}

	public QueryOpContext(AtomConfig config, Connector connector, OperationType opType, Map<String, Object> connProps,
			Map<String, Object> opProps, String objectTypeId, Map<ObjectDefinitionRole, String> cookies,String customOperationType) {
		super(config, connector, opType, connProps, opProps, objectTypeId, cookies);
		// TODO Auto-generated constructor stub
	this._customOperationType=customOperationType;
	}

}
