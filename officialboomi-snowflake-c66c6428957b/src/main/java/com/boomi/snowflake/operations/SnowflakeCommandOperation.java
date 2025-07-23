// Copyright (c) 2024 Boomi, LP.

package com.boomi.snowflake.operations;

import java.io.InputStream;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.util.IOUtil;

public class SnowflakeCommandOperation extends BaseUpdateOperation{

	public SnowflakeCommandOperation(SnowflakeConnection conn) {
		super(conn);
	}

	@Override
	public void executeUpdate(UpdateRequest request, OperationResponse response) {
		ConnectionProperties properties = null;
		SnowflakeWrapper wrapper = null;
		try {
			properties = new ConnectionProperties(getConnection(), getContext().getOperationProperties(),
					getContext().getObjectTypeId(), response.getLogger());
			wrapper = new SnowflakeWrapper(properties.getConnectionGetter(), properties.getConnectionTimeFormat(),
					properties.getLogger(), properties.getTableName());
			for (ObjectData requestData : request) {
				executeAndSetResponse(response, properties, wrapper, requestData);
			}
		} catch(Exception e) {
			throw new ConnectorException(e);
		} finally {
			if (wrapper != null) {
				wrapper.close();
			} else {
				if(properties != null) {
					properties.commitAndClose();
				}
			}
		}
	}

	private void executeAndSetResponse(OperationResponse response, ConnectionProperties properties,
			SnowflakeWrapper wrapper, ObjectData requestData) {
		InputStream result = null;
		try {
			wrapper.setPreparedStatement(
					wrapper.createPreparedStatement(properties.getSnowflakeCommand().getSQLString(requestData)
					,requestData.getDynamicOperationProperties()));
			wrapper.executePreparedStatement();
			result = wrapper.readFromResultSet();
			ResponseUtil.addSuccess(response, requestData, "0", ResponseUtil.toPayload(result));
		} catch (ConnectorException e) {
			SnowflakeOperationUtil.handleConnectorException(response, requestData, e);
		} catch (Exception e) {
			SnowflakeOperationUtil.handleGeneralException(response, requestData, e);
		} finally {
			IOUtil.closeQuietly(result);
		}
	}

	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}
