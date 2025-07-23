//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.actions.RetryableQueryAction;
import com.boomi.connector.sftp.handlers.QueryHandler;
import com.boomi.connector.sftp.results.BaseResult;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * The Class ListResultBuilder.
 *
 * @author Omesh Deoli
 * 
 *
 */
public class ListResultBuilder implements ResultBuilder {

	/**
	 * Make result.
	 *
	 * @param meta the meta
	 * @param dirFullPath the dir full path
	 * @param action the action
	 * @return the base result
	 */
	@Override
	public BaseResult makeResult(LsEntry meta,String dirFullPath , RetryableQueryAction action, PropertyMap propertyMap, OperationResponse operationResponse) {
		
		return new BaseResult(QueryHandler.makeJsonPayload(meta,dirFullPath));
	}
	
}
