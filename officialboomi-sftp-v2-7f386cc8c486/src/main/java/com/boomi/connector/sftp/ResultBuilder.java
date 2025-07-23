//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import java.io.IOException;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.actions.RetryableQueryAction;
import com.boomi.connector.sftp.results.BaseResult;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * The Interface ResultBuilder.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public interface ResultBuilder {

			/**
			 * Make result.
			 *
			 * @param var2 the var 2
			 * @param dirFullPath the dir full path
			 * @param action the action
			 * @param propertyMap 
			 * @param operationResponse 
			 * @return the base result
			 * @throws IOException Signals that an I/O exception has occurred.
			 */
			public BaseResult makeResult(LsEntry var2,String dirFullPath ,RetryableQueryAction action, PropertyMap propertyMap, OperationResponse operationResponse) throws IOException;
	
}
