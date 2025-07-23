// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.operation;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;


/**
 * @author a.kumar.samantaray
 *
 */
public enum CustomOperationType {
	EXECUTE, SEND, LISTEN;

	/**
     * This enum method returns Operation Type from the browser context.
     * @param context
     * @return {@link CustomOperationType}
     * 
     */
	public static CustomOperationType fromContext(BrowseContext context) {
		String operationType = context.getCustomOperationType();
		try {
			return valueOf(operationType);
		} catch (IllegalArgumentException e) {
			throw new ConnectorException("Invalid custom operation type: " + operationType);
		}
	}
}
