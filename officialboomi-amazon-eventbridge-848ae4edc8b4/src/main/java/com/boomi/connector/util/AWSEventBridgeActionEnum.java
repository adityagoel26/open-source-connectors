/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.util;

/**
 * @author a.kumar.samantaray
 *
 */

/**
 * The AWSEventBridgeActionEnum Enum wraps all the events available in AWS. You
 * can use this to retrieve the events under corresponding operations.
 * 
 */
public enum AWSEventBridgeActionEnum {

	EVENTS(Constants.CREATE);
	public final String action;

	private AWSEventBridgeActionEnum(String action) {
		this.action = action;
	}

	public String showValue() {
		return action;
	}

	private static class Constants {

		public static final String CREATE = "CREATE";
	}
}
