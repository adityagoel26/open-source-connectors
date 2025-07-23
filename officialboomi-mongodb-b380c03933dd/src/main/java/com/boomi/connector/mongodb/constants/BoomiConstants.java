// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.constants;

import com.boomi.util.ByteUnit;

/**
 * The Class BoomiConstants.
 * 
 */
public class BoomiConstants {

	/**
	 * Instantiates a new boomi constants.
	 */
	private BoomiConstants() {
		throw new IllegalStateException("Utility class");
	}

	/** The Constant DEFAULT_STATUS_MESSAGE. */
	public static final String DEFAULT_STATUS_MESSAGE = "max size exceeded";

	/** The Constant DEFAULT_STATUS_CODE. */
	public static final String DEFAULT_STATUS_CODE = "413";

	/** The Constant MAX_SIZE. */
	public static final long MAX_SIZE = ByteUnit.MB.getByteUnitSize();

}
