// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome.exception;

public class DellOMEConnectorException extends Exception {

	private static final long serialVersionUID = 1L;

	public DellOMEConnectorException(String message) {
		super(message);
	}

	public DellOMEConnectorException(String string, Exception ex) {
		super(string, ex);
	}

	public DellOMEConnectorException(Exception ex) {
		super(ex);
	}

}
