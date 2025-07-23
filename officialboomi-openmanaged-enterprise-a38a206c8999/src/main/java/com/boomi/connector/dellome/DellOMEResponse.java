// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.logging.Logger;
import javax.net.ssl.HttpsURLConnection;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.dellome.exception.DellOMEConnectorException;


/**
 * Wraps the Connection to an OME API endpoint
 */
public class DellOMEResponse {
	
	private final HttpsURLConnection _conn;
	private final int _responseCode;
	private final String _responseMsg;
	private Logger logger = Logger.getLogger(DellOMEResponse.class.getName());

	public DellOMEResponse(HttpsURLConnection conn) throws DellOMEConnectorException {
		_conn = conn;
		try {
			_responseCode = conn.getResponseCode();
			_responseMsg = conn.getResponseMessage();
			logger.info("DellOMEResponse::DellOMEResponse:  " + conn.getResponseCode() 
													+ " " + conn.getResponseMessage());
		} catch (IOException e) {

			logger.severe("DellOMEResponse::DellOMEResponse:  " + e.getMessage());
			throw new DellOMEConnectorException("Could not instantiate DellOMEResponse", e);
		}

	}

	public InputStream getResponse() throws DellOMEConnectorException {
		try {
			return _conn.getInputStream();
		} catch (IOException e) {
			if (OperationStatus.SUCCESS == getStatus()) {
		    	logger.severe("DellOMEResponse::DellOMEResponse -"
		    			+ "Could not get the input stream: " + e.getMessage());
			   throw new DellOMEConnectorException("Could not get the input stream" + e);
		   }
			return _conn.getErrorStream();
		}
	}
	
	public InputStream getErrorResponse() {
		return _conn.getErrorStream();
	}

	public int getResponseCode() {
		return _responseCode;
	}

	public String getResponseCodeAsString() {
		return String.valueOf(_responseCode);
	}

	public String getResponseMessage() {
		return _responseMsg;
	}

	/**
	 * Returns the OperationStatus for the given http code.
	 * 
	 * @return SUCCESS if the code indicates success, FAILURE otherwise
	 */
	public OperationStatus getStatus() {
		// success: 200 <= code < 300
		if (_responseCode >= HttpURLConnection.HTTP_OK && _responseCode < HttpURLConnection.HTTP_MULT_CHOICE) {
			return OperationStatus.SUCCESS;
		} else if (_responseCode >= HttpURLConnection.HTTP_MULT_CHOICE
				&& _responseCode < HttpURLConnection.HTTP_INTERNAL_ERROR) {
			return OperationStatus.APPLICATION_ERROR;
		} else {
			return OperationStatus.FAILURE;
		}
	}

	/**
	 * Disconnects the underlying connection 
	 */
	public void close() {
		if (null != _conn) {
			_conn.disconnect();
		}
	}

}
