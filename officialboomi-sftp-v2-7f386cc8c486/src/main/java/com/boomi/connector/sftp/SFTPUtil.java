//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.StringUtil;

import java.text.MessageFormat;
import java.text.ParseException;
/**
 * The Class SFTPUtil.
 *
 * @author Omesh Deoli
 * 
 */
public class SFTPUtil {
	
	/**
	 * Instantiates a new SFTP util.
	 */
	private SFTPUtil() {
	    throw new IllegalStateException("Utility class");
	  }

	
	/** The Constant TIMESTAMP_FORMAT. */
	private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	
	/** The Constant ERROR_INVALID_TIMESTAMP. */
	private static final String ERROR_INVALID_TIMESTAMP = MessageFormat
			.format(SFTPConstants.ERROR_INVALID_TIMESTAMP_INPUT, TIMESTAMP_FORMAT);

	/**
	 * Gets the doc property.
	 *
	 * @param input the input
	 * @param propName the prop name
	 * @return the doc property
	 */
	public static String getDocProperty(TrackedData input, String propName) {
		return StringUtil.trim(input.getDynamicProperties().get(propName));
	}

	/**
	 * Parses the date.
	 *
	 * @param dateStr the date str
	 * @return the date
	 */
	public static Date parseDate(String dateStr) {
		try {
			return SFTPUtil.getDateTimeFormat().parse(dateStr);
		} catch (ParseException e) {
			throw new ConnectorException(ERROR_INVALID_TIMESTAMP, (Throwable) e);
		}
	}

	/**
	 * Parses the date.
	 *
	 * @param dateInSeconds the date in seconds
	 * @return the date
	 */
	public static Date parseDate(int dateInSeconds) {

		return new Date(dateInSeconds * 1000L);
	}

	/**
	 * Format date.
	 *
	 * @param date the date
	 * @return the string
	 */
	public static String formatDate(Date date) {
		return SFTPUtil.getDateTimeFormat().format(date);
	}

	/**
	 * Gets the date time format.
	 *
	 * @return the date time format
	 */
	private static SimpleDateFormat getDateTimeFormat() {
		return new SimpleDateFormat(TIMESTAMP_FORMAT);
	}

}
