// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.boomi.util.LogUtil;

/**
 * The Class ConnectionTimeFormat
 * @author s.vanangudi
 *
 */
public class ConnectionTimeFormat {

	/** The Constant LOG. */
	private static final Logger LOG = LogUtil.getLogger(ConnectionTimeFormat.class);
	/** The Constant DATEFORMATS. */
	private static final String DATEFORMATS [] = {
			"DATE_INPUT_FORMAT",
			"DATE_OUTPUT_FORMAT"
	};
	/** The Constant TIMEFORMATS. */
	private static final String TIMEFORMATS [] = {
			"TIME_INPUT_FORMAT",
			"TIME_OUTPUT_FORMAT"
	};
	/** The Constant TIMESTAMPFORMATS. */
	private static final String TIMESTAMPFORMATS [] = {
			"TIMESTAMP_INPUT_FORMAT",
			"TIMESTAMP_OUTPUT_FORMAT",
			"TIMESTAMP_TZ_OUTPUT_FORMAT",
			"TIMESTAMP_NTZ_OUTPUT_FORMAT",
			"TIMESTAMP_LTZ_OUTPUT_FORMAT"
	};
	/** The Constant DEFAULT_DATE_FORMAT. */
	private static final String DEFAULT_DATE_FORMAT = "MMddyyyy";
	/** The default Date Time Format. */
	private String _dateTimeFormat = "yyyyMMdd HHmmss.SSS";
	/** The Constant Time Format. */
	private String _timeFormat = "HHmmss.SSS";
	/** The Constant Date Format. */
	private String _dateFormat = DEFAULT_DATE_FORMAT;
	/** The Snowflake Date Time Map. */
	private Map<String, String> _snowflakeDateTime;
	/** The Constant AUTO. */
	private static final String AUTO = "auto";
	
	/**
	 * Gets the Date Time Format
	 * @return _dateTimeFormat String
	 */
	public String getDateTimeFormat() {
		return _dateTimeFormat;
	}

	/**
	 * Gets the Time Format
	 * @return _timeFormat String
	 */
	public String getTimeFormat() {
		return _timeFormat;
	}

	/**
	 * Gets the Date Format
	 * @return _dateFormat String
	 */
	public String getDateFormat() {
		return _dateFormat;
	}

	/**
	 * @param dateFormat     format for date
	 * @param timeFormat     format for time
	 * @param dateTimeFormat format for date and time
	 */
	public ConnectionTimeFormat(String dateFormat, String timeFormat, String dateTimeFormat) {
		LOG.entering(this.getClass().getCanonicalName(), "ConnectionTimeFormat()");
		_snowflakeDateTime = new HashMap<>();
		// timestamp
		_snowflakeDateTime.put("yyyyMMdd HHmmss.SSS", "YYYYMMDD\" \"HH24MISS.FF3");
		_snowflakeDateTime.put("yyyyMMdd HHmmss", "YYYYMMDD\" \"HH24MISS");
		_snowflakeDateTime.put("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd\"T\"HH24:MI:SS");
		_snowflakeDateTime.put("yyyy-MM-dd'T'HH:mm:ssZ", "YYYY-MM-DDTHH24:MI:SSTZHTZM");
		_snowflakeDateTime.put("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","yyyy-MM-dd\"T\"HH24:MI:SS.FF3\"Z\"");
		// time
		_snowflakeDateTime.put("HHmmss.SSS", "HH24MISS.FF3");
		// date
		_snowflakeDateTime.put(DEFAULT_DATE_FORMAT, DEFAULT_DATE_FORMAT);
		_snowflakeDateTime.put("MM/dd/yyyy", "MM/dd/yyyy");
		_snowflakeDateTime.put("MM-dd-yyyy", "MM-dd-yyyy");
		_snowflakeDateTime.put("MMddyy", "MMddyy");
		_snowflakeDateTime.put("yyyy-MM-dd", "yyyy-MM-dd");
		// for all the date, time and datetime types that is not supported by boomi but supported by Snowflake
		_snowflakeDateTime.put("auto", "AUTO");

		if (valid(dateFormat)) {
			_dateFormat = dateFormat;
		}
		if (valid(timeFormat)) {
			_timeFormat = timeFormat;
		}
		if (valid(dateTimeFormat)) {
			_dateTimeFormat = dateTimeFormat;
		}
	}

	/**
	 * deprecated validation for the date/time/timestamp format for the security injections
	 * 
	 * @param format
	 * @return true if it is valid format
	 */
	private boolean valid(String format) {
		return format != null && _snowflakeDateTime.containsKey(format);
	}

	/**
	 * this function sets output/input formats for the connection
	 * 
	 * @param connection JDBC connection
	 */
	public Properties getFormats() {
		LOG.entering(this.getClass().getCanonicalName(), "getFormats()");
		Properties formatProperties = new Properties();
		
		for (String FORMAT : DATEFORMATS) {
			if(_snowflakeDateTime.get(_dateFormat).equalsIgnoreCase(AUTO) 
					&& !(FORMAT.equalsIgnoreCase(SnowflakeDataTypeConstants.DATE_INPUT_FORMAT))) {
				formatProperties.put(FORMAT, _snowflakeDateTime.get(SnowflakeDataTypeConstants.DATEFORMAT));
			}else {
				formatProperties.put(FORMAT, _snowflakeDateTime.get(_dateFormat));
			}
		}
		
		for (String FORMAT : TIMEFORMATS) {
			if(_snowflakeDateTime.get(_timeFormat).equalsIgnoreCase(AUTO) 
					&& !(FORMAT.equalsIgnoreCase(SnowflakeDataTypeConstants.TIME_INPUT_FORMAT))) {
				formatProperties.put(FORMAT, _snowflakeDateTime.get(SnowflakeDataTypeConstants.TIMEFORMAT));
			}else {
				formatProperties.put(FORMAT, _snowflakeDateTime.get(_timeFormat));
			}
		}
			
		for (String FORMAT : TIMESTAMPFORMATS) {
			if(_snowflakeDateTime.get(_dateTimeFormat).equalsIgnoreCase(AUTO) 
					&& !(FORMAT.equalsIgnoreCase(SnowflakeDataTypeConstants.TIMESTAMP_INPUT_FORMAT))) {
				formatProperties.put(FORMAT, _snowflakeDateTime.get(SnowflakeDataTypeConstants.DATETIMEFORMAT));
			}else {
				formatProperties.put(FORMAT, _snowflakeDateTime.get(_dateTimeFormat));
			}
		}
			
		return formatProperties;
	}
}
