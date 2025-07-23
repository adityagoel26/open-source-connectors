// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public final class SampleConnection {

	private SampleConnection() {
		// Prevent initialization
	}

	public  static Connection getJDBCConnection() {
		Properties connectionProperties = new Properties();
		Connection connection = null;
		try {
			connectionProperties.put("user", System.getProperty("UserName"));
			connectionProperties.put("password", System.getProperty("Password"));
			connectionProperties.put("db", "\"test_DB\"");
			connectionProperties.put("schema", "PUBLIC");
			connectionProperties.put("warehouse", "SPEC_WH");
			connectionProperties.put("role", "SYSADMIN");
			connection = DriverManager.getConnection("jdbc:snowflake://boomi.us-east-1.snowflakecomputing.com", connectionProperties);
		} catch (Exception e) {
			// if exception is thrown, connection is always null
		}
		return connection;
	}
}
