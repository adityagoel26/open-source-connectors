// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.util.BaseConnection;
import com.boomi.snowflake.pool.ConnectionPoolSettings;
import com.boomi.snowflake.pool.SnowflakeConnectionPool;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.snowflake.util.PrivateKeyHandler;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCSException;

/**
 * The Class SnowflakeConnection.
 *
 * @author Vanangudi,S
 */
@SuppressWarnings("rawtypes")
public class SnowflakeConnection  <C extends BrowseContext> extends BaseConnection<C> {

	/** The LOGGER. */
	private static final Logger LOG = LogUtil.getLogger(SnowflakeConnection.class);
	/** The Constant PROP_PASSWORD. */
	private static final String PROP_PASSWORD = "password";
	/** The Constant PROP_PASS_PHRASE. */
	private static final String PROP_PASS_PHRASE = "passphrase";
	/** The Constant PROP_CONNECTION_STRING. */
	private static final String PROP_CONNECTION_STRING = "connectionString";
	/** The Constant PROP_PRIVATE_KEY_STRING. */
	private static final String PROP_PRIVATE_KEY_STRING = "privateKeyString";
	/** The Constant PROP_PRIVATE_KEY. */
	private static final String PROP_PRIVATE_KEY = "privateKey";
	/** The Constant PROP_ACCESS_KEY. */
	private static final String PROP_ACCESS_KEY = "awsAccessKey";
	/** The Constant PROP_SECRET. */
	private static final String PROP_SECRET = "awsSecret";
	/** The Constant PROP_APPLICATION_BOOMI. */
	private static final String PROP_APPLICATION_BOOMI = "Boomi_Snowflake_Connector_v1.0";
	/** The Constant PROP_APPLICATION. */
	private static final String PROP_APPLICATION = "Boomi_Snowflake_Connector_v1.0";
	/** The Constant PROP_AUTHENTICATION. */
	private static final String PROP_AUTHENTICATION = "authentication";
	/** The Constant PROP_AUTHENTICATION_SNOWFLAKE_JWT. */
	private static final String PROP_AUTHENTICATION_SNOWFLAKE_JWT = "snowflake_jwt";
	/** The Constant PROP_DATE_TIME_FORMAT. */
	private static final String PROP_DATE_TIME_FORMAT = "dateTimeFormat";
	/** The Constant PROP_TIME_FORMAT. */
	private static final String PROP_TIME_FORMAT = "timeFormat";
	/** The Constant PROP_DATE_FORMAT. */
	private static final String PROP_DATE_FORMAT = "dateFormat";
	/** The Constant PROP_ENABLE_POOLING. */
	private static final String PROP_ENABLE_POOLING = "enablePooling";
	/** The Constant PROP_MAX_CON. */
	private static final String PROP_MAX_CON = "maximumConnections";
	/** The Constant PROP_MIN_CON. */
	private static final String PROP_MIN_CON = "minimumConnections";
	/** The Constant PROP_MAX_IDLE_TIME. */
	private static final String PROP_MAX_IDLE_TIME = "maximumIdleTime";
	/** The Constant PROP_MAX_WAIT_TIME. */
	private static final String PROP_MAX_WAIT_TIME = "maximumWaitTime";
	
	/** The connection object. */
	private Connection connection = null;
	/** The datasource object. */
	private DataSource datasource;

	/**
	 * The SNOWFLAKE_DRIVER full path.
	 */
	private static final String SNOWFLAKE_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver";

	/**
	 * Instantiates a snowflake connection.
	 * @param context
	 * 			the browse context
	 * */
	@SuppressWarnings("unchecked")
	public SnowflakeConnection(BrowseContext context) {
		super((C) context);
	}
	
	/**
	 * Get the Connection properties
	 * @return proper connection properties
	 */
	private Properties getProperties() {
		LOG.entering(this.getClass().getCanonicalName(), "getProperties()");

		// put all values in properties
		PropertyMap inputProperties = getContext().getConnectionProperties();
		inputProperties.putIfAbsent(PROP_PASSWORD, "");
		inputProperties.putIfAbsent(PROP_PASS_PHRASE, "");
		inputProperties.putIfAbsent(PROP_PRIVATE_KEY_STRING, "");
		if (inputProperties.getProperty(PROP_SECRET) == null) {
			inputProperties.remove(PROP_SECRET);
		}
		if (inputProperties.getLongProperty(PROP_MAX_CON) == null) {
			inputProperties.remove(PROP_MAX_CON);
		}
		if (inputProperties.getLongProperty(PROP_MIN_CON) == null) {
			inputProperties.remove(PROP_MIN_CON);
		}
		if (inputProperties.getLongProperty(PROP_MAX_IDLE_TIME) == null) {
			inputProperties.remove(PROP_MAX_IDLE_TIME);
		}
		if (inputProperties.getLongProperty(PROP_MAX_WAIT_TIME) == null) {
			inputProperties.remove(PROP_MAX_WAIT_TIME);
		}

		Properties connectionProperties = new Properties();
		connectionProperties.putAll(inputProperties);

		connectionProperties.put(PROP_APPLICATION, PROP_APPLICATION_BOOMI);
		// save and remove these properties to construct connection
		String privateKeyString = connectionProperties.remove(PROP_PRIVATE_KEY_STRING).toString();

		// if no private key string provided
		if (StringUtil.isEmpty(privateKeyString)) {
			return connectionProperties;
		}
		try {
			PrivateKey encryptedPrivateKey = PrivateKeyHandler.getPrivateObject(privateKeyString,
			connectionProperties.remove(PROP_PASS_PHRASE).toString().toCharArray());
			connectionProperties.put(PROP_AUTHENTICATION, PROP_AUTHENTICATION_SNOWFLAKE_JWT);
			connectionProperties.put(PROP_PRIVATE_KEY, encryptedPrivateKey);
			return connectionProperties;
		} catch (InvalidKeyException | IOException | OperatorCreationException | PKCSException e) {
			throw new ConnectorException("Error parsing private key: Incorrect private key string or passphrase", e);
		}
    }

	/**
	 * Gets the AWSAccessKey.
	 *
	 * @return the AWSAccessKey
	 */
	public String getAWSAccessKey() {
		return getProperties().getProperty(PROP_ACCESS_KEY);
	}

	/**
	 * Gets the AWSSecret.
	 *
	 * @return the AWSSecret
	 */
	public String getAWSSecret() {
		String secret = getProperties().getProperty(PROP_SECRET);
		return secret == null ? "" : secret; 	// Snyk complains about "hardcoded secret"  ¯\_(ツ)_/¯
	}

	/**
	 * Gets the ConnectionTimeFormat.
	 *
	 * @return the ConnectionTimeFormat
	 */
	public ConnectionTimeFormat getConnectionTimeFormat() {
		return new ConnectionTimeFormat(getProperties().getProperty(PROP_DATE_FORMAT),
				getProperties().getProperty(PROP_TIME_FORMAT),
				getProperties().getProperty(PROP_DATE_TIME_FORMAT));
	}

	/**
	 * Gets the JDBCConnection.
	 *
	 * @return the Connection
	 */	
	public Connection getJDBCConnection() {
		return connection;
	}
	
	/**
	 * create the JDBC Connection
	 * @return JDBC connection
	 */
	public Connection createJdbcConnection() {
		LOG.entering(this.getClass().getCanonicalName(), "getJDBCConnection()");
		
		if(connection != null) {
			return connection;
		}

		Properties connectionProperties = getProperties();
		// get input formats from connection page
		String inputConnectionString = connectionProperties.remove(PROP_CONNECTION_STRING).toString();

		connectionProperties.remove(PROP_DATE_TIME_FORMAT);
		connectionProperties.remove(PROP_DATE_FORMAT);
		connectionProperties.remove(PROP_TIME_FORMAT);

		connectionProperties.remove(PROP_ACCESS_KEY);
		connectionProperties.remove(PROP_SECRET);
		
		try {
			Driver driver = getSoloDriver();
			connectionProperties.putAll(getConnectionTimeFormat().getFormats());
			if(getContext().getConnectionProperties().getBooleanProperty(PROP_ENABLE_POOLING,false)) {
				ConnectionPoolSettings connectionPoolSettings = new ConnectionPoolSettings(inputConnectionString,getContext().getConnectionProperties());
				datasource = SnowflakeConnectionPool.getPooledDataSource(connectionPoolSettings,connectionProperties);
				connection = datasource.getConnection();
			}else {
				connection = driver.connect(inputConnectionString, connectionProperties);
				if (connection == null) {
					connection = DriverManager.getConnection(inputConnectionString, connectionProperties);
				}
			}
		} catch (Exception e) {
			handleException(connectionProperties, e);
		}
		return connection;
	}

	private void handleException(Properties connectionProperties, Exception e) {
		if(connection != null) {
			try {
				connection.close();
			} catch (SQLException e1) {
				throw new ConnectorException("Unable to close connection",e1);
			}
		}
		if (PROP_AUTHENTICATION_SNOWFLAKE_JWT.equals(connectionProperties.get(PROP_AUTHENTICATION)) && "JWT token is invalid.".equals(e.getMessage())) {
			throw new ConnectorException("Unable to open database connection: This is generally due to private key does not match public key.", e);
		}
		if(getContext().getConnectionProperties().getBooleanProperty(PROP_ENABLE_POOLING,false)) {
			if (e.getMessage() != null) {
				throw new ConnectorException(e.getMessage());
			} else {
				throw new ConnectorException("Unable to open database connection: ", e);
			}
		}
		throw new ConnectorException("Unable to open database connection: ", e);
	}

	/**
	 * create the Snowflake Driver with class name
	 *
	 * @return Snowflake Driver
	 */
	public static Driver getSoloDriver() {
		try {
			//DriverManager
			DriverManager.getLoginTimeout();
			return (Driver) Class.forName(SNOWFLAKE_DRIVER).newInstance();
		} catch (ClassNotFoundException e) {
			throw new ConnectorException("Failed Loading the class" + e.getMessage() , e);
		} catch (InstantiationException | IllegalAccessException e) {
			throw new ConnectorException("Failed Instantiating the class or Illegal access for class" + e.getMessage() , e);
		}
	}
}