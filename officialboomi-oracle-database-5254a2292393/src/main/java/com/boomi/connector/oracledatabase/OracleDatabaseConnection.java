// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.oracledatabase.pool.ConnectionPoolSettings;
import com.boomi.connector.oracledatabase.pool.OracleDatabaseConnectorConnectionPool;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.util.BaseConnection;


/**
 * The Class DatabaseConnectorConnection.
 *
 * @author swastik.vn
 */
public class OracleDatabaseConnection extends BaseConnection {

	/** The class name. */
	private String className;

	/** The username. */
	private String username;

	/** The password. */
	private String password;

	/** The url. */
	private String url;
	
	/** The schema name. */
	private String schemaName;

	/** The custom property. */
	private Map<String, String> customProperty;
	
	/** The readTimeOut. */
	private Long readTimeOut;
	
	/** The connect timeout. */
	private Long connectTimeout;
	
	/** The connection object. */
	private Connection connection = null;
	


	/**
	 * Instantiates a new database connector connection.
	 *
	 * @param context the context
	 */
	public OracleDatabaseConnection(BrowseContext context) {
		super(context);
		this.className = getContext().getConnectionProperties().getProperty(CLASSNAME,"");
		this.username = getContext().getConnectionProperties().getProperty(USERNAME,"");
		this.password = getContext().getConnectionProperties().getProperty(PASS,"");
		this.url = getContext().getConnectionProperties().getProperty(URL,"");
		this.schemaName = getContext().getConnectionProperties().getProperty(OracleDatabaseConstants.SCHEMA_NAME,"");
		this.customProperty = context.getConnectionProperties().getCustomProperties("connectionProperties");
		this.readTimeOut = getContext().getConnectionProperties().getLongProperty("readTimeOut");
		this.connectTimeout = getContext().getConnectionProperties().getLongProperty("connectTimeOut");
	}

	/**
	 * Gets the class name.
	 *
	 * @return the class name
	 */
	public String getClassName() {
		return className;
	}

	public Long getReadTimeOut() {
		return readTimeOut;
	}

	/**
	 * Gets the username.
	 *
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Gets the password.
	 *
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Gets the url.
	 *
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	/**
	 * Gets the schema name.
	 *
	 * @return the schema name
	 */
	public String getSchemaName() {
		return schemaName;
	}

	/**
	 * Load properties.
	 *
	 * @return the properties
	 */
	public Properties loadProperties() {
		Properties prop = new Properties();
		if (!getUsername().isEmpty()) {
			prop.put("user", getUsername());
		}
		if (!getPassword().isEmpty()) {
			prop.put("password", getPassword());
		}
		if (customProperty != null && !customProperty.isEmpty()) {
			for (Map.Entry<String, String> entry : customProperty.entrySet()) {
				prop.put(entry.getKey(), entry.getValue());
			}
		}
		return prop;

	}

	/**
	 * Gets the solo connection.
	 *
	 * @return the solo connection
	 */
	public Driver getSoloConnection() {

		try {
			// In order to allow the ability to invoke multiple database drivers at the same
			// time, the DriverManager
			// class needs to be initialized before Class.forName is called. Calling this
			// particular method will invoke
			// the DriverManager so it is class loaded before trying to instantiate drivers.
			setLoginTimeout();
			return (Driver) Class.forName(className).newInstance();
		} catch (ClassNotFoundException e) {
			throw new ConnectorException("Failed Loading the class");
		} catch (InstantiationException | IllegalAccessException e) {
			throw new ConnectorException(e.getMessage());
		}
	}
	
	
	/**
	 * Sets the login timeout.
	 */
	private void setLoginTimeout() {
		if(null != connectTimeout && connectTimeout != 0) {
			DriverManager.setLoginTimeout(QueryBuilderUtil.convertMsToSeconds(connectTimeout.intValue()));
		}else {
			DriverManager.getLoginTimeout();
		}
		
	}
	
	/**
	 * Gets the JDBCConnection.
	 *
	 * @return the Connection
	 */	

	public Connection getOracleConnection() {
		
		/** The datasource object. */
		DataSource datasource;
		if (connection != null) {
			return connection;	
		}
		try {
			//Call to ensure the driver is initialized when a solo connection is needed or when the connection pool is enabled.
			Driver driverInstance = getSoloConnection();
			if (getContext().getConnectionProperties().getBooleanProperty("enablePooling", false)) {
				ConnectionPoolSettings connectionPoolSettings = new ConnectionPoolSettings(this.url,
						getContext().getConnectionProperties());
				datasource = OracleDatabaseConnectorConnectionPool.getPooledDataSource(connectionPoolSettings,
						loadProperties());
				connection = datasource.getConnection();
			} else {
				connection = driverInstance.connect(this.url, loadProperties());
			}
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e1) {
					throw new ConnectorException("Unable to close connection", e1);
				}
			}

				if (e.getMessage() != null) {
					throw new ConnectorException(e.getMessage());
				} else
					throw new ConnectorException("Unable to open database connection: ", e);
			
		}
		return connection;

	}

	/**
	 * Method to test the Database Connection by taking the Standard JDBC
	 * Parameters.
	 *
	 * @throws ConnectorException the connector exception
	 */
	public void test() {
		try (Connection conn = getOracleConnection()) {
			if(conn == null)
				throw new ConnectorException("Connection url is invalid for test connection!!");
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

	}

}