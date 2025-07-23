// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.connection;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.pool.ConnectionPoolSettings;
import com.boomi.connector.databaseconnector.pool.DatabaseConnectorConnectionPool;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.StringUtil;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;


/**
 * The Class DatabaseConnectorConnection.
 *
 * @author swastik.vn
 */
public class DatabaseConnectorConnection<C extends BrowseContext> extends BaseConnection<C> {

	/** The class name. */
	private String className;

	/** The username. */
	private String username;

	/** The password. */
	private String password;

	/** The url. */
	private String url;
	
	/** The read time out. */
	private Long readTimeOut;
	
	/** The connect timeout. */
	private Long connectTimeout;
	
	/** The schema name. */
	private String schemaName;
	

	/** The custom property. */
	private Map<String, String> customProperty;
	
	/** The Constant POSTGRESS. */
	private static final String POSTGRESS = "postgres";
	
	/** The connection object. */
	private Connection connection = null;
	/** The datasource object. */
	private DataSource datasource;

	/**
	 * Instantiates a new database connector connection.
	 *
	 * @param context the context
	 */
	public DatabaseConnectorConnection(C context) {
		super(context);
		PropertyMap properties = getContext().getConnectionProperties();
		this.className = properties.getProperty(DatabaseConnectorConstants.CLASSNAME,"");
		this.username = properties.getProperty(DatabaseConnectorConstants.USERNAME,"");
		this.password = properties.getProperty(DatabaseConnectorConstants.PASS,"");
		this.url = properties.getProperty(DatabaseConnectorConstants.URL,"");
		this.schemaName = properties.getProperty(DatabaseConnectorConstants.SCHEMA_NAME,"");
		this.customProperty = properties.getCustomProperties(DatabaseConnectorConstants.CUSTOM_PROPERTIES);
		this.connectTimeout = properties.getLongProperty(DatabaseConnectorConstants.CONNECT_TIME_OUT);
		this.readTimeOut = properties.getLongProperty(DatabaseConnectorConstants.READ_TIME_OUT);
	}

	/**
	 * Gets the class name.
	 *
	 * @return the class name
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * Sets the class name.
	 *
	 * @param className the new class name
	 */
	public void setClassName(String className) {
		this.className = className;
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
	 * Sets the username.
	 *
	 * @param username the new username
	 */
	public void setUsername(String username) {
		this.username = username;
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
	 * Sets the password.
	 *
	 * @param password the new password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Gets the url.
	 *
	 * @return the url
	 */
	public String getUrl() {
		if(StringUtil.isEmpty(url) || StringUtil.isBlank(url)) {
			throw new ConnectorException("The Connection URL field cannot be empty!!");
		}
		return url;
	}

	/**
	 * Sets the url.
	 *
	 * @param url the new url
	 *
	 */
	public void setUrl(String url) {
		this.url = url;
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
		prop.put("user", getUsername());
		prop.put("password", getPassword());
		if(className.contains(POSTGRESS)) {
			prop.put("loginTimeout", connectTimeout != null ? connectTimeout.intValue() : 0);
		}
		if (!customProperty.isEmpty()) {
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
		if(null != connectTimeout && connectTimeout != 0 && !className.contains(POSTGRESS)) {
			DriverManager.setLoginTimeout(connectTimeout.intValue());
		}else if(!className.contains(POSTGRESS)) {
			DriverManager.setLoginTimeout(0);
		}else {
			DriverManager.getLoginTimeout();
		}
		
	}

	/**
	 * Method to test the Database Connection by taking the Standard JDBC
	 * Parameters.
	 *
	 * @throws ConnectorException the connector exception
	 */
	public void test() {
		try (Connection sqlConnection = getDatabaseConnection()) {
			if(sqlConnection == null)
				throw new ConnectorException("Connection url is invalid for test connection");
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

	}
	/**
	 * Gets the custom property.
	 *
	 * @return the custom property
	 */
	public Map<String, String> getCustomProperty() {
		return customProperty;
	}

	/**
	 * Sets the custom property.
	 *
	 * @param customProperty the custom property
	 */
	public void setCustomProperty(Map<String, String> customProperty) {
		this.customProperty = customProperty;
	}
	
	/**
	 * Gets the read time out.
	 *
	 * @return the read time out
	 */
	public Long getReadTimeOut() {
		return readTimeOut;
	}
	/**
	 * Gets the JDBCConnection.
	 *
	 * @return the Connection
	 */	

	public Connection getDatabaseConnection() {
		if (connection != null) {
			return connection;
		}
		try {
			//Call to ensure the driver is initialized when a solo connection is needed or when the connection pool is enabled.
			Driver driverInstance = getSoloConnection();
			if (getContext().getConnectionProperties().getBooleanProperty("enablePooling", false)) {
				ConnectionPoolSettings connectionPoolSettings = new ConnectionPoolSettings(this.url,
						getContext().getConnectionProperties());
				datasource = DatabaseConnectorConnectionPool.getPooledDataSource(connectionPoolSettings,
						loadProperties());
				connection = datasource.getConnection();
			} else {
				setLoginTimeout();
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
}