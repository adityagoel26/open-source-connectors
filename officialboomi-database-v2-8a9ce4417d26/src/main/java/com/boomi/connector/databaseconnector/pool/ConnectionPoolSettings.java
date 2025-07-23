// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.pool;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import org.apache.commons.dbcp2.PoolableConnection;

import java.util.Map;
/**
 * The Class ConnectionPoolSettings.
 */
public class ConnectionPoolSettings {
	
	
	
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
	

	/** The custom property. */
	private Map<String, String> customProperty;
	
	/** The schema name. */
	private String schemaName;
	/** The Maximum connections for connection pool. */
	private int _maximumConnections;
	
	/** The Minimum connections for connection pool. */
	private int _minimumConnections;
	/** The Maximum Idle Time for the connection to be qualified for eviction. */
	private Long _maximumIdleTime;
	/** The Action when Pool is exhausted. */
	private int _whenExhaustedAction;
	/** The Maximum wait Time for connection if pool is exhausted. */
	private Long _maximumWaitTime;
	/** The Test On Borrow. */
	private boolean _testOnBorrow;
	/** The Test On Return. */
	private boolean _testOnReturn;
	/** The Test On Idle. */
	private boolean _testWhileIdle;
	/** The Validation Query. */
	private String _validationQuery;
	
	/** The Constant PROP_MAX_CON. */
	private static final String PROP_MAX_CON = "maximumConnections";
	/** The Constant PROP_MIN_CON. */
	private static final String PROP_MIN_CON = "minimumConnections";
	/** The Constant PROP_MAX_IDLE_TIME. */
	private static final String PROP_MAX_IDLE_TIME = "maximumIdleTime";
	/** The Constant PROP_MAX_WAIT_TIME. */
	private static final String PROP_MAX_WAIT_TIME = "maximumWaitTime";
	/** The Constant PROP_WHEN_EXHAUSTED. */
	private static final String PROP_WHEN_EXHAUSTED = "whenExhaustedAction";
	/** The Constant PROP_TEST_ON_BORROW. */
	private static final String PROP_TEST_ON_BORROW = "testOnBorrow";
	/** The Constant PROP_TEST_ON_RETURN. */
	private static final String PROP_TEST_ON_RETURN = "testOnReturn";
	/** The Constant PROP_TEST_WHILE_IDLE. */
	private static final String PROP_TEST_WHILE_IDLE = "testWhileIdle";
	/** The Constant PROP_VALIDATION_QUERY. */	
	private static final String PROP_VALIDATION_QUERY = "validationQuery";
	/** 
	 * Instantiates a new connection pool settings
	 * @param connectionUrl connection URL for the connector
	 * @param connectionProperties properties of the connection
	 */
	public ConnectionPoolSettings(String connectionUrl, PropertyMap connectionProperties) {
		super();
		this.className = connectionProperties.getProperty(DatabaseConnectorConstants.CLASSNAME,"");
		this.username = connectionProperties.getProperty(DatabaseConnectorConstants.USERNAME,"");
		this.password = connectionProperties.getProperty(DatabaseConnectorConstants.PASS,"");
		this.url = connectionUrl;
		this.schemaName = connectionProperties.getProperty(DatabaseConnectorConstants.SCHEMA_NAME,"");
		this.customProperty = connectionProperties.getCustomProperties("CustomProperties");
		this.connectTimeout = connectionProperties.getLongProperty("connectTimeOut");
		this.readTimeOut = connectionProperties.getLongProperty("readTimeOut");
		if(connectionProperties.getLongProperty(PROP_MAX_CON) == null) {
			this._maximumConnections = -1;
		}else
			this._maximumConnections = Math.toIntExact(connectionProperties.getLongProperty(PROP_MAX_CON));
		if(connectionProperties.getLongProperty(PROP_MIN_CON) == null) {
			this._minimumConnections = 0;
		}else
			this._minimumConnections = Math.toIntExact(ValidatePositivity(PROP_MIN_CON,connectionProperties.getLongProperty(PROP_MIN_CON)));
		if(connectionProperties.getLongProperty(PROP_MAX_IDLE_TIME) == null) {
			this._maximumIdleTime = 0L;
		}else
			this._maximumIdleTime = ValidatePositivity(PROP_MAX_IDLE_TIME,connectionProperties.getLongProperty(PROP_MAX_IDLE_TIME));
		this._whenExhaustedAction = Integer.parseInt(connectionProperties.getProperty(PROP_WHEN_EXHAUSTED));
		if(connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME) == null) {
			this._maximumWaitTime = 0L;
		}else if(connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME) < -1){
			throw new ConnectorException(PROP_MAX_WAIT_TIME +" must be greater than or equal to -1.");
		}else
			this._maximumWaitTime = connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME);
		this._testOnBorrow = connectionProperties.getBooleanProperty(PROP_TEST_ON_BORROW);
		this._testOnReturn = connectionProperties.getBooleanProperty(PROP_TEST_ON_RETURN);
		this._testWhileIdle = connectionProperties.getBooleanProperty(PROP_TEST_WHILE_IDLE);
		this._validationQuery = connectionProperties.getProperty(PROP_VALIDATION_QUERY);
		
	}
	private static Long ValidatePositivity(String propertyName,Long param) {
		if (param < 0)
			throw new ConnectorException(propertyName +" must be a positive integer");
		
		return param;
	}
	/**
	 * Generate key for the Connection pool
	 *
	 * @return the connectionParameters key value
	 */
	public String generateKey() {
		return this.getUrl()+this.getUsername()+this.getPassword()+
				this.getSchemaName()+ this.getConnectTimeout()+
				this.getReadTimeOut()+this.getCustomProperty();
	}
	/**
	 * Gets the className.
	 *
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}
	/**
	 * Sets the className.
	 *
	 * @param className
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
	 * @param username 
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
	 * @param password
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
		return url;
	}
	/**
	 * Sets the url.
	 *
	 * @param url 
	 */
	public void setUrl(String url) {
		this.url = url;
	}
	/**
	 * Gets the readTimeOut.
	 *
	 * @return the readTimeOut
	 */
	public Long getReadTimeOut() {
		return readTimeOut;
	}
	/**
	 * Sets the readTimeOut.
	 *
	 * @param readTimeOut 
	 */
	public void setReadTimeOut(Long readTimeOut) {
		this.readTimeOut = readTimeOut;
	}
	/**
	 * Gets the connectTimeout.
	 *
	 * @return the connectTimeout
	 */
	public Long getConnectTimeout() {
		return connectTimeout;
	}
	/**
	 * Sets the connectTimeout.
	 *
	 * @param connectTimeout 
	 */
	public void setConnectTimeout(Long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}
	/**
	 * Gets the customProperty.
	 *
	 * @return the customProperty
	 */
	public Map<String, String> getCustomProperty() {
		return customProperty;
	}
	/**
	 * Sets the customProperty.
	 *
	 * @param customProperty 
	 */
	public void setCustomProperty(Map<String, String> customProperty) {
		this.customProperty = customProperty;
	}
	/**
	 * Gets the schemaName.
	 *
	 * @return the schemaName
	 */
	public String getSchemaName() {
		return schemaName;
	}
	/**
	 * Sets the schemaName.
	 *
	 * @param schemaName 
	 */
	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
	/**
	 * Gets the _maximumConnections.
	 *
	 * @return the _maximumConnections
	 */
	public int get_maximumConnections() {
		return _maximumConnections;
	}
	/**
	 * Sets the _maximumConnections.
	 *
	 * @param _maximumConnections
	 */
	public void set_maximumConnections(int _maximumConnections) {
		this._maximumConnections = _maximumConnections;
	}
	/**
	 * Gets the _minimumConnections.
	 *
	 * @return the _minimumConnections
	 */
	public int get_minimumConnections() {
		return _minimumConnections;
	}
	/**
	 * Sets the _minimumConnections.
	 *
	 * @param _minimumConnections 
	 */
	public void set_minimumConnections(int _minimumConnections) {
		this._minimumConnections = _minimumConnections;
	}
	/**
	 * Gets the _maximumIdleTime.
	 *
	 * @return the _maximumIdleTime
	 */
	public Long get_maximumIdleTime() {
		return _maximumIdleTime;
	}
	/**
	 * Sets the _maximumIdleTime.
	 *
	 * @param _maximumIdleTime 
	 */
	public void set_maximumIdleTime(Long _maximumIdleTime) {
		this._maximumIdleTime = _maximumIdleTime;
	}
	/**
	 * Gets the _whenExhaustedAction.
	 *
	 * @return the _whenExhaustedAction
	 */
	public int get_whenExhaustedAction() {
		return _whenExhaustedAction;
	}
	/**
	 * Sets the _whenExhaustedAction.
	 *
	 * @param _whenExhaustedAction 
	 */
	public void set_whenExhaustedAction(int _whenExhaustedAction) {
		this._whenExhaustedAction = _whenExhaustedAction;
	}
	/**
	 * Gets the _maximumWaitTime.
	 *
	 * @return the _maximumWaitTime
	 */
	public Long get_maximumWaitTime() {
		return _maximumWaitTime;
	}
	/**
	 * Sets the _maximumWaitTime.
	 *
	 * @param _maximumWaitTime 
	 */
	public void set_maximumWaitTime(Long _maximumWaitTime) {
		this._maximumWaitTime = _maximumWaitTime;
	}
	/**
	 * gets the _testOnBorrow.
	 *
	 * @return the _testOnBorrow
	 */
	public boolean is_testOnBorrow() {
		return _testOnBorrow;
	}
	/**
	 * Sets the _maximumWaitTime.
	 *
	 * @param _maximumWaitTime
	 */
	public void set_testOnBorrow(boolean _testOnBorrow) {
		this._testOnBorrow = _testOnBorrow;
	}
	/**
	 * Gets the _testOnReturn.
	 *
	 * @return the _testOnReturn
	 */
	public boolean is_testOnReturn() {
		return _testOnReturn;
	}
	/**
	 * Sets the _testOnReturn.
	 *
	 * @param _testOnReturn
	 */
	public void set_testOnReturn(boolean _testOnReturn) {
		this._testOnReturn = _testOnReturn;
	}
	/**
	 * Gets the _testWhileIdle.
	 *
	 * @return the _testWhileIdle
	 */
	public boolean is_testWhileIdle() {
		return _testWhileIdle;
	}
	/**
	 * Sets the _testWhileIdle.
	 *
	 * @param _testWhileIdle
	 */
	public void set_testWhileIdle(boolean _testWhileIdle) {
		this._testWhileIdle = _testWhileIdle;
	}
	/**
	 * Gets the _validationQuery.
	 *
	 * @return the _validationQuery
	 */
	public String get_validationQuery() {
		return _validationQuery;
	}
	/**
	 * Sets the _validationQuery.
	 *
	 * @param _validationQuery
	 */
	public void set_validationQuery(String _validationQuery) {
		this._validationQuery = _validationQuery;
	}
	  /** 
		 * check if pool settings has been changed
		 *
		 * @param connectionPool 
		 * 			Connection Pool parameters
		 * @return boolean
		 */
	@Override
    public boolean equals(Object o) {
		VersionedConnectionPool<PoolableConnection> connectionPool = (VersionedConnectionPool<PoolableConnection>) o;
	    	int maxIdle = Math.min(this.get_maximumConnections(), 50);
	    	maxIdle = maxIdle <= 0 ? 50 : maxIdle;
	    	if(this.get_maximumConnections() != connectionPool.getMaxTotal()
	    		 || this.get_minimumConnections() != connectionPool.getMinIdle()
	    		 || maxIdle != connectionPool.getMaxIdle()
	    		 || (this.get_maximumWaitTime() * 1000L) != connectionPool.getMaxWaitMillis()
	    		 || this.is_testOnBorrow() != connectionPool.getTestOnBorrow()
	    		 || this.is_testOnReturn() != connectionPool.getTestOnReturn()
	    		 || this.is_testWhileIdle() != connectionPool.getTestWhileIdle()
	    		 || (this.get_maximumIdleTime() * 1000L) != connectionPool.getMinEvictableIdleTimeMillis()
	    		 || ((!this.get_validationQuery().isEmpty() || !connectionPool.get_validationQuery().isEmpty())
	    		 && !(this.get_validationQuery().equals(connectionPool.get_validationQuery())))) {
	    		return true;
	    	}
			return false;
	    	
	    }
}
