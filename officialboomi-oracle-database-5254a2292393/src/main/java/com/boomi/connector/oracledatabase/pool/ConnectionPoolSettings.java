// Copyright (c) 2022 Boomi, LP.
package com.boomi.connector.oracledatabase.pool;

import java.util.Map;

import org.apache.commons.dbcp2.PoolableConnection;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
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
	private int maximumConnections;
	
	/** The Minimum connections for connection pool. */
	private int minimumConnections;
	/** The Maximum Idle Time for the connection to be qualified for eviction. */
	private Long maximumIdleTime;
	/** The Action when Pool is exhausted. */
	private int whenExhaustedAction;
	/** The Maximum wait Time for connection if pool is exhausted. */
	private Long _maximumWaitTime;
	/** The Test On Borrow. */
	private boolean testOnBorrow;
	/** The Test On Return. */
	private boolean testOnReturn;
	/** The Test On Idle. */
	private boolean testWhileIdle;
	/** The Validation Query. */
	private String validationQuery;
	
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
		this.className = connectionProperties.getProperty(CLASSNAME,"");
		this.username = connectionProperties.getProperty(USERNAME,"");
		this.password = connectionProperties.getProperty(PASS,"");
		this.url = connectionUrl;
		this.schemaName = connectionProperties.getProperty(SCHEMA_NAME,"");
		this.customProperty = connectionProperties.getCustomProperties("CustomProperties");
		this.connectTimeout = connectionProperties.getLongProperty("connectTimeOut");
		this.readTimeOut = connectionProperties.getLongProperty("readTimeOut");
		if(connectionProperties.getLongProperty(PROP_MAX_CON) == null) {
			this.maximumConnections = -1;
		}else
			this.maximumConnections = Math.toIntExact(connectionProperties.getLongProperty(PROP_MAX_CON));
		if(connectionProperties.getLongProperty(PROP_MIN_CON) == null) {
			this.minimumConnections = 0;
		}else
			this.minimumConnections = Math.toIntExact(validatePositivity(PROP_MIN_CON,connectionProperties.getLongProperty(PROP_MIN_CON)));
		if(connectionProperties.getLongProperty(PROP_MAX_IDLE_TIME) == null) {
			this.maximumIdleTime = 0L;
		}else
			this.maximumIdleTime = validatePositivity(PROP_MAX_IDLE_TIME,connectionProperties.getLongProperty(PROP_MAX_IDLE_TIME));
		this.whenExhaustedAction = Integer.parseInt(connectionProperties.getProperty(PROP_WHEN_EXHAUSTED));
		if(connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME) == null) {
			this._maximumWaitTime = 0L;
		}else if(connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME) < -1){
			throw new ConnectorException(PROP_MAX_WAIT_TIME +" must be greater than or equal to -1.");
		}else
			this._maximumWaitTime = connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME);
		this.testOnBorrow = connectionProperties.getBooleanProperty(PROP_TEST_ON_BORROW);
		this.testOnReturn = connectionProperties.getBooleanProperty(PROP_TEST_ON_RETURN);
		this.testWhileIdle = connectionProperties.getBooleanProperty(PROP_TEST_WHILE_IDLE);
		this.validationQuery = connectionProperties.getProperty(PROP_VALIDATION_QUERY);
		
	}
	
	/**
	 * 
	 * @param propertyName
	 * @param param
	 * @return
	 */
	private static Long validatePositivity(String propertyName,Long param) {
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
	 * Gets the maximumConnections.
	 *
	 * @return the maximumConnections
	 */
	public int getMaximumConnections() {
		return maximumConnections;
	}
	/**
	 * Sets the maximumConnections.
	 *
	 * @param maximumConnections
	 */
	public void setMaximumConnections(int maximumConnections) {
		this.maximumConnections = maximumConnections;
	}
	/**
	 * Gets the minimumConnections.
	 *
	 * @return the minimumConnections
	 */
	public int getMinimumConnections() {
		return minimumConnections;
	}
	/**
	 * Sets the _minimumConnections.
	 *
	 * @param minimumConnections 
	 */
	public void setMinimumConnections(int minimumConnections) {
		this.minimumConnections = minimumConnections;
	}
	/**
	 * Gets the maximumIdleTime.
	 *
	 * @return the maximumIdleTime
	 */
	public Long getMaximumIdleTime() {
		return maximumIdleTime;
	}
	/**
	 * Sets the maximumIdleTime.
	 *
	 * @param maximumIdleTime 
	 */
	public void setMaximumIdleTime(Long maximumIdleTime) {
		this.maximumIdleTime = maximumIdleTime;
	}
	/**
	 * Gets the whenExhaustedAction.
	 *
	 * @return the whenExhaustedAction
	 */
	public int getWhenExhaustedAction() {
		return whenExhaustedAction;
	}
	/**
	 * Sets the whenExhaustedAction.
	 *
	 * @param whenExhaustedAction 
	 */
	public void setWhenExhaustedAction(int whenExhaustedAction) {
		this.whenExhaustedAction = whenExhaustedAction;
	}
	/**
	 * Gets the maximumWaitTime.
	 *
	 * @return the maximumWaitTime
	 */
	public Long getMaximumWaitTime() {
		return _maximumWaitTime;
	}
	/**
	 * Sets the maximumWaitTime.
	 *
	 * @param maximumWaitTime 
	 */
	public void setMaximumWaitTime(Long maximumWaitTime) {
		this._maximumWaitTime = maximumWaitTime;
	}
	/**
	 * gets the testOnBorrow.
	 *
	 * @return the testOnBorrow
	 */
	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}
	/**
	 * Sets the maximumWaitTime.
	 *
	 * @param maximumWaitTime
	 */
	public void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}
	/**
	 * Gets the testOnReturn.
	 *
	 * @return the testOnReturn
	 */
	public boolean isTestOnReturn() {
		return testOnReturn;
	}
	/**
	 * Sets the testOnReturn.
	 *
	 * @param testOnReturn
	 */
	public void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}
	/**
	 * Gets the testWhileIdle.
	 *
	 * @return the testWhileIdle
	 */
	public boolean isTestWhileIdle() {
		return testWhileIdle;
	}
	/**
	 * Sets the testWhileIdle.
	 *
	 * @param testWhileIdle
	 */
	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}
	/**
	 * Gets the validationQuery.
	 *
	 * @return the validationQuery
	 */
	public String getValidationQuery() {
		return validationQuery;
	}
	/**
	 * Sets the validationQuery.
	 *
	 * @param validationQuery
	 */
	public void setValidationQuery(String validationQuery) {
		this.validationQuery = validationQuery;
	}
	 
	/**
	 * 
	 * @param connectionPool
	 * @return
	 */
	public boolean isChanged(VersionedConnectionPool<PoolableConnection> connectionPool) {
	    	int maxIdle = Math.min(this.getMaximumConnections(), 50);
	    	maxIdle = maxIdle <= 0 ? 50 : maxIdle;
	    	if(this.getMaximumConnections() != connectionPool.getMaxTotal()
	    		 || this.getMinimumConnections() != connectionPool.getMinIdle()
	    		 || maxIdle != connectionPool.getMaxIdle()
	    		 || (this.getMaximumWaitTime() * 1000L) != connectionPool.getMaxWaitMillis()
	    		 || this.isTestOnBorrow() != connectionPool.getTestOnBorrow()
	    		 || this.isTestOnReturn() != connectionPool.getTestOnReturn()
	    		 || this.isTestWhileIdle() != connectionPool.getTestWhileIdle()
	    		 || (this.getMaximumIdleTime() * 1000L) != connectionPool.getMinEvictableIdleTimeMillis()
	    		 || ((!this.getValidationQuery().isEmpty() || !connectionPool.getValidationQuery().isEmpty())
	    		 && !(this.getValidationQuery().equals(connectionPool.getValidationQuery())))) {
	    		return true;
	    	}
			return false;
	    	
	    }
}
