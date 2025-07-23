// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.pool;

import org.apache.commons.dbcp2.PoolableConnection;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;

/**
 * The Class ConnectionPoolSettings.
 *
 * @author Vanangudi,S
 */
public class ConnectionPoolSettings {
	
	/** The Constant PROP_USER. */
	private static final String PROP_USER = "user";
	/** The Constant PROP_PASSWORD. */
	private static final String PROP_PASSWORD = "password";
	/** The Constant PROP_PASS_PHRASE. */
	private static final String PROP_PASS_PHRASE = "passphrase";
	/** The Constant PROP_PASS_PHRASE. */
	private static final String PROP_PASS_PRIVATE = "privateKeyString";
	/** The Constant PROP_PASS_ROLE. */
	private static final String PROP_PASS_ROLE = "role";
	/** The Constant PROP_PASS_WAREHOUSE. */
	private static final String PROP_PASS_WAREHOUSE = "warehouse";
	/** The Constant PROP_PASS_DB. */
	private static final String PROP_PASS_DB = "db";
	/** The Constant PROP_PASS_SCHEMA. */
	private static final String PROP_PASS_SCHEMA = "schema";
	/** The Constant PROP_ACCESS_KEY. */
	private static final String PROP_ACCESS_KEY = "awsAccessKey";
	/** The Constant PROP_SECRET. */
	private static final String PROP_SECRET = "awsSecret";
	/** The Constant PROP_DATE_TIME_FORMAT. */
	private static final String PROP_DATE_TIME_FORMAT = "dateTimeFormat";
	/** The Constant PROP_TIME_FORMAT. */
	private static final String PROP_TIME_FORMAT = "timeFormat";
	/** The Constant PROP_DATE_FORMAT. */
	private static final String PROP_DATE_FORMAT = "dateFormat";
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
	
	/** The Connection URL. */
	private final String _url;
	/** The Username. */
	private final String _username;
	/** The Password. */
	private final String _password;
	/** The Passphrase for key pair. */
	private final String _passphrase;
	/** The Private key for key pair. */
	private final String _privateKey;
	/** The Role. */
	private final String _role;
	/** The Warehouse. */
	private final String _warehouse;
	/** The Database. */
	private final String _database;
	/** The Schema. */
	private final String _schema;
	/** The S3 Access Key. */
	private final String _accessKey;
	/** The S3 Secret key. */
	private final String _secretKey;
	/** The Date Output Format. */
	private final String _dateOutputFormat;
	/** The Time Output Format. */
	private final String _timeOutputFormat;
	/** The Timestamp Output Format. */
	private final String _timeStampOutputFormat;
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
	
	
	/** 
	 * Instantiates a new connection pool settings
	 * @param connectionUrl connection URL for the connector
	 * @param connectionProperties properties of the connection
	 */
	public ConnectionPoolSettings(String connectionUrl, PropertyMap connectionProperties) {
		super();
		this._url = connectionUrl;
		this._username = connectionProperties.getProperty(PROP_USER);
		this._password = connectionProperties.getProperty(PROP_PASSWORD);
		this._passphrase = connectionProperties.getProperty(PROP_PASS_PHRASE);
		this._privateKey = connectionProperties.getProperty(PROP_PASS_PRIVATE);
		this._role = connectionProperties.getProperty(PROP_PASS_ROLE);
		this._warehouse = connectionProperties.getProperty(PROP_PASS_WAREHOUSE);
		this._database = connectionProperties.getProperty(PROP_PASS_DB);
		this._schema = connectionProperties.getProperty(PROP_PASS_SCHEMA);
		this._accessKey = connectionProperties.getProperty(PROP_ACCESS_KEY);
		this._secretKey = connectionProperties.getProperty(PROP_SECRET);
		this._dateOutputFormat = connectionProperties.getProperty(PROP_DATE_FORMAT);
		this._timeOutputFormat = connectionProperties.getProperty(PROP_TIME_FORMAT);
		this._timeStampOutputFormat = connectionProperties.getProperty(PROP_DATE_TIME_FORMAT);
		if(connectionProperties.getLongProperty(PROP_MAX_CON) == null) {
			this._maximumConnections = -1;
		}else {
			this._maximumConnections = Math.toIntExact(connectionProperties.getLongProperty(PROP_MAX_CON));
		}
		if(connectionProperties.getLongProperty(PROP_MIN_CON) == null) {
			this._minimumConnections = 0;
		}else {
			this._minimumConnections = Math.toIntExact(ValidatePositivity(PROP_MIN_CON, connectionProperties.getLongProperty(PROP_MIN_CON)));
		}
		if(connectionProperties.getLongProperty(PROP_MAX_IDLE_TIME) == null) {
			this._maximumIdleTime = 0L;
		}else {
			this._maximumIdleTime = ValidatePositivity(PROP_MAX_IDLE_TIME, connectionProperties.getLongProperty(PROP_MAX_IDLE_TIME));
		}
		this._whenExhaustedAction = Integer.parseInt(connectionProperties.getProperty(PROP_WHEN_EXHAUSTED));
		if(connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME) == null) {
			this._maximumWaitTime = 0L;
		}else if(connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME) < -1){
			throw new ConnectorException(PROP_MAX_WAIT_TIME +" must be greater than or equal to -1.");
		}else {
			this._maximumWaitTime = connectionProperties.getLongProperty(PROP_MAX_WAIT_TIME);
		}
		this._testOnBorrow = connectionProperties.getBooleanProperty(PROP_TEST_ON_BORROW);
		this._testOnReturn = connectionProperties.getBooleanProperty(PROP_TEST_ON_RETURN);
		this._testWhileIdle = connectionProperties.getBooleanProperty(PROP_TEST_WHILE_IDLE);
		this._validationQuery = connectionProperties.getProperty(PROP_VALIDATION_QUERY);
		
	}

	/**
	 * Gets the url.
	 *
	 * @return the url
	 */
	public String get_url() {
		return _url;
	}

	/**
	 * Gets the username.
	 *
	 * @return the username
	 */
	public String get_username() {
		return _username;
	}

	/**
	 * Gets the password.
	 *
	 * @return the password
	 */
	public String get_password() {
		return _password;
	}

	/**
	 * Gets the passphrase.
	 *
	 * @return the passphrase
	 */
	public String get_passphrase() {
		return _passphrase;
	}
	
	/**
	 * Gets the privateKey.
	 *
	 * @return the privateKey
	 */
	public String get_privateKey() {
		return _privateKey;
	}

	/**
	 * Gets the role.
	 *
	 * @return the role
	 */
	public String get_role() {
		return _role;
	}

	/**
	 * Gets the warehouse.
	 *
	 * @return the warehouse
	 */
	public String get_warehouse() {
		return _warehouse;
	}

	/**
	 * Gets the database.
	 *
	 * @return the database
	 */
	public String get_database() {
		return _database;
	}

	/**
	 * Gets the schema.
	 *
	 * @return the schema
	 */
	public String get_schema() {
		return _schema;
	}

	/**
	 * Gets the accessKey.
	 *
	 * @return the accessKey
	 */
	public String get_accessKey() {
		return _accessKey;
	}
	
	/**
	 * Gets the secretKey.
	 *
	 * @return the secretKey
	 */
	public String get_secretKey() {
		return _secretKey;
	}
	
	/**
	 * Gets the dateOutputForma.
	 *
	 * @return the dateOutputFormat
	 */
	public String get_dateOutputFormat() {
		return _dateOutputFormat;
	}

	/**
	 * Gets the timeOutputForma.
	 *
	 * @return the timeOutputFormat
	 */
	public String get_timeOutputFormat() {
		return _timeOutputFormat;
	}

	/**
	 * Gets the timeStampOutputFormat.
	 *
	 * @return the timeStampOutputFormat
	 */
	public String get_timeStampOutputFormat() {
		return _timeStampOutputFormat;
	}

	/**
	 * Gets the maximumConnections.
	 *
	 * @return the maximumConnections
	 */
	public int get_maximumConnections() {
		return _maximumConnections;
	}

	/**
	 * Gets the minimumConnections.
	 *
	 * @return the minimumConnections
	 */
	public int get_minimumConnections() {
		return _minimumConnections;
	}

	/**
	 * Gets the maximumIdleTime.
	 *
	 * @return the maximumIdleTime
	 */
	public Long get_maximumIdleTime() {
		return _maximumIdleTime;
	}

	/**
	 * Gets the whenExhaustedAction.
	 *
	 * @return the whenExhaustedAction
	 */
	public int get_whenExhaustedAction() {
		return _whenExhaustedAction;
	}

	/**
	 * Gets the maximumWaitTime.
	 *
	 * @return the maximumWaitTime
	 */
	public Long get_maximumWaitTime() {
		return _maximumWaitTime;
	}

	/**
	 * Gets the testOnBorrow.
	 *
	 * @return the testOnBorrow
	 */
	public boolean is_testOnBorrow() {
		return _testOnBorrow;
	}

	/**
	 * Gets the testOnReturn.
	 *
	 * @return the testOnReturn
	 */
	public boolean is_testOnReturn() {
		return _testOnReturn;
	}

	/**
	 * Gets the testWhileIdle.
	 *
	 * @return the testWhileIdle
	 */
	public boolean is_testWhileIdle() {
		return _testWhileIdle;
	}

	/**
	 * Gets the validationQuery.
	 *
	 * @return the validationQuery
	 */
	public String get_validationQuery() {
		return _validationQuery;
	}
	
	

	/**
	 * Generate key for the Connection pool
	 *
	 * @return the connectionParameters key value
	 */
	public String generateKey() {
		return this.get_url()+this.get_username()+this.get_password()+
				this.get_passphrase()+ this.get_privateKey()+
				this.get_role()+this.get_warehouse()+this.get_database()+
				this.get_schema()+this.get_accessKey()+this.get_secretKey()+
				this.get_dateOutputFormat()+this.get_timeOutputFormat()+
				this.get_timeStampOutputFormat();
	}
	
	/** 
	 * Update the pool settings if it has been updated after creating the pool.
	 *
	 * @param connectionProperties 
	 * 				properties of the connection
	 */
    protected void updatePoolSettings(ConnectionPoolSettings connectionPoolSettings) {
        if (!this.equals(connectionPoolSettings)) {
            throw new IllegalArgumentException("Unable to update settings from a different connection.");
        }
        this._maximumConnections = connectionPoolSettings.get_maximumConnections();
        this._minimumConnections = connectionPoolSettings.get_minimumConnections();
        this._maximumIdleTime = connectionPoolSettings.get_maximumIdleTime();
        this._whenExhaustedAction = connectionPoolSettings.get_whenExhaustedAction();
        this._maximumWaitTime = connectionPoolSettings.get_maximumWaitTime();
        this._testOnBorrow = connectionPoolSettings.is_testOnBorrow();
        this._testOnReturn = connectionPoolSettings.is_testOnReturn();
        this._testWhileIdle = connectionPoolSettings.is_testWhileIdle();
        this._validationQuery = connectionPoolSettings.get_validationQuery();
    }
    
    /** 
	 * check if pool settings has been changed
	 *
	 * @param connectionPool 
	 * 			Connection Pool parameters
	 * @return boolean
	 */
    public boolean isChanged(VersionedConnectionPool<PoolableConnection> connectionPool) {
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
    
    /** 
	 * check if the value is positive
	 *
	 * @param PropertyName 
	 * 			Property name whose value is being checked
	 * @param param 
	 * 			Property value
	 * @return param 
	 * 			Property value
	 */
	private static Long ValidatePositivity(String propertyName,Long param) {
		if (param < 0) {
			throw new ConnectorException(propertyName + " must be a positive integer");
		}
		
		return param;
	}
}
