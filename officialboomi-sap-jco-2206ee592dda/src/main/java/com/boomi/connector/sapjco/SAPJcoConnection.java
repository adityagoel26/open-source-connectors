// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sapjco.listener.SAPServerDataProvider;
import com.boomi.connector.sapjco.util.SAPJcoConstants;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.ClassUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.ObjectUtil;
import com.boomi.util.PropertyUtil;
import com.boomi.util.StringUtil;
import com.sap.conn.jco.JCo;
import com.sap.conn.jco.JCoDestination;
import com.sap.conn.jco.JCoDestinationManager;
import com.sap.conn.jco.JCoException;
import com.sap.conn.jco.JCoFunction;
import com.sap.conn.jco.JCoFunctionTemplate;
import com.sap.conn.jco.JCoRepository;
import com.sap.conn.jco.ext.DestinationDataProvider;
import com.sap.conn.jco.ext.Environment;
import com.sap.conn.jco.ext.ServerDataProvider;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoConnection extends BaseConnection implements Closeable {

	// sap jco properties
	private static final Logger logger = Logger.getLogger(SAPJcoConnection.class.getName());
	private static SAPDestinationDataProvider globalDestinationProvider;
	private static final String BAPI_TRANSACTION_COMMIT = "BAPI_TRANSACTION_COMMIT";
	private final String destinationName = UUID.randomUUID().toString();
	private JCoDestination jcoDestination;
	private SAPDestinationDataProvider destinationProvider;
	// sap jco properties
	// Starting with version 3.0.20, the MS_HOST setting when using AS_HOST caused
	// connection problems.
	private static final SoftwareVersion REFERENCE_VERSION = new SoftwareVersion(3, 0, 20);
	private static SAPServerDataProvider sapServerDataProvider;

	private String connectionType;
	private String server;
	private String userName;
	private String password;
	private String client;
	private String languageCode;
	private String systemNumber;
	private boolean enableTrace;
	private Integer maximumIdleConnections;
	private Integer maximumActiveConnections;
	private Long idleTime;
	private String listenerGatewayHost;
	private String listenerGatewayService;
	private String databaseUserName;
	private String databasePassword;
	private String databaseDriverType;
	private String customDriverClassName;
	private String customConnectionURL;
	private String databaseHost;
	private String databasePort;
	private String databaseName;
	private String additionalOptions;
	private Integer minimumConnections;
	private Integer maximumConnections;
	private String systemName;
	private String groupName;

	private String dbDriverClassName;
	private String dbUrl;
	private Integer listenerConnectionCount;
	private String additionalPropsFileName;
	private String tidManagementOptions;
	private Integer traceLevel;
	private Properties additionalProperties;

	public SAPJcoConnection(ConnectorContext var) {
		super(var);
	}

	public SAPJcoConnection(BrowseContext context) {
		super(context);
		initConnectionSettings(context);
	}

	/**
	 * This method will initialize all the connection settings from UI.
	 * @param context
	 */
	private void initConnectionSettings(BrowseContext context) {
		PropertyMap connectionProperties = context.getConnectionProperties();
		this.connectionType = connectionProperties.getProperty("connectionType");
		this.server = connectionProperties.getProperty("server");
		this.userName = connectionProperties.getProperty("userName");
		this.password = connectionProperties.getProperty("password");
		this.client = connectionProperties.getProperty("client");
		this.languageCode = connectionProperties.getProperty("languageCode");
		this.systemNumber = connectionProperties.getProperty("systemNumber");
		this.maximumIdleConnections = parseInt(connectionProperties.getLongProperty("maximumIdleConnections"));
		this.maximumActiveConnections = parseInt(connectionProperties.getLongProperty("maximumActiveConnections"));
		this.idleTime = parseLong(connectionProperties.getProperty("idleTime"));
		this.listenerGatewayHost = connectionProperties.getProperty("listenerGatewayHost");
		this.listenerGatewayService = connectionProperties.getProperty("listenerGatewayService");
		this.databaseUserName = connectionProperties.getProperty("databaseUserName");
		this.databasePassword = connectionProperties.getProperty("databasePassword");
		this.databaseDriverType = connectionProperties.getProperty("databaseDriverType");
		this.customDriverClassName = connectionProperties.getProperty("customDriverClassName");
		this.customConnectionURL = connectionProperties.getProperty("customConnectionURL");
		this.databaseHost = connectionProperties.getProperty("databaseHost");
		this.databasePort = connectionProperties.getProperty("databasePort");
		this.databaseName = connectionProperties.getProperty("databaseName");
		this.additionalOptions = connectionProperties.getProperty("additionalOptions");
		this.minimumConnections = parseInt(connectionProperties.getLongProperty("minimumConnections"));
		this.maximumConnections = parseInt(connectionProperties.getLongProperty("maximumConnections"));
		this.systemName = connectionProperties.getProperty("systemName");
		this.groupName = connectionProperties.getProperty("groupName");
		this.listenerConnectionCount = parseInt(connectionProperties.getLongProperty("listenerConnectionCount"));
		this.additionalPropsFileName = connectionProperties.getProperty("additionalConnectionSettings");
		this.tidManagementOptions = connectionProperties.getProperty("tidManagementOptions");
		this.traceLevel = parseInt(connectionProperties.getLongProperty("traceLevel"));
		this.additionalProperties = loadAdditionalProperties(getAdditionalPropsFileName());
	}

	/**
	 * This method will validate and parse the Integer value from given string. 
	 * @param string
	 * @return Integer
	 */
	private Integer parseInt(Long l) {
		Integer i = null;
		if(l != null) {
			try {
				i = l.intValue();
			} catch (NumberFormatException e) {
				return i;
			}
		}
		return i;
	}

	/**
	 * This method will validate and parse the Long value from given string. 
	 * @param string
	 * @return Long
	 */
	private Long parseLong(String s) {
		try {
			return Long.parseLong(s);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * This method is responsible for initializing the destination.
	 */
	public void initDestination() {
		try {
			JCo.setTrace(getTraceLevel(), getTracePath());
			jcoDestination = JCoDestinationManager.getDestination(this.destinationName);
			// this implies we are using a destination that isn't ours
			throw new ConnectorException(
					"Unable to setup SAPConnection, Destination already exists but we didn't create it");
		} catch (JCoException e) {
			// expected, ignore
		}
		try {
			this.destinationProvider = SAPJcoConnection.getDestinationDataProvider();
			this.destinationProvider.registerDestination(this.destinationName, this.buildProperties());
		} catch (Exception ex) {
			logger.log(Level.SEVERE, "EXCEPTION WHILE CONNECTING TO DESTINATION : {0}", ex.getMessage());
		}
	}

	/**
	 * This method will create and register the destination data provider.
	 * @return sapDestinationDataProvider
	 */
	private static synchronized SAPDestinationDataProvider getDestinationDataProvider() {
		if (globalDestinationProvider == null) {
			SAPDestinationDataProvider destProvider = new SAPDestinationDataProvider();
			Environment.registerDestinationDataProvider((com.sap.conn.jco.ext.DestinationDataProvider) destProvider);
			globalDestinationProvider = destProvider;
		}
		return globalDestinationProvider;
	}

	public String getDestinationName() {
		return this.destinationName;
	}

	/**
	 * This method is responsible to initialize the jcoDestination with jcoDestinationManager.
	 * @return jcoDestination
	 * @throws JCoException
	 */
	public JCoDestination getDestination() throws JCoException {
		if (this.jcoDestination == null) {
			this.jcoDestination = JCoDestinationManager.getDestination(this.destinationName);
		}
		return this.jcoDestination;
	}

	/**
	 * This method will get the jcoFunction from registered destination repository.
	 * @param functionName
	 * @return jcoFunction
	 * @throws JCoException
	 */
	public JCoFunction getFunction(String name) throws JCoException {
		JCoRepository jCoRepository = this.getDestination().getRepository();
		return jCoRepository.getFunction(name);
	}

	/**
	 * This method will return the jcoFunctionTemplate from registered destination repository.
	 * @param name
	 * @return jcoFunctionTemplate
	 * @throws JCoException
	 */
	public JCoFunctionTemplate getFunctionTemplate(String name) throws JCoException {
		JCoRepository jCoRepository = this.getDestination().getRepository();
		return jCoRepository.getFunctionTemplate(name);
	}

	/**
	 * This method is responsible to clear the registered repository cache.
	 * @throws JCoException
	 */
	public void clearRepositoryCache() throws JCoException {
		this.getDestination().getRepository().clear();
	}

	/**
	 * This method will execute the given JcoFunction.
	 * @param function
	 * @throws JCoException
	 */
	public void executeFunction(JCoFunction function) throws JCoException {
		function.execute(this.getDestination());
	}

	/**
	 * This method will commit the executed BAPI transaction at sap destination.
	 * @throws JCoException
	 */
	public void commitBAPITx() throws JCoException {
		JCoFunction commitFunc = this.getFunction(BAPI_TRANSACTION_COMMIT);
		if (commitFunc == null) {
			throw new ConnectorException("BAPI_TRANSACTION_COMMIT not found, unable to commit bapi tranaction");
		}
		commitFunc.getImportParameterList().setValue("WAIT", "X");
		this.executeFunction(commitFunc);
	}

	/**
	 * This method will build the sap properties from connection properties.
	 * @return sapProperties
	 */
	private Properties buildProperties() {
		Properties connectionProperties = new Properties();
		String connType = (String) ObjectUtil.defaultIfNull((Object) getConnectionType(),
				(Object) SAPJcoConstants.AHOST);
		if (connType.equals(SAPJcoConstants.AHOST)) {
			if (getVersion().isLessThan(REFERENCE_VERSION)) {
				connectionProperties.put(DestinationDataProvider.JCO_MSHOST, StringUtil.EMPTY_STRING);
			}
			connectionProperties.put(DestinationDataProvider.JCO_R3NAME, StringUtil.EMPTY_STRING);
			connectionProperties.setProperty(DestinationDataProvider.JCO_ASHOST, getServer());
			connectionProperties.setProperty(DestinationDataProvider.JCO_SYSNR, getSystemNumber());
		} else if (connType.equals(SAPJcoConstants.MHOST)) {
			connectionProperties.put(DestinationDataProvider.JCO_MSHOST, getServer());
			connectionProperties.put(DestinationDataProvider.JCO_ASHOST, StringUtil.EMPTY_STRING);
			connectionProperties.put(DestinationDataProvider.JCO_SYSNR, StringUtil.EMPTY_STRING);
			connectionProperties.put(DestinationDataProvider.JCO_R3NAME, getSystemName());
			connectionProperties.put(DestinationDataProvider.JCO_GROUP, getGroupName());
		}

		connectionProperties.setProperty(DestinationDataProvider.JCO_CLIENT, getClient());
		connectionProperties.setProperty(DestinationDataProvider.JCO_USER, getUserName());
		connectionProperties.setProperty(DestinationDataProvider.JCO_PASSWD, getPassword());
		connectionProperties.setProperty(DestinationDataProvider.JCO_LANG, getLanguageCode());
		
		setIfNotNull(connectionProperties, DestinationDataProvider.JCO_POOL_CAPACITY, getMaximumIdleConnections());
		setIfNotNull(connectionProperties, DestinationDataProvider.JCO_PEAK_LIMIT, getMaximumActiveConnections());
		setIfNotNull(connectionProperties, DestinationDataProvider.JCO_EXPIRATION_TIME, getIdleTime());
		
		if (getAdditionalProperties().isEmpty()){
            logger.log(Level.FINE, "No SAP JCo properties file found for connection component");
        }
        for (Entry<Object,Object> e : getAdditionalProperties().entrySet()) {
            String key = (String) e.getKey();
            String value = (String) e.getValue();
            setIfNotNull(connectionProperties, key, value);
        }

		return connectionProperties;
	}
	
	/**
	 * This method returns the trace path where all the trace and error files get stored.
	 * @return tracePath
	 */
	private String getTracePath() {
		String path = "";
		File directory = new File("./");
		try {
			path = directory.getCanonicalPath() + File.separator + "logs";
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Exception while getting trace path : {0}" , e.getMessage());
		}
		return path;
	}

	/**
	 * This method will validate and sets the properties.
	 * @param props
	 * @param key
	 * @param value
	 */
	private void setIfNotNull(Properties props, String key, Object value) {
		if (value != null) {
			props.put(key, value);
		}
	}

	public String getConnectionType() {
		return connectionType;
	}

	public String getServer() {
		return server;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassword() {
		return password;
	}

	public String getClient() {
		return client;
	}

	public String getLanguageCode() {
		return languageCode;
	}

	public boolean isEnableTrace() {
		return enableTrace;
	}

	public Integer getMaximumIdleConnections() {
		return maximumIdleConnections;
	}

	public Integer getMaximumActiveConnections() {
		return maximumActiveConnections;
	}

	public Long getIdleTime() {
		return idleTime;
	}

	public String getListenerGatewayHost() {
		return listenerGatewayHost;
	}

	public String getListenerGatewayService() {
		return listenerGatewayService;
	}

	public String getDatabaseUserName() {
		return databaseUserName;
	}

	public String getDatabasePassword() {
		return databasePassword;
	}

	public String getDatabaseDriverType() {
		return databaseDriverType;
	}

	public String getCustomDriverClassName() {
		return customDriverClassName;
	}

	public String getCustomConnectionURL() {
		return customConnectionURL;
	}

	public String getDatabaseHost() {
		return databaseHost;
	}

	public String getDatabasePort() {
		return databasePort;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getAdditionalOptions() {
		return additionalOptions;
	}

	public Integer getMinimumConnections() {
		return minimumConnections;
	}

	public Integer getMaximumConnections() {
		return maximumConnections;
	}

	public String getSystemNumber() {
		return systemNumber;
	}

	public String getSystemName() {
		return systemName;
	}

	public String getGroupName() {
		return groupName;
	}
	
	public String getTidManagementOptions() {
		return tidManagementOptions;
	}
	
	public Properties getAdditionalProperties() {
		return additionalProperties;
	}
	
	/**
	 * This method will return the trace level count from connection settings.
	 * @return traceLevel.
	 */
	public Integer getTraceLevel() {
		if(traceLevel == null || traceLevel <= 0 || traceLevel > 10) {
			return 0;
		}
		return traceLevel;
	}
	
	/**
	 * This method will return the connection count from connection settings.
	 * @return ListenerConnectionCount.
	 */
	public Integer getListenerConnectionCount() {
		int defaultCount = 2;
		if(listenerConnectionCount == null || listenerConnectionCount <= 0 || listenerConnectionCount > 10) {
			return defaultCount;
		}
		return listenerConnectionCount;
	}

	/**
	 * This method returns the db driver class based on the db selected.
	 * @return driverClass
	 */
	public String getDbDriverClassName() {
		if (getDatabaseDriverType().equals(SAPJcoConstants.CUSTOM_DB)) {
			return getCustomDriverClassName();
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.SQL_SERVER_JTDS)) {
			return SAPJcoConstants.SQL_SERVER_JTDS_DRIVER_CLASS;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.ORACLE)) {
			return SAPJcoConstants.ORACLE_DRIVER_CLASS;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.MYSQL)) {
			return SAPJcoConstants.MYSQL_DRIVER_CLASS;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.SQL_SERVER_MICROSOFT)) {
			return SAPJcoConstants.SQL_SERVER_MICROSOFT_DRIVER_CLASS;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.SAP_HANA)) {
			return SAPJcoConstants.SAP_HANA_DRIVER_CLASS;
		}
		return dbDriverClassName;

	}
	
	public String getAdditionalPropsFileName() {
		return additionalPropsFileName;
	}
	
	/**
	 * This method will load the additional properties resource and read the properties.
	 * @param resourceName
	 * @return properties
	 */
	private static Properties loadAdditionalProperties(String resourceName){
		Properties props = new Properties();
		if(StringUtil.isNotBlank(resourceName)) {
			InputStream resource = null;
			try {
				resource = ClassUtil.getResourceAsStream(resourceName);
	        	if (resource != null) {
	        		props.putAll(PropertyUtil.load(resource));
	        	}
			}catch(Exception e) {
				logger.log(Level.SEVERE, "Exception while loading properties file {0}, Exception details:{1}",new Object[]{resourceName,e.getMessage()});
			}finally {
				IOUtil.closeQuietly(resource);
			}
		}
		return props;
    }

	/**
	 * This method will return the dbUrl based on the db selected.
	 * @return dbUrl
	 */
	public String getDbUrl() {
		if (getDatabaseDriverType().equals(SAPJcoConstants.CUSTOM_DB)) {
			return getCustomConnectionURL();
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.SQL_SERVER_JTDS)) {
			return SAPJcoConstants.SQL_SERVER_JTDS_CONNECTION_URL;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.ORACLE)) {
			return SAPJcoConstants.ORACLE_CONNECTION_URL;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.MYSQL)) {
			return SAPJcoConstants.MYSQL_CONNECTION_URL;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.SQL_SERVER_MICROSOFT)) {
			return SAPJcoConstants.SQL_SERVER_MICROSOFT_CONNECTION_URL;
		} else if (getDatabaseDriverType().equals(SAPJcoConstants.SAP_HANA)) {
			return SAPJcoConstants.SAP_HANA_CONNECTION_URL;
		}
		return dbUrl;
	}
	
    /**
     * This method will register and return the sapServerDataProvider.
     * @return sapServerDataProvider
     */
    public static synchronized SAPServerDataProvider getServerDataProvider() {
        if (sapServerDataProvider == null) {
            SAPServerDataProvider serverProvider = new SAPServerDataProvider();
            Environment.registerServerDataProvider((ServerDataProvider)serverProvider);
            sapServerDataProvider = serverProvider;
        }
        return sapServerDataProvider;
    }

    @Override
    public void close() throws IOException {
        if (this.destinationProvider == null) {
            try {
                JCoDestinationManager.getDestination((String)this.destinationName);
            }
            catch (JCoException jCoException) {
            	logger.log(Level.WARNING, "Unable to close SAPConnection, DestinationManager was missing", (Throwable) jCoException);
            }
        } else {
            this.destinationProvider.unregisterDestination(this.destinationName);
        }
    }

	/*
	 * Using reflection to access the library functions so we don't run into runtime
	 * errors if people are using an old library that doesn't support any of these
	 * classes/methods.
	 */
	private static SoftwareVersion getVersion() {
		try {
			Class<?> jcoRuntimeFactoryClass = Class.forName("com.sap.conn.jco.rt.JCoRuntimeFactory");
			Class<?> jcoRuntimeClass = Class.forName("com.sap.conn.jco.rt.JCoRuntime");

			Method getRuntimeMethod = jcoRuntimeFactoryClass.getMethod("getRuntime");
			Method getVersionMethod = jcoRuntimeClass.getMethod("getVersion");

			return SoftwareVersion.newInstance((String) getVersionMethod.invoke(getRuntimeMethod.invoke(null)));
		} catch (Exception e) {
			logger.log(Level.WARNING, "Unable to determine SAP connector library version.", e);
			return SoftwareVersion.UNKNOWN_VERSION;
		}
	}

	/*
	 * This class facilitates comparison of version strings. Version strings have
	 * the form: 3.0.6 (2010-08-24), 3.0.9 (2012-07-19), 3.0.19 (2018-12-03), 3.0.20
	 * (2019-09-05), 3.1.2 (2019-10-02)
	 */
	static class SoftwareVersion implements Comparable<SoftwareVersion> {
		private static final Pattern PATTERN_VERSION = Pattern.compile("^([0-9]+\\.[0-9]+\\.[0-9]+)\\s+.*$");

		public static final SoftwareVersion UNKNOWN_VERSION = new SoftwareVersion(0, 0, 0);

		private static final int INDEX_VERSION_MAJOR = 0;
		private static final int INDEX_VERSION_MINOR = 1;
		private static final int INDEX_VERSION_REVISION = 2;

		private final int major;
		private final int minor;
		private final int revision;

		public SoftwareVersion(int major, int minor, int revision) {
			this.major = major;
			this.minor = minor;
			this.revision = revision;
		}

		public int getMajor() {
			return major;
		}

		public int getMinor() {
			return minor;
		}

		public int getRevision() {
			return revision;
		}

		/**
		 * This method will compare the software version.
		 * @param softwareVersion
		 * @return true if softwareVersion < 0
		 */
		public boolean isLessThan(SoftwareVersion softwareVersion) {
			return compareTo(softwareVersion) < 0;
		}

		@Override
		public int compareTo(SoftwareVersion o) {
			int result = Integer.compare(major, o.major);

			if (result == 0) {
				result = Integer.compare(minor, o.minor);

				if (result == 0) {
					result = Integer.compare(revision, o.revision);
				}
			}

			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			} else if (obj == null) {
				return false;
			} else if (getClass() != obj.getClass()) {
				return false;
			}

			SoftwareVersion other = (SoftwareVersion) obj;
			// using apache EqualsBuilder since boomi sdk is removed this class.
			return new EqualsBuilder().append(major, other.major).append(minor, other.minor)
					.append(revision, other.revision).isEquals();
		}

		@Override
		public int hashCode() {
			// using apache HashCodeBuilder since boomi sdk is removed this class.
			return new HashCodeBuilder().append(major).append(minor).append(revision).toHashCode();
		}

		@Override
		public String toString() {
			// using apache ToStringBuilder since boomi sdk is removed this class.
			return new ToStringBuilder(this).append("verion", StringUtil.join(".", major, minor, revision)).toString();
		}

		/**
		 * This method will find and return the software version of the jar files.
		 * @param versionString
		 * @return softwareVersion
		 */
		public static SoftwareVersion newInstance(String versionString) {
			if (StringUtil.isNotBlank(versionString)) {
				Matcher matcher = PATTERN_VERSION.matcher(versionString);

				if (matcher.matches()) {
					String[] components = StringUtil.fastSplit('.', matcher.group(1));

					return new SoftwareVersion(Integer.parseInt(components[INDEX_VERSION_MAJOR]),
							Integer.parseInt(components[INDEX_VERSION_MINOR]),
							Integer.parseInt(components[INDEX_VERSION_REVISION]));
				}
			}

			return UNKNOWN_VERSION;
		}

	}

	
	
}