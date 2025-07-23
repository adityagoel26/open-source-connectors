// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.mongodb;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.AUTHDATABASE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.AUTHENTICATION_TYPE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.AUTH_TYPE_NONE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.COMMA;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.CONSTANT_MONGOPD;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.DATABASE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.HOSTNAME;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.JAAS_PATH;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.KDC;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.KERBEROS;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.KRB_PATH;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.LDAP;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.PORT;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.REALM;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.REPLICA_SET_MEMBERS;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.SCRAM_SHA_1;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.SCRAM_SHA_256;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.STATUS_CODE_SUCCESS;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.TRUST_STORE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.USER_CERTIFICATE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.USER_NAME;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.USESSL;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.X509;
import static com.mongodb.client.model.Filters.in;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PrivateKeyStore;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.PublicKeyStore;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.mongodb.bean.BatchResult;
import com.boomi.connector.mongodb.bean.ErrorDetails;
import com.boomi.connector.mongodb.bean.OutputDocument;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.CertificateUtils;
import com.boomi.connector.mongodb.util.DocumentUtil;
import com.boomi.connector.mongodb.util.ErrorUtils;
import com.boomi.connector.mongodb.util.MongoDBConnectorPayloadUtil;
import com.boomi.connector.util.BaseConnection;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.mongodb.Block;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterSettings.Builder;
import com.mongodb.connection.SslSettings;

/**
 * Logic to initialize the Boomi Connector and handle connection to MongoDB.
 *
 */
@SuppressWarnings("rawtypes")
public class MongoDBConnectorConnection extends BaseConnection {

	/** The logger. */
	public static final Logger logger = Logger.getLogger(MongoDBConnectorConnection.class.getName());

	/** The exceptions termed failure. */
	@SuppressWarnings("rawtypes")
	private static List<Class> exceptionsTermedFailures = initListOfExceptionsTermedFailure();

	/** The mongo client. */
	private MongoClient _mongoClient;

	/** The mongo name space. */
	MongoNamespace mongoNameSpace = null;
	
	/** The mongo DB. */
	private MongoDatabase _mongoDB;

	/** The connection url. */
	private String connectionUrl;

	/** The collection. */
	MongoCollection<Document> collection = null;
	
	/** The number parser. */
	private NumberFormat numberParser = null;

	/** The database name. */
	private String databaseName;

	/**
	 * Inits the list of exceptions termed failure.
	 *
	 * @return the list
	 */
	@SuppressWarnings("rawtypes")
	private static List<Class> initListOfExceptionsTermedFailure() {
		@SuppressWarnings("rawtypes")
		List<Class> listOfExceptionsTermedFailure = new ArrayList<>();
		listOfExceptionsTermedFailure.add(MongoTimeoutException.class);
		listOfExceptionsTermedFailure.add(MongoDBConnectException.class);
		listOfExceptionsTermedFailure.add(MongoSecurityException.class);
		listOfExceptionsTermedFailure.add(MongoCommandException.class);
		listOfExceptionsTermedFailure.add(MongoSocketOpenException.class);
		listOfExceptionsTermedFailure.add(MongoSocketWriteException.class);
		return listOfExceptionsTermedFailure;
	}

	/**
	 * Instantiates a new mongo DB connector connection.
	 *
	 * @param context the context
	 */
	public MongoDBConnectorConnection(BrowseContext context) {
		super(context);
		databaseName = context.getConnectionProperties().getProperty(DATABASE);
	}

	/**
	 * Initialise database.
	 */
	public void initialiseDatabase() {
		if(null == _mongoClient) {
			openConnection();
		}
		_mongoDB = _mongoClient.getDatabase(getDatabaseName());
	}

	/**
	 * 
	 * specifies the settings of a connection to a MongoDB server.
	 */
	public void openConnection() {
		PropertyMap connectionProperties = getContext().getConnectionProperties();
		String replicaSetMembers = connectionProperties.getProperty(REPLICA_SET_MEMBERS);
		String authType = connectionProperties.getProperty(AUTHENTICATION_TYPE);
		MongoClientSettings clientSettings = null;
		boolean mongosrv = connectionProperties.getBooleanProperty(MongoDBConstants.MONGO_SRV, false);
		if (mongosrv) {
			String connectionString = connectionProperties.getProperty(MongoDBConstants.CONNECTION_STRING);
			String username = connectionProperties.getProperty(MongoDBConstants.USER_NAME);
			String password = connectionProperties.getProperty(MongoDBConstants.CONSTANT_MONGOPD);
			if (StringUtil.isNotBlank(username) && StringUtil.isNotBlank(password)) {
				String replace = new StringBuffer(MongoDBConstants.DOUBLE_FORWARD_SLASH).append(username)
						.append(MongoDBConstants.COLON).append(password).append(MongoDBConstants.AT).toString();
				connectionString = connectionString.replace(MongoDBConstants.DOUBLE_FORWARD_SLASH, replace);
			}
			_mongoClient = MongoClients.create(connectionString);
		} else {
			if (AUTH_TYPE_NONE.equals(authType)) {
				clientSettings = this.getMongoClientSettingNoAuth(replicaSetMembers, connectionProperties);
			} else if (X509.equals(authType)) {
				clientSettings = this.getMongoClientSettingX509(replicaSetMembers, connectionProperties);
			} else if (SCRAM_SHA_1.equals(authType) || SCRAM_SHA_256.equals(authType) || LDAP.equals(authType)) {
				clientSettings = this.getMongoClientSettingSHA1SHA256LDAP(replicaSetMembers, connectionProperties,
						authType);
			} else if (KERBEROS.equals(authType)) {
				clientSettings = this.getMonClienSettingsGSSAPAI(replicaSetMembers, connectionProperties, authType);
			}

			if(clientSettings != null) {
			_mongoClient = MongoClients.create(clientSettings);
			}
		}
	}

	private static MongoClientSettings getMongoClientSettings (final List<ServerAddress> serverAddressList, final boolean useSSL, final MongoCredential credential){
		return MongoClientSettings.builder().applyToClusterSettings(new Block<ClusterSettings.Builder>() {
			@Override
			public void apply(Builder builder) {
				builder.hosts(serverAddressList);

			}
		}).applyToSslSettings(new Block<SslSettings.Builder>() {

			@Override
			public void apply(SslSettings.Builder builder) {
				builder.enabled(useSSL);

			}
		}).credential(credential).retryWrites(true).build();
	}

	/**
	 * specifies the GSSAPI authentication connection settings to a MongoDB server.
	 *
	 * @param replicaSetMembers    the replica set members
	 * @param connectionProperties the connection properties
	 * @param authType             the auth type
	 * @return the mongo client setting SHA 1 SHA 256 LDAP
	 */

	private MongoClientSettings getMonClienSettingsGSSAPAI(String replicaSetMembers, PropertyMap connectionProperties,
			String authType) {
		String userName = connectionProperties.getProperty(USER_NAME);
		final boolean useSsl = connectionProperties.getBooleanProperty(USESSL, false);
		final String host = connectionProperties.getProperty(HOSTNAME);
		final String port = connectionProperties.getProperty(PORT);
		MongoCredential credential = null;
		String kdc = connectionProperties.getProperty(KDC);
		String realm = connectionProperties.getProperty(REALM);
		String krb5confPath = connectionProperties.getProperty(KRB_PATH);
		String jassConfPath = connectionProperties.getProperty(JAAS_PATH);

		MongoClientSettings clientSettings = null;
		if (KERBEROS.equals(authType)) {
			System.setProperty("java.security.krb5.conf", krb5confPath);
			System.setProperty("java.security.krb5.realm", realm);
			System.setProperty("java.security.krb5.kdc", kdc);
			System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
			System.setProperty("java.security.auth.login.config", jassConfPath);
			credential = MongoCredential.createGSSAPICredential(userName);
		}
		if (StringUtil.isNotBlank(replicaSetMembers)) {
			final List<ServerAddress> replicate = getServerAddressList(replicaSetMembers);
			if(credential != null) {
				clientSettings = getMongoClientSettings(replicate, useSsl, credential);
			}
		} else {
			if(credential != null) {
				clientSettings = getMongoClientSettings((Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))), useSsl, credential);
			}
		}
		return clientSettings;
	}

	/**
	 * specifies the LDAP,SCRAMSHA-1/256 authentication connection settings to a
	 * MongoDB server.
	 *
	 * @param replicaSetMembers    the replica set members
	 * @param connectionProperties the connection properties
	 * @param authType             the auth type
	 * @return the mongo client setting SHA 1 SHA 256 LDAP
	 */

	private MongoClientSettings getMongoClientSettingSHA1SHA256LDAP(String replicaSetMembers,
			PropertyMap connectionProperties, String authType) {
		MongoCredential credential = null;
		String userName = connectionProperties.getProperty(USER_NAME);
		String password = connectionProperties.getProperty(CONSTANT_MONGOPD);
		String authDatabase = connectionProperties.getProperty(AUTHDATABASE);
		final boolean useSsl = connectionProperties.getBooleanProperty(USESSL, false);
		final String host = connectionProperties.getProperty(HOSTNAME);
		final String port = connectionProperties.getProperty(PORT);
		MongoClientSettings clientSettings = null;

		if (SCRAM_SHA_1.equals(authType)) {
			credential = MongoCredential.createScramSha1Credential(userName, authDatabase, password.toCharArray());

		}
		if (SCRAM_SHA_256.equals(authType)) {
			credential = MongoCredential.createScramSha256Credential(userName, authDatabase, password.toCharArray());
		}
		if (LDAP.equals(authType)) {
			credential = MongoCredential.createPlainCredential(userName, authDatabase, password.toCharArray());
		}

		if (StringUtil.isNotBlank(replicaSetMembers)) {
			final List<ServerAddress> replicate = getServerAddressList(replicaSetMembers);
			if(credential != null) {
				clientSettings = getMongoClientSettings(replicate, useSsl, credential);
			}
		} else {
			if(credential != null) {
				clientSettings = getMongoClientSettings((Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))), useSsl, credential);
			}
		}
		return clientSettings;
	}

	/**
	 * Creates the mongo client setting X 509 for connection to the Mongo server.
	 *
	 * @param replicaSetMembers    the replica set members
	 * @param connectionProperties the connection properties
	 * @return the mongo client setting X 509
	 */

	private MongoClientSettings getMongoClientSettingX509(String replicaSetMembers, PropertyMap connectionProperties) {
		MongoClientSettings clientSettings = null;
		String x509SubjectName = null;
		MongoCredential credential = null;
		final String host = connectionProperties.getProperty(HOSTNAME);
		final String port = connectionProperties.getProperty(PORT);
		final boolean useSsl = connectionProperties.getBooleanProperty(USESSL, false);
		PrivateKeyStore privateKeyStore = connectionProperties.getPrivateKeyStoreProperty(USER_CERTIFICATE);
		PublicKeyStore trustStore = connectionProperties.getPublicKeyStoreProperty(TRUST_STORE);
		try {
			if (null != privateKeyStore) {
				x509SubjectName = getPrivateKeyStoreDetails(privateKeyStore);
			}
			final SSLContext sslContext = ((BrowseContext) getContext()).getSSLContext(trustStore, privateKeyStore);
			if (sslContext != null) {
				if(x509SubjectName != null)
					credential = MongoCredential.createMongoX509Credential(x509SubjectName);
				if (StringUtil.isNotBlank(replicaSetMembers)) {
					final List<ServerAddress> replicate = getServerAddressList(replicaSetMembers);
					if(credential != null) {
						clientSettings = MongoClientSettings.builder()
								.applyToClusterSettings(new Block<ClusterSettings.Builder>() {

									@Override
									public void apply(Builder t) {
										t.hosts(replicate);

									}
								}).applyToSslSettings(new Block<SslSettings.Builder>() {

									@Override
									public void apply(SslSettings.Builder t) {
										t.enabled(useSsl);
										t.context(sslContext);

									}
								}).credential(credential).retryWrites(true).build();
					}
				} else {
					if(credential != null) {
						clientSettings = MongoClientSettings.builder()
								.applyToClusterSettings(new Block<ClusterSettings.Builder>() {

									@Override
									public void apply(Builder t) {
										t.hosts((Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))));

									}
								}).applyToSslSettings(new Block<SslSettings.Builder>() {

									@Override
									public void apply(SslSettings.Builder t) {
										t.enabled(useSsl);
										t.context(sslContext);

									}
								}).credential(credential).retryWrites(true).build();
					}
				}

			}
		} catch (KeyStoreException ex) {
			logger.log(Level.SEVERE, ex.getMessage(), ex);
		} catch (Exception e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			StringBuilder errMsg = new StringBuilder("Error in connecting to MongoDB, Unable to get the SSL Context- ")
					.append(e.getMessage());
			throw new ConnectorException(errMsg.toString());
		}
		return clientSettings;
	}

	private String getPrivateKeyStoreDetails(PrivateKeyStore privateKeyStore) throws KeyStoreException {
		String x509SubjectName;
		String aliasName;
		KeyStore keystore = privateKeyStore.getKeyStore();
		aliasName = privateKeyStore.getKeyStore().aliases().nextElement();
		if (!aliasName.isEmpty()) {
			x509SubjectName = CertificateUtils.fetchCertSubject(keystore, aliasName, logger);
		} else {
			x509SubjectName = CertificateUtils.fetchCertSubject(keystore, "1", logger);
		}
		return x509SubjectName;
	}

	/**
	 * Creates the username and password authentication for connection to the Mongo
	 * server.
	 *
	 * @param replicaSetMembers    the replica set members
	 * @param connectionProperties the connection properties
	 * @return the mongo client setting no auth
	 */

	private MongoClientSettings getMongoClientSettingNoAuth(String replicaSetMembers,
			PropertyMap connectionProperties) {
		MongoClientSettings clientSettings = null;
		final String host = connectionProperties.getProperty(HOSTNAME);
		final String port = connectionProperties.getProperty(PORT);
		final boolean useSsl = connectionProperties.getBooleanProperty(USESSL, false);
		if (StringUtil.isNotBlank(replicaSetMembers)) {
			final List<ServerAddress> replicate = getServerAddressList(replicaSetMembers);
			clientSettings = MongoClientSettings.builder().applyToClusterSettings(new Block<ClusterSettings.Builder>() {

				@Override
				public void apply(Builder t) {
					t.hosts(replicate);

				}
			}).applyToSslSettings(new Block<SslSettings.Builder>() {

				@Override
				public void apply(SslSettings.Builder t) {
					t.enabled(useSsl);

				}
			}).build();
		} else {
			clientSettings = MongoClientSettings.builder().applyToClusterSettings(new Block<ClusterSettings.Builder>() {

				@Override
				public void apply(Builder t) {
					t.hosts((Arrays.asList(new ServerAddress(host, Integer.parseInt(port)))));

				}
			}).applyToSslSettings(new Block<SslSettings.Builder>() {

				@Override
				public void apply(SslSettings.Builder t) {
					t.enabled(useSsl);
				}
			}).build();

		}
		return clientSettings;
	}

	/**
	 * Gets the server address list for building the connection settings of MongoDB
	 * server.
	 *
	 * @param replicaSetMembers the replica set members
	 * @return the server address list
	 */
	private List<ServerAddress> getServerAddressList(String replicaSetMembers) {
		List<ServerAddress> serverAddressList = new ArrayList<>();
		String[] memberList = replicaSetMembers.split("\\s*,\\s*");
		for (String member : memberList) {
			String[] hostAndPort = member.split(":");
			ServerAddress serverAddress = new ServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
			serverAddressList.add(serverAddress);
		}
		return serverAddressList;
	}

	/**
	 * Close the client, which will close all underlying cached resources.
	 * MongoClient internal connection pooling managed by MongoDB Close where
	 * possible
	 */
	public void closeConnection() {
		setCollection(null);
		if (null != _mongoClient)
			_mongoClient.close();

	}

	/**
	 * Sets the projections in query.
	 *
	 * @param query       the query
	 * @param projections the projections
	 * @return the find iterable
	 */
	public FindIterable<Document> setProjectionsInQuery(FindIterable<Document> query, String projections) {
		String[] projectionFields = null;
		projectionFields = StringUtil.isNotEmpty(projections) ? projections.split(COMMA) : null;
		if (null != projectionFields) {
			List<Bson> projectionDetails = new ArrayList<>();
			projectionDetails.add(Projections.include(projectionFields));
			query = query.projection(Projections.fields(projectionDetails));
		}
		return query;
	}

	/**
	 * Find last updated document in the MongoDB.
	 *
	 * @param collectionName the collection name
	 * @return the document
	 */
	public Document findLastUpdatedDoc(String collectionName) {
		Document doc = null;
		MongoCollection<Document> coll = getCollection(collectionName);
		FindIterable<Document> query = coll.find().sort(Sorts.descending("$natural")).limit(1);
		doc = query.first();
		return doc;
	}

	/**
	 * Format input for string type.
	 *
	 * @param input the input
	 * @param field the field
	 * @return the object
	 * @throws UnsupportedEncodingException 
	 * @throws ConnectorException
	 */
	Object formatInputForStringType(String input, String field) throws MongoDBConnectException {
		Object val = null;
			if (ObjectId.isValid(field)) {
				val = new ObjectId(field);
			}
			else {
				throw new MongoDBConnectException(new StringBuffer("Invalid input in param value- ").append(field)
						.append(MongoDBConstants.FIELD).append(field).toString());
			}
		return val;
	}
	
	/**
	 * Format input for number type.
	 *
	 * @param input the input
	 * @param field the field
	 * @return the number
	 * @throws ConnectorException the connector exception
	 */
	Number formatInputForNumberType(String input, String field) throws ConnectorException  {
		Number numberValue = null;
		try {
			numberValue = getNumberParser().parse(field);
		} catch (ParseException e) {
			throw new ConnectorException(new StringBuffer("Invalid number format in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
		return numberValue;
	}
	/**
	 * Format input for Decimal type.
	 * @param input
	 * @param field
	 * @return
	 * @throws ConnectorException
	 */
	Decimal128 formatInputForDecimal128Type(String input, String field) throws ConnectorException {
		Decimal128 decimalValue = null;
		try {
			decimalValue = new Decimal128(new BigDecimal(field));
		} catch (NumberFormatException e) {
			throw new ConnectorException(new StringBuffer("Invalid number format in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
		return decimalValue;
	}

	/**
	 * Format input for boolean type.
	 *
	 * @param input the input
	 * @param field the field
	 * @return the boolean
	 * @throws ConnectorException the mongo DB connect exception
	 */
	Boolean formatInputForBooleanType(String input, String field) throws ConnectorException {
		Boolean booleanValue = null;
		if (MongoDBConstants.BOOLEAN_TRUE.equalsIgnoreCase(field)
				|| MongoDBConstants.BOOLEAN_FALSE.equalsIgnoreCase(field)) {
			booleanValue = Boolean.parseBoolean(field);
		} else {
			throw new ConnectorException(new StringBuffer("Invalid Boolean input in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
		return booleanValue;
	}

	/**
	 * Format input for double type.
	 *
	 * @param input the input
	 * @param field the field
	 * @return the double
	 * @throws ConnectorException the mongo DB connect exception
	 */
	Double formatInputForDoubleType(String input, String field) throws ConnectorException {
		Double doubleValue = null;
		try {
			if (StringUtil.isBlank(field)) {
				throw new ConnectorException(
						new StringBuffer("Blank param value not allowed for field-").append(field).toString());
			}
			doubleValue = Double.parseDouble(field);
		} catch (NumberFormatException e) {
			throw new ConnectorException(new StringBuffer("Invalid Double input in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
		return doubleValue;
	}

	/**
	 * Format input for long type.
	 *
	 * @param input the input
	 * @param field the field
	 * @return the long
	 * @throws ConnectorException the mongo DB connect exception
	 */
	Long formatInputForLongType(String input, String field) throws ConnectorException {
		Long longValue = null;
		try {
			longValue = Long.parseLong(field);
		} catch (NumberFormatException e) {
			throw new ConnectorException(new StringBuffer("Invalid Long input in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
		return longValue;
	}

	/**
	 * Format input for null type.
	 *
	 * @param input the input
	 * @param field the field
	 * @return the long
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	Long formatInputForNullType(String input, String field) throws ConnectorException {
		if (!MongoDBConstants.NULL_STRING.equalsIgnoreCase(field)) {
			throw new ConnectorException(new StringBuffer("Invalid input in param value- ").append(input)
					.append(MongoDBConstants.FIELD).append(field).toString());
		}
		return null;
	}

	/**
	 * Gets the number parser.
	 *
	 * @return the number parser
	 */
	public NumberFormat getNumberParser() {
		if (null == numberParser) {
			numberParser = NumberFormat.getInstance();
		}
		return numberParser;
	}
	
	/**
	 * Checks if is error recoverable before going for the retry attempt.
	 *
	 * @param ex the ex
	 * @return true, if is error recoverable
	 */
	public boolean isErrorRecoverable(Exception ex) {
		return (ex instanceof MongoTimeoutException);
	}

	/**
	 * Executes a query using the given ObjectId.
	 *
	 * @param id             The Document ObjectId to fetch
	 * @param objectTypeId   the object type id
	 * @param dataType the response logger
	 * @return Json String of the query result
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */

	public Document doGet(String objectTypeId, String id, String dataType) throws MongoDBConnectException {
		return MongoDBConnectorConnectionExt.findDocumentById(this, objectTypeId, id, null, dataType);
	}
	

	/**
	 * Process errors occured during the execution of the operations and send the
	 * final result as the output payload to the platform.
	 *
	 * @param ex                        the ex
	 * @param batch                     the batch
	 * @param unsuccessfulRecordsResult the unsuccessful records result
	 * @return the batch result
	 */
	public BatchResult processError(Exception ex, List<TrackedDataWrapper> batch,
			BatchResult unsuccessfulRecordsResult) {
		if (null != ex) {
			boolean isBatchFailed = isBatchFailed(ex);
			unsuccessfulRecordsResult.setMarkBatchAsFailed(isBatchFailed);
			if (isBatchFailed) {
				ErrorUtils.updateErrorDetailsinBatch(ex, batch);
			} else if (ex instanceof MongoWriteException) {
				int failedRecIndex = 0;
				WriteError error = ((MongoWriteException) ex).getError();
				batch.get(failedRecIndex).setErrorDetails(error.getCode(), error.getMessage());
				unsuccessfulRecordsResult.getFailedRecords().add(batch.get(failedRecIndex));
				unsuccessfulRecordsResult.getFailedRecIndexes().add(failedRecIndex);
			} else if (ex instanceof MongoBulkWriteException) {
				mongoBulkWriteExceptionExt(ex, batch, unsuccessfulRecordsResult);
			} else if (ex instanceof MongoWriteConcernException) {
				WriteConcernError writeConcernErr = ((MongoWriteConcernException) ex).getWriteConcernError();
				WriteConcernResult writeConcernResult = ((MongoWriteConcernException) ex).getWriteResult();
				writeConcernResult.getCount();
				unsuccessfulRecordsResult.getFailedRecords().addAll(batch);
				for (int index = 0; index < batch.size(); index++) {
					unsuccessfulRecordsResult.getFailedRecIndexes().add(index);
				}
				ErrorUtils.updateErrorDetails(batch, writeConcernErr.getCode(), writeConcernErr.getMessage());
			} else {
				unsuccessfulRecordsResult.setMarkBatchAsFailed(false);
				ErrorUtils.updateErrorDetailsinBatch(ex, batch);
			}
		}
		return unsuccessfulRecordsResult;
	}

	private void mongoBulkWriteExceptionExt(Exception ex, List<TrackedDataWrapper> batch,
			BatchResult unsuccessfulRecordsResult) {
		List<BulkWriteError> bulWriteErrDetails = ((MongoBulkWriteException) ex).getWriteErrors();
		for (int i = 0; i < bulWriteErrDetails.size(); i++) {
			BulkWriteError bulkWriteErr = bulWriteErrDetails.get(i);
			int recIndex = bulkWriteErr.getIndex();
			TrackedDataWrapper trackedInputData = batch.get(recIndex);
			TrackedData trackedData = trackedInputData.getTrackedData();
			trackedData.getLogger().severe(bulkWriteErr.getMessage());
			trackedInputData.setErrorDetails(bulkWriteErr.getCode(), bulkWriteErr.getMessage());
			unsuccessfulRecordsResult.getFailedRecords().add(trackedInputData);
			unsuccessfulRecordsResult.getFailedRecIndexes().add(recIndex);
		}
	}

	/**
	 * Process the error occured during the execution of the Query operation.
	 *
	 * @param ex the ex
	 * @return the error details
	 */
	public ErrorDetails processQueryError(Exception ex) {
		ErrorDetails errorDetails = null;
		Integer errorCode = null;
		if (null != ex) {
			if (ex instanceof MongoException) {
				errorCode = ((MongoException) ex).getCode();
			}
			errorDetails = new ErrorDetails(errorCode, ex.getMessage());
		}
		return errorDetails;
	}

	/**
	 * Update the response for a given operation and send the final result as an
	 * output payload to the platform.
	 * 
	 * @param operationResponse         the operation response
	 * @param ex                        the ex
	 * @param batch                     the batch
	 * @param inputWrapper              the input wrapper
	 * @param unsuccessfulRecordsResult the unsuccessful records result
	 * @throws MongoDBConnectException 
	 */
	public void updateOperationResponse(OperationResponse operationResponse, Exception ex,
			List<TrackedDataWrapper> batch, InputWrapper inputWrapper, BatchResult unsuccessfulRecordsResult) {
		unsuccessfulRecordsResult = processError(ex, batch, unsuccessfulRecordsResult);
		updateBatchStatus(unsuccessfulRecordsResult.isMarkBatchAsFailed(), batch,
				unsuccessfulRecordsResult.getFailedRecords(), unsuccessfulRecordsResult.getFailedRecIndexes(),
				inputWrapper, operationResponse);
	}

	/**
	 * Checks if the batch is failed.
	 *
	 * @param ex the ex
	 * @return true, if is batch failed
	 */
	public boolean isBatchFailed(Exception ex) {
		boolean isBatchFailed = false;
		if (null != ex) {
			for (@SuppressWarnings("rawtypes") Class failureEx : exceptionsTermedFailures) {
				isBatchFailed = failureEx.isInstance(ex);
				if (isBatchFailed) {
					break;
				}
			}
		}
		return isBatchFailed;
	}

	/**
	 * Executes Insert operation into MongoCollection.
	 *
	 * @param document     the document
	 * @param objectTypeId the object type id
	 */

	public void doCreate(List<TrackedDataWrapper> document, String objectTypeId) {
		List<Document> docsList = DocumentUtil.getDocsFromInputBatch(document);
		MongoCollection<Document> coll = getCollection(objectTypeId);
		if (!document.isEmpty()) {
			if (docsList.size() == 1) {
				coll.insertOne(docsList.get(0));
			} else {
				InsertManyOptions insertManyOptions = new InsertManyOptions();
				insertManyOptions.ordered(false);
				coll.insertMany(docsList, insertManyOptions);
			}
		}
	}

	/**
	 * Construct filter by id.
	 *
	 * @param inputData      the input data
	 * @return the bson
	 */
	private Bson constructFilterById(TrackedDataWrapper inputData, boolean isObjectIdData) {
		String sObjectId = null;
		ArrayList<Object> filterList = new ArrayList<>();
		Object filterId = inputData.getDoc();
		if (isObjectIdData && null != inputData.getObjectId()) {
			sObjectId = inputData.getObjectId().toString();
			if (ObjectId.isValid(sObjectId)) {
				filterList.add(new ObjectId(sObjectId));
			} else {
				filterList.add(inputData.getObjectId());
			}
		} else if (null == inputData.getObjectId() && filterId == null) {
			filterList.add(inputData.getObjectId());
		} else {
			Object idValue = inputData.getDoc().get(MongoDBConstants.ID_FIELD_NAME);
			filterList.add(idValue);
			getDataTypeValue(inputData, filterList, idValue);
			}
		return in(MongoDBConstants.ID_FIELD_NAME, filterList);
	}

	private void getDataTypeValue(TrackedDataWrapper inputData, ArrayList<Object> filterList, Object idValue) {
		if (idValue instanceof Document) {
			logger.info("constructFilterById _id class"
					+ inputData.getDoc().get(MongoDBConstants.ID_FIELD_NAME).getClass().getName());
		} else if (idValue instanceof ObjectId) {
			filterList.add(((ObjectId) idValue).toHexString());
		} else if (idValue instanceof String || idValue instanceof Boolean || idValue instanceof Double
				|| idValue instanceof Integer || idValue instanceof Decimal128 || idValue instanceof Long) {
			if (ObjectId.isValid(idValue.toString())) {
				filterList.add(new ObjectId(idValue.toString()));
			}
		} else if (idValue != null) {
			throw new IllegalArgumentException(idValue.getClass() + " id type is not supported");
		}
	}
	
	/**
	 * Construct delete filter.
	 *
	 * @param objectIdDataBatch the object id data batch
	 * @return the bson
	 */
	private Bson constructDeleteFilter(List<TrackedDataWrapper> objectIdDataBatch) {
		Bson deleteFilter = null;
		List<Bson> idMatchFilters = new ArrayList<>();
		for (TrackedDataWrapper objectIdData : objectIdDataBatch) {
			idMatchFilters.add(constructFilterById(objectIdData, true));
		}
		deleteFilter = Filters.or(idMatchFilters);
		return deleteFilter;
	}


	/**
	 * Performs a delete operation in the MongoDB for the provided object id.
	 *
	 * @param document     the document
	 * @param objectTypeId the object type id
	 */
	public void doDelete(List<TrackedDataWrapper> document, String objectTypeId) {
		Bson batchDeleteFilter = constructDeleteFilter(document);
		MongoCollection<Document> coll = getCollection(objectTypeId);
		if (!document.isEmpty()) {
			if (document.size() == 1) {
				coll.deleteOne(batchDeleteFilter);
			} else {
				coll.deleteMany(batchDeleteFilter);
			}
		}
	}

	/**
	 * Checks the status of the current batch and updates the response accordingly
	 * and sends the status to the payload.
	 *
	 * @param markBatchAsFailed the mark batch as failed
	 * @param batch             the batch
	 * @param appErrorRecords   the app error records
	 * @param appErrRecIndexes  the app err rec indexes
	 * @param inputWrapper      the input wrapper
	 * @param response          the response
	 * @throws MongoDBConnectException
	 */
	
	private void updateBatchStatus(boolean markBatchAsFailed, List<TrackedDataWrapper> batch,
			List<TrackedDataWrapper> appErrorRecords, List<Integer> appErrRecIndexes, InputWrapper inputWrapper,
			OperationResponse response) {
			if (markBatchAsFailed) {
				updateResponseForList(batch, response, OperationStatus.FAILURE);
			} else {
				inputWrapper.getAppErrorRecords().addAll(appErrorRecords);
 				updateResponseForList(inputWrapper.getAppErrorRecords(), response, OperationStatus.APPLICATION_ERROR);
 				for (int i = 0; i < batch.size(); i++) {
 					if(!appErrRecIndexes.contains(i)) {
 					 			try(Payload payload = MongoDBConnectorPayloadUtil
 					 							.toPayload(new OutputDocument(OperationStatus.SUCCESS, batch.get(i)))){
 					 					response.addResult(batch.get(i).getTrackedData(), OperationStatus.SUCCESS,
 					 							MongoDBConstants.STATUS_CODE_SUCCESS, MongoDBConstants.STATUS_MESSAGE_SUCCESS, payload);
 					 				} catch (Exception e) {
 					 					throw new ConnectorException(e);
 					 				}
 					}
 					}
			inputWrapper.getFailedRecords().clear();
			inputWrapper.getSucceededRecords().clear();
			inputWrapper.getAppErrorRecords().clear();
			}
			}
	/**
	 * Update response for list.
	 *
	 * @param inputRecords the input records
	 * @param response     the response
	 * @param status       the status
	 * @throws MongoDBConnectException 
	 */
	private void updateResponseForList(List<TrackedDataWrapper> inputRecords, OperationResponse response,
			OperationStatus status){
			for (TrackedDataWrapper input : inputRecords) {
				ErrorDetails erroDetails = input.getErrorDetails();
				try(Payload payload = MongoDBConnectorPayloadUtil.toPayload(new OutputDocument(status, input))){
					response.addResult(input.getTrackedData(), status, ErrorUtils.fetchErrorCode(erroDetails),
							erroDetails.getErrorMessage(), payload);
				} catch (Exception e) {
					throw new ConnectorException(e);
				}
		}
	}

	/**
	 * Prepare input configuration using
	 * {@linkQUERY_BATCHSIZE,DEFAULTMAXRETRY,INCLUDE_SIZE_EXCEEDED_PAYLOAD}.
	 *
	 * @param operationContext the operation context
	 * @param responseLogger   the response logger
	 * @return the map
	 */
	public Map<String, Object> prepareInputConfig(OperationContext operationContext, Logger responseLogger) {
		Map<String, Object> inputConfig = new HashMap<>();
		PropertyMap operationProperties = operationContext.getOperationProperties();
		Long batchSizeLong = operationProperties.getLongProperty(MongoDBConstants.QUERY_BATCHSIZE,
				MongoDBConstants.DEFAULTBATCHSIZE);
		AtomConfig atomConfig = operationContext.getConfig();
		int batchSize = Math.min(batchSizeLong.intValue(), atomConfig.getMaxPageSize());
		boolean includeSizeLimit = includeSizeLimitForInput(operationContext);
		if (batchSize != batchSizeLong) {
			String logMessage = new StringBuffer("Batch size reduced to max batch size allowed in atom config-")
					.append(batchSize).toString();
			responseLogger.info(logMessage);
		}
		inputConfig.put(MongoDBConstants.QUERY_BATCHSIZE, batchSize);
		inputConfig.put(MongoDBConstants.IS_UPSERT_OPERATION,
				OperationType.UPSERT.equals(operationContext.getOperationType()));
		inputConfig.put(MongoDBConstants.INCLUDE_SIZE_EXCEEDED_PAYLOAD, includeSizeLimit);
		return inputConfig;
	}

	/**
	 * Adds the field in output if not exists.
	 *
	 * @param inputDoc the input doc
	 * @param field    the field
	 * @param value    the value
	 */
	private void addFieldInOutputIfNotExists(Document inputDoc, String field, Object value) {
		if (!inputDoc.containsKey(field)) {
			inputDoc.append(field, value);
		}
	}

	/**
	 * Include size limit for input.
	 *
	 * @param operationContext the operation context
	 * @return true, if successful
	 */
	private boolean includeSizeLimitForInput(OperationContext operationContext) {
		PropertyMap operationProperties = operationContext.getOperationProperties();
		OperationType operationType = ((BrowseContext) getContext()).getOperationType();
		if (OperationType.DELETE == operationType || OperationType.QUERY == operationType) {
			return false;
		} else {
			return (operationProperties.getBooleanProperty(MongoDBConstants.INCLUDE_SIZE_EXCEEDED_PAYLOAD));
		}
	}

	/**
	 * Performs update/upsert operation based on the selection done on the
	 * platform. @link findOneAndReplace() is being used , if we get one document at
	 * a time .{@linkbulkWrite()} is used when we use batch of documents. A boolean
	 * flag isUpsert is being used to differentiate upsert from update.
	 *
	 * @param document     the document
	 * @param objectTypeId the object type id
	 * @param isUpsert     the is upsert
	 * @throws MongoException 
	 */
	public void doModify(List<TrackedDataWrapper> document, String objectTypeId, boolean isUpsert) throws MongoException {
		boolean modifyOne = document.size() == 1;
		MongoCollection<Document> coll = getCollection(objectTypeId);
		if (!document.isEmpty()) {
			if (modifyOne) {
				TrackedDataWrapper input = document.get(0);
				Bson filterDocument = constructFilterById(input, false);
				FindOneAndReplaceOptions findOneAndReplace = new FindOneAndReplaceOptions();
				findOneAndReplace.upsert(isUpsert);
				findOneAndReplace.returnDocument(ReturnDocument.AFTER);
				Document inputDoc = input.getDoc();
				if (isUpsert && !inputDoc.containsKey(MongoDBConstants.ID_FIELD_NAME)) {
					doCreate(document, objectTypeId);
				} else {
					getIsUpsertForsingleDocument(isUpsert, coll, filterDocument, findOneAndReplace, inputDoc);
				}
			} else {
				BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
				bulkWriteOptions.ordered(false);
				List<WriteModel<Document>> replacingList = prepModifyWriteModelList(document, isUpsert, objectTypeId);
				BulkWriteResult bulkWriteResult = coll.bulkWrite(replacingList, bulkWriteOptions);
				if (isUpsert) {
					getIsUpsertValue(document, bulkWriteResult);
				}
			}
		}
	}

	private void getIsUpsertForsingleDocument(boolean isUpsert, MongoCollection<Document> coll, Bson filterDocument,
			FindOneAndReplaceOptions findOneAndReplace, Document inputDoc) {
		Document result = coll.findOneAndReplace(filterDocument, inputDoc, findOneAndReplace);
		if (isUpsert) {
			addFieldInOutputIfNotExists(inputDoc, MongoDBConstants.ID_FIELD_NAME,
					result.get(MongoDBConstants.ID_FIELD_NAME));
		}
		else if (result == null) {
			String message = "update executed result: " + result;
			logger.log(Level.INFO, message);
			throw new MongoException("No document found with " + filterDocument.toString());
		}
	}

	private void getIsUpsertValue(List<TrackedDataWrapper> document, BulkWriteResult bulkWriteResult) {
		List<BulkWriteUpsert> bulkWriteUpserts = bulkWriteResult.getUpserts();
		if (null != bulkWriteUpserts) {
			for (BulkWriteUpsert upsert : bulkWriteUpserts) {
				addFieldInOutputIfNotExists(document.get(upsert.getIndex()).getDoc(),
						MongoDBConstants.ID_FIELD_NAME, upsert.getId());
			}
		}
	}

	/**
	 * Update query response.
	 *
	 * @param resultCursor the result cursor
	 * @param requestData  the request data
	 * @param response     the response
	 * @param errorDetails the error details
	 */
	public void updateQueryResponse(MongoCursor<Document> resultCursor, FilterData requestData,
			OperationResponse response, ErrorDetails errorDetails,
			OperationContext operationContext){
		boolean noDocFound = null != resultCursor && !resultCursor.hasNext();
		int batchCount = 0;
		int docCount = 0;
		PropertyMap operationProperties = operationContext.getOperationProperties();
		Long batch = operationProperties.getLongProperty(MongoDBConstants.QUERY_BATCHSIZE,
				MongoDBConstants.DEFAULTBATCHSIZE);
		if(batch <= 0L) {
			batch = MongoDBConstants.DEFAULTBATCHSIZE;
		}
		if (noDocFound) {
			ResponseUtil.addEmptySuccess(response, requestData, MongoDBConstants.STATUS_CODE_SUCCESS);
		} else if (null != resultCursor) {
			while (resultCursor.hasNext()) {
				docCount++;
				Document doc = resultCursor.next();
				try (Payload payload = DocumentUtil.toPayLoad(doc)) {
					response.addPartialResult(requestData, OperationStatus.SUCCESS,
							MongoDBConstants.STATUS_CODE_SUCCESS, MongoDBConstants.STATUS_MESSAGE_SUCCESS, payload);
					if (docCount % batch == 0 || resultCursor.hasNext()) {
						String logMessage = new StringBuffer("Total number of documents parsed in ")
								.append("batch :").append(++batchCount).append(" are ").append(docCount).toString();
						response.getLogger().log(Level.INFO, logMessage);
						docCount = 0;
					}
				} catch (Exception e) {
					throw new ConnectorException(e);
				}		
			}
			response.finishPartialResult(requestData);
		} else {
			cursorIsNull(requestData, response, errorDetails);
		}
	}

	private void cursorIsNull(FilterData requestData, OperationResponse response, ErrorDetails errorDetails) {
		if (null != errorDetails) {
			try (Payload payload = MongoDBConnectorPayloadUtil
					.toPayload(new OutputDocument(OperationStatus.FAILURE, errorDetails))) {
				response.addResult(requestData, OperationStatus.FAILURE, ErrorUtils.fetchErrorCode(errorDetails),
						errorDetails.getErrorMessage(), payload);
			} catch (Exception e) {
				throw new ConnectorException(e);
			}
		}
	}


	/**
	 * Update operation response for Get operation and the sends the final result as
	 * output payload to the platform.
	 *
	 * @param request           the request
	 * @param operationResponse the operation response
	 * @param errorDetails      the error details
	 * @param doc               the doc
	 * @param objId             the obj id
	 * @param id                the id
	 * @param objectIdType      the objectIdType
	 */
	public void updateOperationResponseforGet(GetRequest request, OperationResponse operationResponse,
			ErrorDetails errorDetails, Document doc, String objId, ObjectIdData id, String objectIdType) {
		Logger responseLogger = operationResponse.getLogger();
		Payload payload = null;
		try {
			if (doc != null) {
				payload = DocumentUtil.toPayLoad(doc);
				ResponseUtil.addSuccess(operationResponse, id, STATUS_CODE_SUCCESS, payload);
			} else if (errorDetails != null) {
				payload = MongoDBConnectorPayloadUtil
						.toPayload(new OutputDocument(OperationStatus.FAILURE, errorDetails));
				operationResponse.addResult(request.getObjectId(), OperationStatus.APPLICATION_ERROR,
						ErrorUtils.fetchErrorCode(errorDetails), errorDetails.getErrorMessage(), payload);
			} else {
				String logMessage = new StringBuffer().append(((BrowseContext) getContext()).getOperationType())
						.append(" failed for objectId-").append(objId).append(" in collection-").toString();
				responseLogger.severe(logMessage);
				ResponseUtil.addEmptySuccess(operationResponse, id, STATUS_CODE_SUCCESS);
			}
		} finally {
			IOUtil.closeQuietly(payload);
		}
	}

	/**
	 * Process error details for the Get operation.
	 *
	 * @param ex the ex
	 * @return the error details
	 */
	public ErrorDetails processErrorForGet(Exception ex) {
		ErrorDetails errorDetails = null;
		Integer errorCode = null;
		if (ex != null) {
			if (ex instanceof MongoException) {
				errorCode = Integer.valueOf(((MongoException) ex).getCode());
			}
			errorDetails = new ErrorDetails(errorCode, ex.getMessage());
		}
		return errorDetails;
	}

	/**
	 * Performs a query operation on the MongoDB collecton based on the provided
	 * filters, projections and the sortkeys on the platform.It also checks for the
	 * size of the document, which should be less than equal to 1MB.
	 *
	 * @param collectionName the collection name
	 * @param bsonFilter     the bson filter
	 * @param bsonprojection the bsonprojection
	 * @param sortKeys       the sort keys
	 * @param batchSize      the batch size
	 * @return the mongo cursor
	 */
	public MongoCursor<Document> doQuery(String collectionName, Bson bsonFilter, Bson bsonprojection, Bson sortKeys,
			int batchSize) {
		MongoCursor<Document> resultCursor;
		FindIterable<Document> result;
		MongoCollection<Document> coll = getCollection(collectionName);
		Bson finalFilter = DocumentUtil.buildFilterWithMaxDocumentSize(getContext().getConfig(), bsonFilter);
		result = coll.find(finalFilter);
		if (null != bsonprojection) {
			result.projection(bsonprojection);
		}
		if (null != sortKeys) {
			result.sort(sortKeys);
		}
		resultCursor = result.batchSize(batchSize).iterator();
		return resultCursor;
	}

	/**
	 * Prepares the list of documents that has to be replaced by the current ones
	 * present in the given collection of a database.
	 *
	 * @param document the document
	 * @param isUpsert the is upsert
	 * @return the list
	 */
	private List<WriteModel<Document>> prepModifyWriteModelList(List<TrackedDataWrapper> document, boolean isUpsert,
			String objectTypeId) {
		List<WriteModel<Document>> replaceModelList = new ArrayList<>();
		List<TrackedDataWrapper> docListWithoutId = new ArrayList<>();
		ReplaceOptions replaceOptions = new ReplaceOptions();
		replaceOptions.upsert(isUpsert);
		for (TrackedDataWrapper replaceDocument : document) {
			Document replaceDoc = replaceDocument.getDoc();
			if (isUpsert && !replaceDoc.containsKey(MongoDBConstants.ID_FIELD_NAME)) {
				docListWithoutId.add(replaceDocument);
			} else {
				ReplaceOneModel<Document> replaceOneModel = new ReplaceOneModel<>(
						constructFilterById(replaceDocument, false), replaceDoc, replaceOptions);
				replaceModelList.add(replaceOneModel);
			}
		}
		if (!docListWithoutId.isEmpty()) {
			doCreate(docListWithoutId, objectTypeId);
		}
		return replaceModelList;
	}

	/**
	 * Returns the name of the Mongo database.
	 *
	 * @return the mongo DB
	 */
	public MongoDatabase getMongoDB() {
		if(null == _mongoDB){
			initialiseDatabase();
		}
		return _mongoDB;
	}

	/**
	 * Returns the collection name.
	 *
	 * @param objectTypeId the object type id
	 * @return the collection
	 */
	public MongoCollection<Document> getCollection(String objectTypeId) {
		MongoCollection<Document> coll = getCollection();
		if (null == coll) {
			coll = getMongoDB().getCollection(objectTypeId);
			setCollection(coll);
		}
		return coll;
	}

	/**
	 * Gets the database name.
	 *
	 * @return the database name
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * Gets the collection.
	 *
	 * @return the collection
	 */
	public MongoCollection<Document> getCollection() {
		return collection;
	}

	/**
	 * Gets the connection url.
	 *
	 * @return the connection url
	 */
	public String getConnectionUrl() {
		return connectionUrl;
	}

	/**
	 * Sets the connection url.
	 *
	 * @param connectionUrl the new connection url
	 */
	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	/**
	 * Sets the collection.
	 *
	 * @param collection the new collection
	 */
	public void setCollection(MongoCollection<Document> collection) {
		this.collection = collection;
	}

	/**
	 * List collections of DB.
	 *
	 * @return the mongo iterable
	 */
	public MongoIterable<String> listCollectionsOfDB() {
		MongoIterable<String> collections = null;
		collections = getMongoDB().listCollectionNames();
		return collections;
	}

}