// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import static com.boomi.connector.mongodb.constants.MongoDBConstants.DATASTRUCTURE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.DATATYPE;
import static com.boomi.connector.mongodb.constants.MongoDBConstants.OBJECTID;

import java.io.IOException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.bson.Document;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.mongodb.bean.OutputDocument;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.util.DocumentUtil;
import com.boomi.connector.mongodb.util.JsonSchemaUtil;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;


/**
 * The Class MongoDBConnectorBrowser.
 * 
 */
public class MongoDBConnectorBrowser extends BaseBrowser implements ConnectionTester {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(MongoDBConnectorBrowser.class.getName());

	/** The collection. */
	private MongoCollection<Document> collection;

	/** The jsonfactory. */
	private JsonFactory jsonfactory;

	/** The json parser. */
	private JsonParser jsonParser;

	/**
	 * Instantiates a new mongo DB connector browser.
	 *
	 * @param conn the conn
	 */
	protected MongoDBConnectorBrowser(MongoDBConnectorConnection conn) {
		super(conn);
	}

	/**
	 * Sets the input and the output type of the ObjectDefinition.
	 *
	 * @param objDefn    the obj defn
	 * @param inputType  the input type
	 * @param outputType the output type
	 */
	private void setInputOutType(ObjectDefinition objDefn, ContentType inputType, ContentType outputType) {
		objDefn.setInputType(inputType);
		objDefn.setOutputType(outputType);
	}

	/**
	 * Creates an ObjectDefinition based on the input provided.
	 *
	 * @param jsonSchema    the json schema
	 * @param operationType the operation type
	 * @return the object definition
	 */
	private ObjectDefinition initObjectDefinition(String jsonSchema, OperationType operationType) {
		ObjectDefinition objDefn = new ObjectDefinition();
		if (!StringUtil.isBlank(jsonSchema)) {
			objDefn.setElementName(StringUtil.EMPTY_STRING);
			objDefn.setJsonSchema(jsonSchema);
			if (OperationType.QUERY == operationType || OperationType.GET == operationType) {
				objDefn.setCookie(jsonSchema);
			}
			setInputOutType(objDefn, ContentType.JSON, ContentType.JSON);
		} else {
			setInputOutType(objDefn, ContentType.BINARY, ContentType.BINARY);
		}
		return objDefn;
	}

	/**
	 * Gets the object definition for the upsert operation.
	 *
	 * @param clazz         the clazz
	 * @param operationType the operation type
	 * @return the object definition for class
	 */
	private ObjectDefinition getObjectDefinitionForClass(@SuppressWarnings("rawtypes") Class clazz, OperationType operationType) {
		String jsonSchema = null;
		try {
			jsonSchema = DocumentUtil.getJsonSchema(clazz);
		} catch (JsonProcessingException e) {
			logger.severe(e.getClass().getSimpleName() + " while constructing jsonSchema for class-" + clazz.getName()
					+ ". Setting Profile as Unstructured");
		}
		return initObjectDefinition(jsonSchema, operationType);
	}

	/**
	 * Creates an ObjectDefinition with jsonSchema extracted from the MongoDB
	 * document for the given collection(ObjectTypeId) and document Id(objectId).
	 *
	 * @param objectId      the _id of the document whose schema should be set as
	 *                      the jsonSchema of the objectDefinition to be returned
	 * @param objectTypeId  the name of the collection from which the document and
	 *                      jsonSchema should be fetched
	 * @param operationType the operation type
	 * @return the objectDefinition If a document for given Id is found ,the schema
	 *         of the document is returned as the JSON profile in the
	 *         objectDefinition. In input is invalid or objectId is not provided,
	 *         the objectDefinition is set as Unstructured
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private ObjectDefinition getObjectDefinitionForObjectId(String objectTypeId, String objectId, String dataType,
			OperationType operationType){
		String jsonSchema = null;
		Document doc = null;
		try {
			if (StringUtils.isBlank(objectId) && OperationType.QUERY == operationType) {
				doc = getConnection().findLastUpdatedDoc(objectTypeId);
			} else {
				doc = MongoDBConnectorConnectionExt.findDocumentById(getConnection(), objectTypeId, objectId, null, dataType);
			}
			if (null == doc) {
				throw new ConnectorException(new StringBuffer("No record found for id: ").append(objectId)
						.append(" in collection:").append(objectTypeId).toString());
			} else {
				jsonSchema = JsonSchemaUtil.createJsonSchema(doc);
			}
		} catch (Exception e) {
			throw new ConnectorException(e.getMessage());
		}

		return initObjectDefinition(jsonSchema, operationType);
	}

	/**
	 * Creates an ObjectDefinition based on the input provided during profile
	 * import.
	 *
	 * @param isStructureData should be true if the profile is structured (Json
	 *                        Schema fetched from an existing document or outpayload
	 *                        bean
	 * @param objectTypeId    the name of the collection from which the document and
	 *                        jsonSchema should be fetched
	 * @param objectId        the _id of the document whose schema should be set as
	 *                        the jsonSchema of the objectDefinition to be returned
	 * @param operationType   the operation type
	 * @return the objectDefinition If a document for given Id is found ,the schema
	 *         of the document is returned as the JSON profile in the
	 *         objectDefinition. In input is invalid or objectId is not provided,
	 *         the objectDefinition is set as Unstructured
	 */
	private ObjectDefinition getObjectDefinition(boolean isStructureData, String objectTypeId,String objectId ,String dataType,
			OperationType operationType) {
		ObjectDefinition objDefn = null;
		if (isStructureData && (!StringUtil.isBlank(objectId) || OperationType.QUERY == operationType)) {
			try {
				objDefn = getObjectDefinitionForObjectId(objectTypeId, objectId, dataType, operationType);
			} catch (Exception e) {
				throw new ConnectorException(e);
			}
		} else {
			objDefn = initObjectDefinition(null, operationType);
		}
		String cookie = objDefn.getCookie();
		String cookieValue = dataType + MongoDBConstants.COOKIE + cookie;
		objDefn.setCookie(cookieValue);
		return objDefn;
	}

	/**
	 * Validate ObjectDefinition configuration based on the input provided.
	 *
	 * @param isStructuredData the is structured data
	 * @param objectId         the object id
	 * @param role             the role
	 * @param operationType    the operation type
	 */
	private void validateObjectDefConfig(boolean isStructuredData, String objectId, ObjectDefinitionRole role,
			OperationType operationType) {
		boolean validConfig = !(isStructuredData ^ StringUtil.isNotBlank(objectId));
		boolean checkConfig = false;
		if (role == ObjectDefinitionRole.INPUT) {
			switch (operationType) {
			case CREATE:
			case UPDATE:
			case UPSERT:
				checkConfig = true;
				break;
			default:
				break;
			}
		} else {
			switch (operationType) {
			case GET:
			case QUERY:
				checkConfig = true;
				break;
			default:
				break;
			}
		}
		if (checkConfig && !validConfig) {
			throw new ConnectorException(new StringBuffer("Invalid Profile config for ").append(role.name())
					.append(" Config provided isStructuredData=").append(isStructuredData).append(" and objectId=")
					.append(objectId).append(" for operation=").append(operationType).toString());
		}
	}

	/**
	 * Creates the object definitions for the provided objectId and the roles.
	 *
	 * @param objectTypeId the object type id
	 * @param roles        the roles
	 * @return the object definitions
	 */
	/*
	 * (non-Javadoc)
	 * 
	 
	 * 
	 * @see com.boomi.connector.api.Browser#getObjectDefinitions(java.lang.String,
	 * java.util.Collection)
	 */
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
		PropertyMap operationProperties = getContext().getOperationProperties();
		String objectId = operationProperties.getProperty(OBJECTID, null);	
		String dataType = operationProperties.getProperty(DATATYPE, null);
		boolean isStructured = operationProperties.getBooleanProperty(DATASTRUCTURE, false);
		ObjectDefinitions objectdefinitions = new ObjectDefinitions();
		OperationType operationType = getContext().getOperationType();
		try {
			for (ObjectDefinitionRole role : roles) {
				validateObjectDefConfig(isStructured, objectId, role, operationType);
				if (role == ObjectDefinitionRole.INPUT) {
					switch (operationType) {
					case GET:
					case EXECUTE:
					case DELETE:
					case QUERY:
						break;
					case CREATE:
					case UPDATE:
					case UPSERT:
						objectdefinitions.getDefinitions()
								.add((getObjectDefinition(isStructured, objectTypeId, objectId, dataType, operationType)));
						break;
					default:
						break;  
					}
				} else if (role == ObjectDefinitionRole.OUTPUT) {
					switch (operationType) {
					case GET:
					case QUERY:
						objectdefinitions.getDefinitions()
								.add(getObjectDefinition(isStructured || (OperationType.QUERY == operationType),
										objectTypeId, objectId, dataType, operationType));
						break;
					case CREATE:
					case DELETE:
					case UPDATE:
					case UPSERT:
						objectdefinitions.getDefinitions()
								.add(getObjectDefinitionForClass(OutputDocument.class, operationType));
						break;
					case EXECUTE:
						break;
					default:
						break;

					}
				}
			}
		} finally {
			getConnection().closeConnection();
		}
		return objectdefinitions;
	}

	/**
	 * Gets the object types.
	 *
	 * @return the object types
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.api.Browser#getObjectTypes()
	 */
	@Override
	public ObjectTypes getObjectTypes() {
		MongoIterable<String> typeNames = null;
		ObjectTypes types = new ObjectTypes();
		ConnectorException ex = null;
		try {
			typeNames = getConnection().listCollectionsOfDB();
			for (String typeName : typeNames) {
				types.getTypes().add(getObjectType(typeName, null));
			}
		} catch (MongoTimeoutException mTEx) {
			ex = new ConnectorException(new StringBuffer("Unable to connect to MongoDB with connectionString-")
					.append(getConnection().getConnectionUrl()).append(StringUtil.FAILURE_SEPARATOR)
					.append(mTEx.getMessage()).toString());
			throw ex;
		} finally {
			getConnection().closeConnection();
		}
		return types;
	}

	/**
	 * Returns an object with given id and label.
	 *
	 * @param id    the id
	 * @param label the label
	 * @return the object type
	 */
	private ObjectType getObjectType(String id, String label) {
		ObjectType objectType = new ObjectType();
		objectType.setId(id);
		if (!StringUtil.isBlank(label)) {
			objectType.setLabel(label);
		}
		return objectType;
	}

	/**
	 * Gets the collection.
	 *
	 * @return the collection
	 */
	MongoCollection<Document> getCollection() {
		return collection;
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.util.BaseBrowser#getConnection()
	 */
	@Override
	public MongoDBConnectorConnection getConnection() {
		return (MongoDBConnectorConnection) super.getConnection();
	}

	/**
	 * Test connection.
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.api.ConnectionTester#testConnection()
	 */
	@Override
	public void testConnection() {
		try {
			getConnection().listCollectionsOfDB().first();
		} catch (MongoTimeoutException | MongoSecurityException mEx) {
			logger.log(Level.SEVERE, mEx.getMessage(), mEx);
			StringBuilder errMsg = new StringBuilder("Unable to connect to MongoDB with connectionString-")
					.append(getConnection().getConnectionUrl()).append(StringUtil.FAILURE_SEPARATOR)
					.append(mEx.getMessage());
			throw new ConnectorException(errMsg.toString());
		} finally {
			if (null != getConnection()) {
				getConnection().closeConnection();
			}
		}
	}

	/**
	 * Parses the input json string.
	 *
	 * @param json the json
	 * @return the json parser
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public JsonParser getJsonParser(String json) throws IOException {
		if (null == jsonParser) {
			jsonParser = getJsonfactory().createParser(json);
		}
		return jsonParser;
	}

	/**
	 * Gets the jsonfactory.
	 *
	 * @return the jsonfactory
	 */
	public JsonFactory getJsonfactory() {
		if (null == jsonfactory) {
			jsonfactory = new JsonFactory();
		}
		return jsonfactory;
	}

}