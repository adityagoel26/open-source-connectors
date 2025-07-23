//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

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
import com.boomi.connector.cosmosdb.bean.DeleteOperationRequest;
import com.boomi.connector.cosmosdb.bean.OutputDocument;
import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.*;
import com.boomi.connector.cosmosdb.util.DocumentUtil;
import com.boomi.connector.cosmosdb.util.JsonSchemaBuilder;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBBrowser extends BaseBrowser implements ConnectionTester {

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(CosmosDBBrowser.class.getName());

	protected CosmosDBBrowser(CosmosDBConnection conn) {
		super(conn);
	}

	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {

		PropertyMap operationProperties = getContext().getOperationProperties();
		String objectId = operationProperties.getProperty(OBJECTID, null);
		String customActionTYpe = this.getContext().getCustomOperationType();
		String partitionKeyValue = operationProperties.getProperty(PARTITION_KEY_VALUE, null);
		boolean isStructured = operationProperties.getBooleanProperty(DATASTRUCTURE, false);
		ObjectDefinitions objectdefinitions = new ObjectDefinitions();
		OperationType operationType = getContext().getOperationType();
		try {
			for (ObjectDefinitionRole role : roles) {
				validateObjectDefConfig(isStructured, objectId, role, operationType);
				if (role == ObjectDefinitionRole.INPUT) {
					switch (operationType) {
					case EXECUTE:
						if (customActionTYpe.equals(HTTP_DELETE)) {
							objectdefinitions.getDefinitions()
									.add(getObjectDefinitionForClass(DeleteOperationRequest.class));
						} else {
							objectdefinitions.getDefinitions()
									.add(initObjectDefinition(JsonSchemaBuilder.getInputJsonSchema(HTTP_GET),operationType));
						}
						break;
					case CREATE:
						
					case UPSERT:

					case UPDATE:
						objectdefinitions.getDefinitions()
								.add(getObjectDefinition(isStructured, objectTypeId, objectId, partitionKeyValue,operationType));
						break;
					default:
						throw new UnsupportedOperationException();
					}
				} else if (role == ObjectDefinitionRole.OUTPUT) {
					switch (operationType) {
					case EXECUTE:
					case QUERY:
						objectdefinitions.getDefinitions()
								.add(getObjectDefinition(isStructured, objectTypeId, objectId, partitionKeyValue,operationType));
						break;
						
					case CREATE:
						
					case UPSERT:
						
					case UPDATE:
						objectdefinitions.getDefinitions().add(getObjectDefinitionForClass(OutputDocument.class));
						break;
					case DELETE:
						break;
					default:
						throw new UnsupportedOperationException();
					}
				}
			}
		} catch (CosmosDBConnectorException exception) {
			throw new ConnectorException(exception.getMessage());
		}
		return objectdefinitions;

	}

	/**
	 * Creates an ObjectDefinition based on the input provided during profile
	 * import.
	 *
	 * @param isStructureData   should be true if the profile is structured (Json
	 *                          Schema fetched from an existing document or
	 *                          outpayload bean {@link BulkOperationStatus} for Bulk
	 *                          operations : CREATE,UPDATE,UPSERT,DELETE
	 * @param objectTypeId      the name of the collection from which the document
	 *                          and jsonSchema should be fetched
	 * @param objectId          the _id of the document whose schema should be set
	 *                          as the jsonSchema of the objectDefinition to be
	 *                          returned
	 * @param partitionKeyValue partition key data/value used for retrieving or
	 *                          updat any record or data.
	 * @return the objectDefinition If a document for given Id is found ,the schema
	 *         of the document is returned as the JSON profile in the
	 *         objectDefinition. In input is invalid or objectId is not provided,
	 *         the objectDefinition is set as Unstructured
	 */
	private ObjectDefinition getObjectDefinition(boolean isStructureData, String objectTypeId, String objectId,
			String partitionKeyValue, OperationType operationType) {
		ObjectDefinition objDefn = null;
		if (isStructureData && (!StringUtil.isBlank(objectId))) {
			objDefn = getObjectDefinitionForObjectId(objectId, objectTypeId, partitionKeyValue, operationType);
		} else {
			if(operationType.equals(OperationType.QUERY)) {
				objDefn = getObjectDefinitionForObjectId(null, objectTypeId, partitionKeyValue, operationType);
			} else {
			objDefn = initObjectDefinition(null,null);
			}
		}
		return objDefn;
	}

	/**
	 * Validate object def config.
	 *
	 * @param isStructuredData the is structured data
	 * @param objectId         the object id
	 * @param role             the role
	 * @param operationType    the operation type
	 */
	private void validateObjectDefConfig(boolean isStructuredData, String objectId, ObjectDefinitionRole role,
			OperationType operationType) {
		boolean validConfig = !(isStructuredData ^ StringUtil.isNotBlank(objectId));

		if (!validConfig) {
			throw new ConnectorException(new StringBuilder(PROFILE_ERROR_MSG_ONE).append(role.name())
					.append(PROFILE_ERROR_MSG_TWO).append(isStructuredData)
					.append(PROFILE_ERROR_MSG_THREE).append(objectId)
					.append(PROFILE_ERROR_MSG_FOUR).append(operationType).toString());
		}
	}

	/**
	 * Creates an ObjectDefinition with jsonSchema extracted from the CosmosDB
	 * document for the given collection(ObjectTypeId) and document Id(objectId).
	 *
	 * @param objectId     the _id of the document whose schema should be set as the
	 *                     jsonSchema of the objectDefinition to be returned
	 * @param objectTypeId the name of the collection from which the document and
	 *                     jsonSchema should be fetched
	 * @return the objectDefinition If a document for given Id is found ,the schema
	 *         of the document is returned as the JSON profile in the
	 *         objectDefinition. In input is invalid or objectId is not provided,
	 *         the objectDefinition is set as Unstructured
	 */
	private ObjectDefinition getObjectDefinitionForObjectId(String objectId, String objectTypeId,
			String partitionKeyValue, OperationType operationType) {
		String jsonSchema = null;
		try {
			jsonSchema = getConnection().findDocumentById(objectTypeId, objectId, partitionKeyValue);
		} catch (Exception exception) {
			throw new ConnectorException(exception.getMessage());
		}
		return initObjectDefinition(jsonSchema,operationType);
	}

	/**
	 * Inits the object definition.
	 *
	 * @param jsonSchema the json schema
	 * @return the object definition
	 */
	private ObjectDefinition initObjectDefinition(String jsonSchema, OperationType operationType) {
		ObjectDefinition objDefn = new ObjectDefinition();
		if (!StringUtil.isBlank(jsonSchema)) {
			objDefn.setElementName(StringUtil.EMPTY_STRING);
			objDefn.setJsonSchema(jsonSchema);
			if(operationType != null && operationType.equals(OperationType.QUERY)) {
				objDefn.setCookie(jsonSchema);
			}
			setInputOutType(objDefn, ContentType.JSON, ContentType.JSON);
		} else {
			setInputOutType(objDefn, ContentType.BINARY, ContentType.BINARY);
		}
		return objDefn;
	}

	@Override
	public ObjectTypes getObjectTypes() {
		ObjectTypes types = new ObjectTypes();
		List<String> typeNames = getConnection().listCollectionsOfDB();
		for (String typeName : typeNames) {
			types.getTypes().add(getObjectType(typeName, null));
		}
		return types;
	}

	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.api.ConnectionTester#testConnection()
	 */
	@Override
	public void testConnection() {

		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build();) {
			callCosmosConnection(clientConnection);
		} catch (Exception ex) {
			throw new ConnectorException(ex.getMessage());
		}
	}

	/*
	 * This method is used for testing the Cosmos DB connection.
	 * 
	 */
	private void callCosmosConnection(CloseableHttpClient clientConnection) throws CosmosDBConnectorException {
		String databaseName = getConnection().getDatabaseName();
		StringBuilder errMsg = null;
		Exception ex = null;
		try (CloseableHttpResponse response = clientConnection.execute(getConnection().buildUriRequest(
				DB + databaseName + COLL, HTTP_GET));) {
			if (response.getStatusLine().getStatusCode() == 200) {
				logger.info(CONNECTION_SUCCESS_MSG);
			} else {
				errMsg = new StringBuilder(CONNECTION_ERROR_MSG)
						.append(getConnection().getConnectionUrl())
						.append(FOR_DATABASE + databaseName).append(StringUtil.FAILURE_SEPARATOR)
						.append(response.getStatusLine().getReasonPhrase());
				throw new CosmosDBConnectorException(errMsg.toString());
			}
		} catch (Exception exception) {
			if (errMsg == null) {
				ex = exception;
				errMsg = new StringBuilder(CONNECTION_ERROR_MSG)
						.append(getConnection().getConnectionUrl())
						.append(FOR_DATABASE + databaseName).append(StringUtil.FAILURE_SEPARATOR)
						.append(ex.getMessage());
			} else {
				throw new CosmosDBConnectorException(exception.getMessage());
			}
			throw new CosmosDBConnectorException(errMsg.toString());
		} finally {
			if (null != ex) {
				logger.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}
	}

	/**
	 * Gets the object definition for class.
	 *
	 * @param clazz the clazz
	 * @return the object definition for class
	 */
	@SuppressWarnings("rawtypes")
	private ObjectDefinition getObjectDefinitionForClass(Class definitionClass) {
		String jsonSchema = null;
		try {
			jsonSchema = DocumentUtil.getJsonSchema(definitionClass);
		} catch (JsonProcessingException e) {
			logger.severe(e.getClass().getSimpleName() + " while constructing jsonSchema for class-"
					+ definitionClass.getName() + ". Setting Profile as Unstructured");
		}
		return initObjectDefinition(jsonSchema, null);
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
	 * Sets the input out type.
	 *
	 * @param objDefn    the obj defn
	 * @param inputType  the input type
	 * @param outputType the output type
	 */
	private void setInputOutType(ObjectDefinition objDefn, ContentType inputType, ContentType outputType) {
		objDefn.setInputType(inputType);
		objDefn.setOutputType(outputType);
	}

}