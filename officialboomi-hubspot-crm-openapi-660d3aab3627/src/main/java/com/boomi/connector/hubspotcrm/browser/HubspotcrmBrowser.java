// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.browser;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.hubspotcrm.HubspotcrmConnection;
import com.boomi.connector.hubspotcrm.browser.profile.ProfileFactory;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HttpClientFactory;
import com.boomi.connector.hubspotcrm.util.ImportableFieldUtils;
import com.boomi.connector.openapi.OpenAPIBrowser;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Collection;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class extends the OpenAPIBrowser and implements the ConnectionTester interface.
 * It is responsible for browsing and testing the connection to the Hubspot CRM API.
 */
public class HubspotcrmBrowser extends OpenAPIBrowser implements ConnectionTester {

    private static final Logger LOG = LogUtil.getLogger(HubspotcrmBrowser.class);
    private final HubspotcrmOperationType _operationType;
    private final HubspotcrmObjectMetadataRetriever _metadataRetriever;
    private static final String PATH_VOBJECTS_FOR_TEST_CONNECTION = "/crm/v3/objects/contacts";
    private static final String SLASH = "/";
    private static final String RESULTS = "results";
    private static final String NAME_FIELD = "name";
    private static final String CONTACTS = "contacts";
    private static final String CONTACT_BROWSE_FIELDS_PATH = "fields/contactBrowseFields.json";
    private static final int INDEX = 0;
    private static final String PROPERTIES_NOT_FOUND = "No Properties Found";

    /**
     * Constructs a new {@code HubspotcrmBrowser} instance using the provided {@code HubspotcrmConnection}.
     *
     * @param connection the {@code HubspotcrmConnection} object to use for this browser instance.
     *                   This connection provides context and configuration needed for operations.
     * @throws IllegalArgumentException if the provided connection is {@code null} or if the context
     *                                  from the connection is not of the expected type.
     */
    public HubspotcrmBrowser(HubspotcrmConnection connection) {
        super(connection);
        BrowseContext browseContext = (BrowseContext) connection.getContext();
        _operationType = HubspotcrmOperationType.from(browseContext);
        _metadataRetriever = new HubspotcrmObjectMetadataRetriever(HttpClientFactory.createHttpClient(),
                getConnection());
    }

    /**
     * Returns the connection as a {@link HubspotcrmConnection}.
     *
     * @return the {@link HubspotcrmConnection} instance.
     */
    @Override
    public HubspotcrmConnection getConnection() {
        return (HubspotcrmConnection) super.getConnection();
    }

    /**
     * Tests the connection to the remote service.
     *
     * @throws ConnectorException if the connection test fails due to an unexpected status code or
     *                            an exception occurs during the HTTP request/response handling.
     * @see HttpClientFactory#createHttpClient()
     * @see #getConnection()
     */
    @Override
    public void testConnection() {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        try {
            httpClient = HttpClientFactory.createHttpClient();
            response = getConnection().testConnection(httpClient, PATH_VOBJECTS_FOR_TEST_CONNECTION);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ConnectorException(response.toString());
            }
        } catch (IOException ioException) {
            LOG.log(Level.SEVERE, "IOException occurred while test connection.", ioException);
            throw new ConnectorException(ioException.getMessage(), ioException);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Exception occurred while test connection.", e);
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(response, httpClient);
        }
    }

    /**
     * Retrieves object definitions based on the specified object type and roles.
     *
     * @param objectTypeId the ID of the object type for which definitions are to be retrieved.
     * @param roles        a collection of {@code ObjectDefinitionRole} objects specifying the roles associated
     *                     with the object definitions.
     * @return an {@code ObjectDefinitions} object containing the definitions for the specified object type
     * and roles.
     * @throws UnsupportedOperationException if the current operation type is not supported for retrieving
     *                                       object definitions.
     */
    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        try {
            ProfileFactory queryProfileFactory;
            switch (_operationType) {
                case CREATE:
                case UPDATE:
                    queryProfileFactory = ProfileFactory.getProfileFactory(_operationType, _metadataRetriever);
                    return getCustomObjectDefinitions(objectTypeId, queryProfileFactory, roles, _operationType);
                case DELETE:
                case GET:
                    return getObjectDefinitionsFromSpec(objectTypeId, roles, _operationType);

                case QUERY:
                    queryProfileFactory = ProfileFactory.getProfileFactory(_operationType, _metadataRetriever);
                    return getObjectDefinitionsQuery(objectTypeId, queryProfileFactory, roles, _operationType);

                default:
                    throw new UnsupportedOperationException("Unknown operation: " + _operationType);
            }
        } finally {
            IOUtil.closeQuietly(_metadataRetriever);
        }
    }

    /**
     * Retrieves custom object definitions based on the specified object type, profile factory, and roles.
     *
     * @param objectTypeId   the ID of the object type for which definitions are to be retrieved.
     * @param profileFactory the {@code ProfileFactory} used to obtain JSON profiles for the object type.
     * @param roles          a collection of {@code ObjectDefinitionRole} objects specifying the roles for which
     *                       object definitions are to be created.
     * @return an {@code ObjectDefinitions} object containing the custom definitions based on the provided
     * object type, roles, and profile factory.
     */
    private ObjectDefinitions getCustomObjectDefinitions(String objectTypeId, ProfileFactory profileFactory,
            Collection<ObjectDefinitionRole> roles, HubspotcrmOperationType operationType) {
        ObjectDefinitions objectDefinitions = new ObjectDefinitions();
        for (ObjectDefinitionRole role : roles) {
            if (ObjectDefinitionRole.INPUT == role) {
                ObjectDefinition objDefinition = new ObjectDefinition();
                objDefinition.setInputType(ContentType.JSON);
                String rootNode = ExecutionUtils.getRootNode(operationType, objectTypeId);
                objDefinition.setElementName(String.format("%s%s", SLASH, rootNode));
                String schema = profileFactory.getJsonProfile(objectTypeId, operationType, rootNode);
                objDefinition.setJsonSchema(schema);
                objectDefinitions.getDefinitions().add(objDefinition);
            } else if (ObjectDefinitionRole.OUTPUT == role) {
                ArrayList<ObjectDefinitionRole> objectDefinitionRole = new ArrayList<>();
                objectDefinitionRole.add(role);
                objectDefinitions.getDefinitions().add(super.getObjectDefinitions(objectTypeId, objectDefinitionRole)
                        .getDefinitions().get(0));
            }
        }

        if (operationType == HubspotcrmOperationType.UPDATE && objectTypeId.contains(CONTACTS)) {
            addImportableFields(objectDefinitions);
        }
        return objectDefinitions;
    }

    /**
     * Retrieves object definitions specifically for archive and retrieve operations.
     * This method extends the base object definitions by clearing operation fields
     * and adding importable fields for contact-related for Retrieve operations.
     *
     * @param objectTypeId  The identifier of the object type. This typically includes
     *                      the HTTP method and endpoint path (e.g., "GET/contacts")
     * @param roles         A collection of ObjectDefinitionRole that defines the roles
     *                      associated with the object definition
     * @param operationType The type of operation being performed, which influences
     *                      the returned definitions
     * @return ObjectDefinitions containing the modified definitions with cleared
     * operation fields and, for contact GET operations, additional
     * importable fields
     */
    private ObjectDefinitions getObjectDefinitionsFromSpec(String objectTypeId, Collection<ObjectDefinitionRole> roles,
            HubspotcrmOperationType operationType) {
        ObjectDefinitions def = super.getObjectDefinitions(objectTypeId, roles);
        def.getOperationFields().clear();
        if (operationType == HubspotcrmOperationType.GET && !def.getDefinitions().isEmpty()) {
            def.getDefinitions().get(INDEX).setCookie(fetchPropertiesForObjectType(objectTypeId));
            if (objectTypeId.contains(CONTACTS)) {
                addImportableFields(def);
            }
        }
        return def;
    }

    /**
     * Adds importable fields to the object definitions if applicable.
     */
    private void addImportableFields(ObjectDefinitions objectDefinitions) {
        ImportableFieldUtils.getImportableFields(objectDefinitions, CONTACT_BROWSE_FIELDS_PATH);
    }

    /**
     * Fetches and concatenates property names for a specific object type from HubSpot.
     *
     * @param objectTypeId The unique identifier of the object type for which to fetch properties
     * @return A comma-separated string containing all property names for the specified entity type
     */
    private String fetchPropertiesForObjectType(String objectTypeId) {
        JSONArray resultsArray = _metadataRetriever.getObjectMetadata(objectTypeId).optJSONArray(RESULTS);
        if (resultsArray == null || resultsArray.length() == 0) {
            throw new ConnectorException(PROPERTIES_NOT_FOUND);
        }
        return IntStream.range(INDEX, resultsArray.length()).mapToObj(
                i -> resultsArray.getJSONObject(i).optString(NAME_FIELD)).collect(
                Collectors.joining(ExecutionUtils.COMMA_SEPARATOR));
    }

    /**
     * Retrieves object definitions for the QUERY operation type.
     *
     * @param objectTypeId        the ID of the object type for which definitions are to be retrieved.
     * @param queryProfileFactory the {@code ProfileFactory} used to obtain JSON profiles for the object type.
     * @param roles               a collection of {@code ObjectDefinitionRole} objects specifying the roles for which
     *                            object definitions are to be created.
     * @return an {@code ObjectDefinitions} object containing the definitions for the specified object type
     * and roles.
     */
    private ObjectDefinitions getObjectDefinitionsQuery(String objectTypeId, ProfileFactory queryProfileFactory,
            Collection<ObjectDefinitionRole> roles, HubspotcrmOperationType operationType) {

        if (!roles.contains(ObjectDefinitionRole.OUTPUT)) {
            return new ObjectDefinitions();
        }
        ObjectDefinition definition = new ObjectDefinition().withOutputType(ContentType.JSON);
        String rootNode = ExecutionUtils.getRootNode(operationType, objectTypeId);
        definition.setElementName(String.format("%s%s", SLASH, rootNode));
        String schema = queryProfileFactory.getJsonProfile(objectTypeId, operationType, rootNode);
        definition.setJsonSchema(schema);

        ObjectDefinitions definitions = new ObjectDefinitions();
        definitions.getDefinitions().add(definition);

        return definitions;
    }
}
