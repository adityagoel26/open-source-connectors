// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.openapi.OpenAPIBrowser;
import com.boomi.connector.veeva.VeevaConnection;
import com.boomi.connector.veeva.browser.profile.FileProfileLoader;
import com.boomi.connector.veeva.browser.profile.ProfileFactory;
import com.boomi.connector.veeva.operation.query.VeevaQueryOperation;
import com.boomi.connector.veeva.util.HttpClientFactory;
import com.boomi.connector.veeva.util.OpenAPIAction;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.json.JSONUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class VeevaBrowser extends OpenAPIBrowser implements ConnectionTester {

    public static final String QUERY_PARAMETER_FIELD_ID_PREFIX = "QUERYPARAM_";
    public static final String SUBQUERY_SUFFIX = "__Subquery";

    private static final Logger LOG = LogUtil.getLogger(VeevaBrowser.class);
    private static final String DOCUMENTS = "documents";
    private static final String PATH_VOBJECTS_FOR_TEST_CONNECTION = "/metadata/vobjects";
    private static final String KEY_DOCUMENTS = "documents";
    private static final String KEY_RELATIONSHIPS = "relationships";
    private static final String KEY_LABEL = "label";
    private static final String BATCH_DOCUMENTS_PATH = "/objects/documents/batch";
    private static final String CREATE_ITEMS_PATH = "/services/file_staging/items";
    private static final String JOIN_DEPTH_BROWSE_PROPERTY = "JOIN_DEPTH";
    private final VeevaOperationType _operationType;
    private final VeevaObjectMetadataRetriever _metadataRetriever;

    public VeevaBrowser(VeevaConnection connection) {
        super(connection);
        BrowseContext browseContext = (BrowseContext) connection.getContext();
        _operationType = VeevaOperationType.from(browseContext);
        _metadataRetriever = new VeevaObjectMetadataRetriever(HttpClientFactory.createHttpClient(browseContext),
                getConnection());
    }

    @Override
    public ObjectTypes getObjectTypes() {
        ObjectTypes objectTypes = new ObjectTypes();
        try {
            switch (_operationType) {
                case QUERY:
                    // Add Query specific Types
                    addDocumentObjectType(objectTypes);
                    addDocumentRelationshipType(objectTypes);
                    addGetItemsAtPathType(objectTypes);
                    // Add Veeva Objects Types
                    addVObjectObjectTypes(objectTypes, _metadataRetriever.getAllObjects());
                    break;
                case UPDATE:
                case CREATE:
                    // Add Veeva Objects Types
                    addVObjectObjectTypes(objectTypes, _metadataRetriever.getAllObjects());
                    break;
                case EXECUTE:
                    // Use OpenAPI specification Types
                    objectTypes = super.getObjectTypes();
                    break;
                default:
                    throw new IllegalArgumentException("unexpected operation type: " + _operationType);
            }
            return objectTypes;
        } finally {
            IOUtil.closeQuietly(_metadataRetriever);
        }
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        try {
            switch (_operationType) {
                case QUERY:
                    return getQueryDefinitions(objectTypeId);
                case UPDATE:
                case CREATE:
                    ProfileFactory createUpdateProfileFactory = ProfileFactory.getCreateUpdateFactory(_operationType,
                            getContext(), _metadataRetriever);
                    return getCustomObjectDefinitions(objectTypeId, createUpdateProfileFactory, roles);
                case EXECUTE:
                    OpenAPIAction action = new OpenAPIAction(objectTypeId);
                    switch (action.getPath()) {
                        case BATCH_DOCUMENTS_PATH:
                            ProfileFactory batchDocumentsProfileFactory = ProfileFactory.getBatchDocumentsFactory(
                                    action.getMethod(), _metadataRetriever);
                            return getCustomObjectDefinitions(DOCUMENTS, batchDocumentsProfileFactory, roles);
                        case CREATE_ITEMS_PATH:
                            ObjectDefinitions objectDefinitions = super.getObjectDefinitions(objectTypeId, roles);
                            objectDefinitions.getDefinitions().get(0).setInputType(ContentType.BINARY);
                            return objectDefinitions;
                        default:
                            return super.getObjectDefinitions(objectTypeId, roles);
                    }
                default:
                    throw new UnsupportedOperationException("unknown operation: " + _operationType);
            }
        } finally {
            IOUtil.closeQuietly(_metadataRetriever);
        }
    }

    private static boolean isDocumentObjectType(String objectType) {
        return DOCUMENTS.equals(objectType);
    }

    private ObjectDefinitions getQueryDefinitions(String objectTypeId) {
        ObjectDefinition definition = new ObjectDefinition().withOutputType(ContentType.JSON);
        ObjectDefinitions definitions = new ObjectDefinitions();
        definitions.getDefinitions().add(definition);

        QueryImportableFieldsHelper.addImportableFields(definitions, objectTypeId);

        long maxJoinDepth = this.getContext().getOperationProperties().getLongProperty(JOIN_DEPTH_BROWSE_PROPERTY, 0L);

        // get the empty list reference and pass it to the CustomProfileFactory to be filled with the selectable fields
        // while building the profile
        List<FieldSpecField> fieldSpecFields = definition.getFieldSpecFields();

        ProfileFactory profileFactory = ProfileFactory.getQueryFactory(fieldSpecFields, maxJoinDepth, getContext(),
                _metadataRetriever);

        if (VeevaQueryOperation.GET_ITEMS_AT_PATH_LIST_OBJECT_TYPE.equals(objectTypeId)) {
            definition.setElementName(JSONUtil.FULL_DOCUMENT_JSON_POINTER);
        } else {
            definition.setElementName("/" + objectTypeId);
        }

        String schema = profileFactory.getJsonProfile(objectTypeId);
        definition.setJsonSchema(schema);
        return definitions;
    }

    private static ObjectDefinitions getCustomObjectDefinitions(String objectTypeId, ProfileFactory profileFactory,
            Collection<ObjectDefinitionRole> roles) {
        ObjectDefinitions objectDefinitions = new ObjectDefinitions();
        for (ObjectDefinitionRole role : roles) {
            ObjectDefinition objDefinition = new ObjectDefinition();
            if (ObjectDefinitionRole.INPUT == role) {
                objDefinition.setInputType(ContentType.JSON);
                objDefinition.setElementName("/" + objectTypeId);
                String schema = profileFactory.getJsonProfile(objectTypeId);
                objDefinition.setJsonSchema(schema);
                objectDefinitions.getDefinitions().add(objDefinition);
            } else if (ObjectDefinitionRole.OUTPUT == role) {
                objDefinition.setOutputType(ContentType.JSON);

                objDefinition.setElementName("/editResponse");
                objDefinition.setJsonSchema(
                        FileProfileLoader.getStaticOutputResponseProfileSchema(isDocumentObjectType(objectTypeId)));

                objectDefinitions.getDefinitions().add(objDefinition);
            }
        }
        return objectDefinitions;
    }

    @Override
    public VeevaConnection getConnection() {
        return (VeevaConnection) super.getConnection();
    }

    /**
     * Test the connection by requesting a list for objects to Veeva. If it fails, it throws an exception that will be
     * displayed on the test connection wizard.
     */
    @Override
    public void testConnection() {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;
        try {
            httpClient = HttpClientFactory.createHttpClient(getContext());
            response = getConnection().testConnection(httpClient, PATH_VOBJECTS_FOR_TEST_CONNECTION);

            JSONObject jsonResponse = new JSONObject(new JSONTokener(response.getEntity().getContent()));
            if (!"SUCCESS".contentEquals(jsonResponse.getString("responseStatus"))) {
                throw new ConnectorException(jsonResponse.toString());
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Test connection failed. ", e);
            throw new ConnectorException(e.getMessage());
        } finally {
            IOUtil.closeQuietly(response, httpClient);
        }
    }

    static void addDocumentObjectType(ObjectTypes objectTypes) {
        ObjectType objectType = new ObjectType();
        objectType.setId(KEY_DOCUMENTS);
        objectType.setLabel("Document");
        objectTypes.getTypes().add(objectType);
    }

    static void addDocumentRelationshipType(ObjectTypes objectTypes) {
        ObjectType objectType = new ObjectType();
        objectType.setId(KEY_RELATIONSHIPS);
        objectType.setLabel("Document Relationships");
        objectTypes.getTypes().add(objectType);
    }

    static void addGetItemsAtPathType(ObjectTypes objectTypes) {
        ObjectType objectType = new ObjectType();
        objectType.setId(VeevaQueryOperation.GET_ITEMS_AT_PATH_LIST_OBJECT_TYPE);
        objectType.setLabel("Get items at path list");
        objectTypes.getTypes().add(objectType);
    }

    static void addVObjectObjectTypes(ObjectTypes objectTypes, JSONObject vObjectList) {
        List<ObjectType> vObjectTypes = new ArrayList<>();
        JSONArray objects = vObjectList.getJSONArray("objects");

        IntStream.range(0, objects.length()).mapToObj(objects::getJSONObject).forEach(object -> {
            ObjectType objectType = new ObjectType();
            objectType.setId(object.getString("name"));
            objectType.setLabel(object.getString(KEY_LABEL));
            vObjectTypes.add(objectType);
        });

        // Add Veeva Objects sorted by id
        vObjectTypes.sort(Comparator.comparing(ObjectType::getId));
        objectTypes.withTypes(vObjectTypes);
    }
}