//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.connector.workdayprism.responses.DescribeTableResponse;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.connector.workdayprism.utils.CreateBucketHelper;
import com.boomi.connector.workdayprism.utils.TableListHelper;
import com.boomi.connector.workdayprism.utils.DescribeSchemaBuilder;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.boomi.util.StringUtil.EMPTY_STRING;

/**
 * Specific implementation of BaseBrowser for the Workday Prism Connector version 2.0.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class PrismBrowser extends BaseBrowser implements ConnectionTester { 
    private static final Logger LOG = LogUtil.getLogger(PrismBrowser.class);

    private static final String ERROR_COULD_NOT_DESCRIBE = "the dataset couldn't be described";
    private static final String OP_PROP_ENTITY_FILTER = "entity_filter";
    private static final String OP_PROP_USE_EXISTING_SCHEMA = "use_existing_schema";
    private static final String RESOURCE_SCHEMA_PATTERN = "schema/%s_%s_%s.json";
    private static final String BUCKET_PREFIX = "BUCKET for: ";
    private static final int TEST_OFFSET = 0;
    private static final int TEST_LIMIT = 1;

    PrismBrowser(PrismConnection connection) {
        super(connection);
    }

    @Override
    public PrismConnection getConnection() {
        return (PrismConnection) super.getConnection();
    }
     
    /**
     * Return the list of object types available for an established connection
     *
     * @return an instance of ObjectTypes 
     */
    @Override
    public ObjectTypes getObjectTypes() {
        ObjectTypes objectTypes = new ObjectTypes();

        switch (getContext().getOperationType()) { 
        case GET:
        case EXECUTE:
        	if (Constants.IMPORT_CUSTOM_TYPE_ID.equals(getContext().getCustomOperationType())) {
        		objectTypes.withTypes(getCreateObjectTypes());
        		break;
    		} else {
    	    objectTypes.withTypes(Constants.OBJECT_TYPE_BUCKET);
            break;
    		}
        case CREATE:
            objectTypes.withTypes(getCreateObjectTypes());
            break;
        default:
            throw new UnsupportedOperationException();
        }

        return objectTypes;
    }

    private List<ObjectType> getCreateObjectTypes() {
        if (OperationType.CREATE != getContext().getOperationType()
        		&& OperationType.EXECUTE != getContext().getOperationType()) { 
            throw new UnsupportedOperationException();
        }
        List<ObjectType> types = new ArrayList<>();
        boolean useExisting = getContext().getOperationProperties().getBooleanProperty(OP_PROP_USE_EXISTING_SCHEMA,
                false);
        switch (getEntity()) {
        case Constants.ENTITY_BUCKET:
        	if(!useExisting)
            types.add(Constants.OBJECT_TYPE_DYNAMIC_DATASET);
        	else
            types.addAll(fetchTables(BUCKET_PREFIX));
            break;
        case Constants.ENTITY_DATASET:
           types.add(Constants.OBJECT_TYPE_DATASET);
            break;
        default:
            throw new UnsupportedOperationException();
        }
        return types;
    }

    private String getEntity() {
        return getContext().getOperationProperties().getProperty(Constants.PROPERTY_ENTITY_TYPE);
    }

    private SortedSet<ObjectType> fetchTables(String labelPrefix) {
        TableListHelper helper = new TableListHelper(getConnection(), labelPrefix);
        String filter = getContext().getOperationProperties().getProperty(OP_PROP_ENTITY_FILTER, EMPTY_STRING);
        return helper.getObjectTypes(filter);
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        ObjectDefinitions definitions = new ObjectDefinitions();
        for (ObjectDefinitionRole role : roles) {
            definitions.withDefinitions(getSchemaResource(role, objectTypeId));
        }

        return definitions;
    }

    private ObjectDefinition getSchemaResource(ObjectDefinitionRole role, String objectTypeId) {
    	
    	
    	String cookie = isStaticBucketCreate(objectTypeId, role) ? getFieldsCookie(objectTypeId) : null;	
    	String entity = StringUtil.defaultIfBlank(getEntity(), objectTypeId);
        String resourceName = StringUtil.isBlank(cookie) ? entity :  Constants.ENTITY_STATIC_BUCKET;
        String operation = getContext().getOperationType().name().toLowerCase();
        String objectFileName = String.format(RESOURCE_SCHEMA_PATTERN, operation, resourceName,
                role.name().toLowerCase());
        
        try {
            return JSONUtil.newJsonDefinitionFromResource(role, objectFileName).withCookie(cookie);
        }
        catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * Determines if a CREATE operation is being configured to add a new Bucket into a specific Dataset.
     * This is important because in those scenarios it's possible to retrieve the data schema fields from the Dataset
     * and use it to create the bucket instead of forcing the user to include it in the input document.
     *
     * The conditions for this case are:
     * 1- The {@link ObjectDefinitionRole} must be input (we're deciding the approach for the input document profile
     * 2- The {@link OperationType} is CREATE
     * 3- The objectTypeId is neither "bucket" nor "dynamic_dataset"
     *
     */
    private boolean isStaticBucketCreate(String objectTypeId, ObjectDefinitionRole role) {
        boolean useExisting = getContext().getOperationProperties().getBooleanProperty(OP_PROP_USE_EXISTING_SCHEMA,
                false);
        return useExisting && ObjectDefinitionRole.INPUT == role
                && (OperationType.CREATE == getContext().getOperationType() || OperationType.EXECUTE == getContext().getOperationType())
                && !Constants.ENTITY_DATASET.equals(objectTypeId) && !Constants.ID_DYNAMIC_DATASET.equals(
                objectTypeId);
    }

    /**
     * This method will fetch a Table Describe response from Workday Prism and try to extract the schema fields.
     * Not all the Tables will have a defined schema because they inherit it from the last Bucket used to upload
     * data or an issue might arise when the response is processed but it's fine to return null because that will
     * that a new schema must be generated and the Input profile will be generated accordingly.
     *
     */
    
    private String getFieldsCookie(String objectTypeId) {
        try {
            DescribeTableResponse response = getConnection().describeTable(objectTypeId);
            JsonNode schema = DescribeSchemaBuilder.getSchema(response);
            return CreateBucketHelper.extractFieldArrayString(schema);            
        }
        catch (IOException | ConnectorException e) {
            // We don’t mind if the dataset couldn't be described at this point because the complete profile will be
            // generated in that case and the user will be able to specify the schema files through the input document.
            LOG.log(Level.WARNING, ERROR_COULD_NOT_DESCRIBE, e);
        }
        return null;
    }

	

    @Override
    public void testConnection() {
        try {
        	getConnection().getTables(TEST_OFFSET, TEST_LIMIT);  
        }
        catch (Exception e) {
            throw new ConnectorException(e);
        }
    }
}
