// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import io.swagger.v3.oas.models.PathItem;
import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.connector.veeva.browser.VeevaOperationType;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Factory class to create JSON Profiles for Veeva Operations
 */
public abstract class ProfileFactory {

    private static final String STRING = "string";
    private static final String NUMBER = "number";
    private static final String BOOLEAN = "boolean";
    private static final String ARRAY = "array";
    private static final String ITEMS = "items";
    private static final String OBJECT = "object";
    private static final String PROPERTIES = "properties";
    private static final String DOCUMENTS = "documents";
    private static final String RELATIONSHIPS = "relationships";
    private static final String MAXLENGTH = "maxLength";
    private static final String MAX_LENGTH = "max_length";
    private static final String KEY_HELPCONTENT = "helpContent";
    private static final String KEY_HELP_CONTENT = "help_content";
    private static final String KEY_LABEL = "label";
    private static final String CHILD = "child";
    private static final String RELATIONSHIP_TYPE = "relationship_type";
    private static final String REFERENCE_INBOUND = "reference_inbound";

    protected static final String ID = "id";
    private static final String BYTE = "byte";
    private static final String INT_16 = "int16";
    private static final String INT_32 = "int32";
    private static final String INT_64 = "int64";
    private static final String INTEGER = "integer";
    private static final String SINGLE = "single";
    private static final String DECIMAL = "decimal";
    private static final String DOUBLE = "double";
    private static final String AUTONUMBER = "autonumber";
    private static final String DATETIME = "datetime";
    private static final String DATE_TIME = "date-time";
    private static final String DATE = "date";
    private static final String TIME = "time";
    private static final String OBJECTREFERENCE = "objectreference";
    private static final String URL = "url";
    private static final String LOOKUP = "lookup";
    private static final String PICKLIST = "picklist";
    private static final String EXACTMATCHSTRING = "exactmatchstring";
    private static final String TYPE = "type";
    private static final String FORMAT = "format";
    private static final String DESCRIPTION = "description";
    private static final String DESCRIPTION_SEPARATOR = " - ";
    private static final String SUBQUERY = "subquery";
    private static final String NAME = "name";
    private static final String FIELDS = "fields";
    private static final String SCHEMA = "$schema";
    private static final String SCHEMA_HEADER = "http://json-schema.org/schema#";

    protected final VeevaOperationType _operationType;
    protected final VeevaObjectMetadataRetriever _metadataRetriever;

    /**
     * Get a profile factory instance for Batch Documents operations
     *
     * @param method            the POST or PUT http method
     * @param metadataRetriever the metadataRetriever to obtain the entity information for the operation profile
     * @return an instance of {@link ProfileFactory}
     */
    public static ProfileFactory getBatchDocumentsFactory(PathItem.HttpMethod method,
            VeevaObjectMetadataRetriever metadataRetriever) {
        switch (method) {
            case POST:
                return new BatchDocumentsProfileFactory(VeevaOperationType.CREATE, metadataRetriever);
            case PUT:
                return new BatchDocumentsProfileFactory(VeevaOperationType.UPDATE, metadataRetriever);
            default:
                throw new IllegalArgumentException("unexpected method: " + method);
        }
    }

    /**
     * Get a profile factory instance for Query operation
     *
     * @param queryFields       the list to populate the query fields
     * @param maxDepth          a number indicating the depth of the profile
     * @param context           the context with the browse configuration
     * @param metadataRetriever the metadataRetriever to obtain the entity information for the operation profile
     * @return an instance of {@link ProfileFactory}
     */
    public static ProfileFactory getQueryFactory(List<FieldSpecField> queryFields, long maxDepth, BrowseContext context,
            VeevaObjectMetadataRetriever metadataRetriever) {
        return new QueryProfileFactory(queryFields, maxDepth, context, metadataRetriever);
    }

    /**
     * Get a profile factory instance for Create and Update operations
     *
     * @param operationType     the operation type indicating if it is a Create or Update operation
     * @param context           the context with the browse configuration
     * @param metadataRetriever the metadataRetriever to obtain the entity information for the operation profile
     * @return an instance of {@link ProfileFactory}
     */
    public static ProfileFactory getCreateUpdateFactory(VeevaOperationType operationType, BrowseContext context,
            VeevaObjectMetadataRetriever metadataRetriever) {
        return new CreateUpdateProfileFactory(operationType, context, metadataRetriever);
    }

    ProfileFactory(VeevaOperationType operationType, VeevaObjectMetadataRetriever metadataRetriever) {
        _operationType = operationType;
        _metadataRetriever = metadataRetriever;
    }

    /**
     * Build the profile for the given object type ID
     *
     * @param objectTypeId representing the Veeva Object name
     * @return a string representing the JSON profile schema
     */
    public String getJsonProfile(String objectTypeId) {
        JSONObject profile = new JSONObject();
        profile.put(SCHEMA, SCHEMA_HEADER);

        JSONObject root = new JSONObject();
        root.put(TYPE, ARRAY);
        profile.put(objectTypeId, root);

        JSONObject items = new JSONObject();
        items.put(TYPE, OBJECT);
        root.put(ITEMS, items);

        JSONObject objectProperties = new JSONObject();
        items.put(PROPERTIES, objectProperties);

        addFieldsToProfile(objectTypeId, objectProperties, StringUtil.EMPTY_STRING, 0, StringUtil.EMPTY_STRING);

        return profile.toString();
    }

    /**
     * Override this method to add the fields corresponding to the given Object Type ID to the profile being generated.
     *
     * @param objectTypeId           representing the Veeva Object name
     * @param profile                the profile being constructed
     * @param path                   the current base path of the fields
     * @param depth                  the current depth of the fields
     * @param lookupRelationshipName the current relationship name of the fields
     */
    abstract void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
            String lookupRelationshipName);

    void addFieldToProfile(JSONObject profile, ProfileField field) {
        if (field.isSubQuery() || field.getName() == null) {
            return;
        }

        profile.put(field.getName(), field.getSchema());
    }

    /**
     * Get the fields associated with the given Object Type ID to be included in the profile
     *
     * @param objectTypeId representing the Veeva Object name
     * @return an {@link Iterable} of {@link ProfileField} to be added to the profile
     */
    Iterable<ProfileField> getProfileFields(String objectTypeId) {
        if (isDocumentObjectType(objectTypeId)) {
            JSONObject metadata = _metadataRetriever.getDocumentsMetadata();
            return buildProfileFieldsIterable(metadata.getJSONArray(PROPERTIES));
        }

        if (isRelationshipObjectType(objectTypeId)) {
            return buildProfileFieldsIterable(
                    new JSONArray(new JSONTokener(FileProfileLoader.getQueryRelationshipsResponse())));
        }

        JSONObject metadata = _metadataRetriever.getObjectMetadata(objectTypeId);
        return getProfileFields(metadata);
    }

    private Iterable<ProfileField> getProfileFields(JSONObject metadata) {
        JSONObject object = metadata.getJSONObject(OBJECT);
        JSONArray fields = object.getJSONArray(FIELDS);

        if (VeevaOperationType.QUERY == _operationType && object.has(RELATIONSHIPS)) {
            // We are going to copy child relationships (1:M) to fields for processing by addPropertyToSchema
            JSONArray relationships = object.getJSONArray(RELATIONSHIPS);
            for (int i = 0; i < relationships.length(); i++) {
                JSONObject relationship = relationships.getJSONObject(i);
                if (CHILD.equals(relationship.getString(RELATIONSHIP_TYPE)) || REFERENCE_INBOUND.equals(
                        relationship.getString(RELATIONSHIP_TYPE))) {
                    fields.put(relationship);
                    relationship.put(TYPE, SUBQUERY);
                    relationship.put(NAME, StringUtil.EMPTY_STRING);
                }
            }
        }

        return buildProfileFieldsIterable(fields);
    }

    private Iterable<ProfileField> buildProfileFieldsIterable(JSONArray metadata) {
        return CollectionUtil.toIterable(new Iterator<ProfileField>() {
            private int _currentIndex;

            @Override
            public boolean hasNext() {
                return metadata.length() > _currentIndex;
            }

            @Override
            public ProfileField next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return getProfileField(metadata.getJSONObject(_currentIndex++));
            }
        });
    }

    static boolean isDocumentObjectType(String objectType) {
        return DOCUMENTS.equals(objectType);
    }

    static boolean isRelationshipObjectType(String objectType) {
        return RELATIONSHIPS.equals(objectType);
    }

    private ProfileField getProfileField(JSONObject fieldMetadata) {
        String typeName = fieldMetadata.getString(TYPE).toLowerCase();
        // If a child relationship, we don't want to create a primitive string/foreign key field
        // just a child relationship object for subqueries
        if (SUBQUERY.contentEquals(typeName)) {
            return new ProfileField(true, true, null, STRING, fieldMetadata);
        }

        boolean isObjectReference = false;
        boolean isChild = false;
        String specFilterType = STRING;
        String jsonSchemaTypeName;

        JSONObject schema = new JSONObject();
        String format = null;

        switch (typeName) {
            case BYTE:
            case INT_16:
            case INT_32:
            case INT_64:
                jsonSchemaTypeName = INTEGER;
                specFilterType = NUMBER;
                break;
            case SINGLE:
            case DOUBLE:
            case DECIMAL:
            case NUMBER:
            case AUTONUMBER:
                jsonSchemaTypeName = NUMBER;
                specFilterType = NUMBER;
                break;
            case BOOLEAN:
                jsonSchemaTypeName = BOOLEAN;
                specFilterType = BOOLEAN;
                break;
            case DATETIME:
                jsonSchemaTypeName = STRING;
                format = DATE_TIME;
                specFilterType = DATE;
                break;
            case DATE:
                jsonSchemaTypeName = STRING;
                format = DATE;
                specFilterType = DATE;
                break;
            case TIME:
                jsonSchemaTypeName = STRING;
                format = TIME;
                break;
            case OBJECTREFERENCE:
            case OBJECT:
                jsonSchemaTypeName = STRING;
                isObjectReference = true;
                break;
            case STRING:
            case ID:
            case URL:
            case LOOKUP:
            case EXACTMATCHSTRING:
            case PICKLIST:
            default:
                jsonSchemaTypeName = STRING;
                break;
        }
        if (PICKLIST.equals(typeName) && VeevaOperationType.QUERY == _operationType) {
            // picklist query results return an array of strings, not a string field
            JSONObject picklistArray = new JSONObject();
            picklistArray.put(TYPE, STRING);
            schema.put(TYPE, ARRAY);
            schema.put(ITEMS, picklistArray);
        } else {
            schema.put(TYPE, jsonSchemaTypeName);
            schema.putOpt(FORMAT, format);
            putMaxLength(fieldMetadata, schema);
        }

        addJSONSchemaFieldDescription(schema, fieldMetadata);

        return new ProfileField(isObjectReference, isChild, schema, specFilterType, fieldMetadata);
    }

    private static void putMaxLength(JSONObject fieldMetadata, JSONObject schema) {
        if (fieldMetadata.has(MAXLENGTH) && !fieldMetadata.isNull(MAXLENGTH)) {
            schema.put(MAXLENGTH, fieldMetadata.get(MAXLENGTH));
        } else if (fieldMetadata.has(MAX_LENGTH) && !fieldMetadata.isNull(MAX_LENGTH)) {
            schema.put(MAXLENGTH, fieldMetadata.get(MAX_LENGTH));
        }
    }

    /**
     * Default visibility for testing purposes.
     */
    static void addJSONSchemaFieldDescription(JSONObject property, JSONObject metadataField) {
        String description = StringUtil.EMPTY_STRING;
        if (metadataField.has(KEY_LABEL) && !metadataField.isNull(KEY_LABEL)) {
            description = metadataField.getString(KEY_LABEL);
        }

        String helpContent = StringUtil.EMPTY_STRING;
        if (metadataField.has(KEY_HELPCONTENT) && !metadataField.isNull(KEY_HELPCONTENT)) {
            helpContent = metadataField.getString(KEY_HELPCONTENT);
        } else if (metadataField.has(KEY_HELP_CONTENT) && !metadataField.isNull(KEY_HELP_CONTENT)) {
            helpContent = metadataField.getString(KEY_HELP_CONTENT);
        }
        if (StringUtil.isNotBlank(helpContent)) {
            if (StringUtil.isNotBlank(description)) {
                description += DESCRIPTION_SEPARATOR;
            }
            description += helpContent;
        }
        if (StringUtil.isNotBlank(description)) {
            property.put(DESCRIPTION, description);
        }
    }
}
