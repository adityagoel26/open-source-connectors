// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.veeva.browser.VeevaBrowser;
import com.boomi.connector.veeva.browser.VeevaObjectMetadataRetriever;
import com.boomi.connector.veeva.browser.VeevaOperationType;
import com.boomi.connector.veeva.operation.query.VeevaQueryOperation;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import org.json.JSONObject;

import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link ProfileFactory} for Query operation profiles
 */
class QueryProfileFactory extends ProfileFactory {

    private static final String TYPE_V = "type__v";
    private static final String SUBTYPE_V = "subtype__v";
    private static final String CLASSIFICATION_V = "classification__v";
    private static final String OBJECT = "object";
    private static final String PROPERTIES = "properties";
    private static final String STRING = "string";
    private static final String ARRAY = "array";
    private static final String ITEMS = "items";
    private static final String INCLUDE_SYSTEM_FIELDS_BROWSE_PROPERTY = "INCLUDE_SYSTEM_FIELDS";
    private static final String GET_ITEMS_AT_PATH_LIST_QUERY_PROFILE = "get_items_at_path_list_query_profile.json";
    private static final Set<String> ALLOWED_SYSTEM_FIELDS = CollectionUtil.asImmutableSet("id", "created_by__v",
            "created_date__v", "format__v", "modified_by__v", "modified_date__v", "name__v", "status__v",
            "external_id__v", "global_id__sys", "filename__v", "size__v", "latest_version__v",
            "major_version_number__v", "minor_version_number__v", TYPE_V, SUBTYPE_V, CLASSIFICATION_V, "version_id",
            "file_modified_date__v", "version_modified_date__v", "file_created_date__v", "version_created_date__v",
            "file_modified_by__v", "version_modified_by__v", "file_created_by__v", "version_created_by__v");

    private final List<FieldSpecField> _queryFields;
    private final long _maxDepth;
    private final boolean _includeSystemField;

    QueryProfileFactory(List<FieldSpecField> queryFields, long maxDepth, BrowseContext context,
            VeevaObjectMetadataRetriever metadataRetriever) {
        super(VeevaOperationType.QUERY, metadataRetriever);
        _queryFields = queryFields;
        _maxDepth = maxDepth;
        _includeSystemField = context.getOperationProperties().getBooleanProperty(INCLUDE_SYSTEM_FIELDS_BROWSE_PROPERTY,
                false);
    }

    @Override
    public String getJsonProfile(String objectTypeId) {
        if (VeevaQueryOperation.GET_ITEMS_AT_PATH_LIST_OBJECT_TYPE.equals(objectTypeId)) {
            return FileProfileLoader.readResource(GET_ITEMS_AT_PATH_LIST_QUERY_PROFILE);
        }

        JSONObject profile = new JSONObject();
        profile.put("$schema", "http://json-schema.org/schema#");

        JSONObject root = new JSONObject();
        root.put("type", OBJECT);
        profile.put(objectTypeId, root);

        JSONObject properties = new JSONObject();
        root.put(PROPERTIES, properties);

        addFieldsToProfile(objectTypeId, properties, StringUtil.EMPTY_STRING, 0, null);

        _queryFields.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));

        return profile.toString();
    }

    @Override
    void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
            String lookupRelationshipName) {
        for (ProfileField field : getProfileFields(objectTypeId)) {
            if (!shouldIncludeQueryField(field, objectTypeId)) {
                continue;
            }

            boolean isVaultObject = !isDocumentObjectType(objectTypeId) && !isRelationshipObjectType(objectTypeId);
            if (isVaultObject || field.isQueryable() || field.isSubQuery()) {
                addField(profile, field, path, depth, lookupRelationshipName);
            }
        }
    }

    private boolean shouldIncludeQueryField(ProfileField field, String objectTypeId) {
        boolean includeSystemFields = includeSystemFields(objectTypeId);
        if (includeSystemFields) {
            return true;
        }

        // include the field anyway if it is in the allowed list
        if (ALLOWED_SYSTEM_FIELDS.contains(field.getName())) {
            return true;
        }

        // don't include the field if it is a system attribute
        return !field.isSystemAttribute();
    }

    private boolean includeSystemFields(String objectTypeId) {
        if (isDocumentObjectType(objectTypeId) && _maxDepth > 0) {
            return false;
        }
        return _includeSystemField;
    }

    private void addField(JSONObject profile, ProfileField field, String path, int currentDepth,
            String lookupRelationshipName) {
        if (lookupRelationshipName == null) {
            lookupRelationshipName = "";
        }

        if (!field.isSubQuery()) {
            profile.put(lookupRelationshipName + field.getName(), field.getSchema());
        }

        if (StringUtil.isNotBlank(field.getName())) {
            addFieldSpecField(path + field.getName(), field.getSpecFilterType());
        }

        if (field.isObjectReference() && currentDepth < _maxDepth) {
            addRelationshipFields(profile, field, field.isChild(), currentDepth, lookupRelationshipName);
        }
    }

    private void addRelationshipFields(JSONObject profile, ProfileField field, boolean isChild, int currentDepth,
            String lookupRelationshipName) {
        String relationshipName = field.getRelationshipName();
        String relationshipObject = field.getRelationshipObject();

        if (StringUtil.isBlank(relationshipName) || StringUtil.isBlank(relationshipObject)) {
            return;
        }

        if (isChild) {
            // If the field represents a child Veeva Object, create an empty field schema to be recursively populated
            // by #addFieldsToProfile
            JSONObject child = new JSONObject();
            child.put("type", OBJECT);
            profile.put(relationshipName, child);

            JSONObject arrayProperties = new JSONObject();
            child.put(PROPERTIES, arrayProperties);

            JSONObject array = new JSONObject();
            array.put("type", ARRAY);
            arrayProperties.put("data", array);

            JSONObject items = new JSONObject();
            items.put("type", OBJECT);
            array.put(ITEMS, items);

            // the properties node is later filled with the child Veeva Object fields
            JSONObject childProperties = new JSONObject();
            items.put(PROPERTIES, childProperties);

            // add subquery suffix to the relationship name for child relationships
            relationshipName = relationshipName + VeevaBrowser.SUBQUERY_SUFFIX;
            profile = childProperties;
        } else {
            // add dot suffix to the relationship name for other relationships
            lookupRelationshipName = relationshipName + ".";
        }

        // add the relationship field to the list of fieldSpecs
        addFieldSpecField(relationshipName, STRING);

        // recursive call to populate the relationship fields
        addFieldsToProfile(relationshipObject, profile, relationshipName + "/", currentDepth + 1,
                lookupRelationshipName);
    }

    private void addFieldSpecField(String name, String filterType) {
        FieldSpecField fieldSpecField = new FieldSpecField().withName(name).withSelected(false).withSelectable(true)
                .withFilterable(true).withSortable(true).withType(filterType);
        _queryFields.add(fieldSpecField);
    }
}
