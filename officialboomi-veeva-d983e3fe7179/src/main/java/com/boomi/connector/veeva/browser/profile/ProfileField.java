// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser.profile;

import org.json.JSONObject;

/**
 * Class representing a Veeva Object property. Each property is mapped as a Profile Field
 */
class ProfileField {

    private static final String SUBQUERY = "subquery";
    private static final String TYPE = "type";
    private static final String NAME = "name";
    private static final String RELATIONSHIP_OUTBOUND_NAME = "relationship_outbound_name";
    private static final String RELATIONSHIPNAME = "relationshipName";
    private static final String RELATIONSHIP_NAME = "relationship_name";
    private static final String OBJECT_TYPE = "objectType";
    private static final String OBJECT = "object";
    private static final String QUERYABLE = "queryable";
    private static final String SYSTEM_ATTRIBUTE = "systemAttribute";
    private static final String REQUIRED = "required";
    private static final String EDITABLE = "editable";
    private static final String SET_ON_CREATE_ONLY = "setOnCreateOnly";

    private final boolean _isObjectReference;
    private final boolean _isChild;
    private final JSONObject _schema;
    private final String _specFilterType;
    private final JSONObject _metadata;

    /**
     * Construct a Profile Field from the given metadata. The field can represent a primitive value or be a reference
     * to another Veeva Object
     *
     * @param isObjectReference indicates whether this field is a reference to another Veeva Object or not
     * @param isChild           indicates if this field represents a child Veeva Object
     * @param schema            the JSON schema representing this profile field
     * @param specFilterType    the Query SpecFilter Type for this field
     * @param metadata          the metadata associated to this Veeva Object field
     */
    ProfileField(boolean isObjectReference, boolean isChild, JSONObject schema, String specFilterType,
            JSONObject metadata) {
        _isObjectReference = isObjectReference;
        _isChild = isChild;
        _schema = schema;
        _specFilterType = specFilterType;
        _metadata = metadata;
    }

    /**
     * Get the name of this field or {@code null} if not available
     *
     * @return the name of the profile field
     */
    public String getName() {
        return _metadata.has(NAME) ? _metadata.getString(NAME) : null;
    }

    /**
     * Indicate whether this field represents a Veeva SubQuery field or not
     *
     * @return {@code true} if the field represents a SubQuery, {@code false} otherwise
     */
    boolean isSubQuery() {
        return SUBQUERY.equals(_metadata.getString(TYPE));
    }

    /**
     * Indicates whether this field represents a child Veeva Object or not
     *
     * @return {@code true} if the field represents a child Veeva Object, {@code false} otherwise
     */
    boolean isChild() {
        return _isChild;
    }

    /**
     * Indicates whether this field is a reference to another Veeva Object or not
     *
     * @return {@code true} if the field is a Veeva Object reference, {@code false} otherwise
     */
    boolean isObjectReference() {
        return _isObjectReference;
    }

    /**
     * Get the JSON schema representing this profile field
     *
     * @return the profile field JSON schema
     */
    JSONObject getSchema() {
        return _schema;
    }

    /**
     * Get the Query SpecFilter type for this profile field
     *
     * @return the Query SpecFilter types
     */
    String getSpecFilterType() {
        return _specFilterType;
    }

    /**
     * Indicates whether this field has the property queryable set to true
     *
     * @return {@code true} if the field is queryable, {@code false} otherwise
     */
    boolean isQueryable() {
        return _metadata.has(QUERYABLE) && _metadata.getBoolean(QUERYABLE);
    }

    /**
     * Indicates whether this field has the property systemAttribute set to true
     *
     * @return {@code true} if the field is a systemAttribute, {@code false} otherwise
     */
    boolean isSystemAttribute() {
        return _metadata.has(SYSTEM_ATTRIBUTE) && _metadata.getBoolean(SYSTEM_ATTRIBUTE);
    }

    /**
     * Indicates whether this field has the property required set to true
     *
     * @return {@code true} if the field is required, {@code false} otherwise
     */
    boolean isRequired() {
        return _metadata.has(REQUIRED) && _metadata.getBoolean(REQUIRED);
    }

    /**
     * Indicates whether this field has the property editable set to true
     *
     * @return {@code true} if the field is editable, {@code false} otherwise
     */
    boolean isEditable() {
        return _metadata.has(EDITABLE) && _metadata.getBoolean(EDITABLE);
    }

    /**
     * Indicates whether this field has the property setOnCreateOnly set to true
     *
     * @return {@code true} if the field is setOnCreateOnly, {@code false} otherwise
     */
    boolean isSetOnCreateOnly() {
        return _metadata.has(SET_ON_CREATE_ONLY) && _metadata.getBoolean(SET_ON_CREATE_ONLY);
    }

    /**
     * Get the relationship name of the Veeva Object represented by this field with the Veeva Object that contains it
     *
     * @return the relationship name
     */
    String getRelationshipName() {
        if (_metadata.has(RELATIONSHIP_OUTBOUND_NAME)) {
            return _metadata.getString(RELATIONSHIP_OUTBOUND_NAME);
        }
        if (_metadata.has(RELATIONSHIPNAME)) {
            return _metadata.getString(RELATIONSHIPNAME);
        }
        if (_metadata.has(RELATIONSHIP_NAME)) {
            return _metadata.getString(RELATIONSHIP_NAME);
        }
        return null;
    }

    /**
     * Get the relationship object of the Veeva Object represented by this field with the Veeva Object that contains it
     *
     * @return the relationship object
     */
    String getRelationshipObject() {
        if (_metadata.has(OBJECT_TYPE)) {
            return _metadata.getString(OBJECT_TYPE);
        }
        if (_metadata.has(OBJECT)) {
            JSONObject object = _metadata.getJSONObject(OBJECT);
            if (object.has(NAME)) {
                return object.getString(NAME);
            }
        }

        return null;
    }
}
