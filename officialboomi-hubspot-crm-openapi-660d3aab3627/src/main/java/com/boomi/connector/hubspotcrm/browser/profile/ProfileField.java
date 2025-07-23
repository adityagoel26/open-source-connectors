// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.hubspotcrm.browser.profile;

import org.json.JSONObject;

/**
 * Class representing a hubspot-crm Object property. Each property is mapped as a Profile Field
 */
class ProfileField {

    private static final String SUBQUERY = "subquery";
    private static final String TYPE = "type";
    private static final String NAME = "name";
    private final JSONObject _schema;
    private final JSONObject _metadata;

    /**
     * Construct a Profile Field from the given metadata. The field can represent a primitive value or be a reference
     * to another hubspot-crm Object
     *
     * @param isObjectReference indicates whether this field is a reference to another hubspot-crm Object or not
     * @param isChild           indicates if this field represents a child hubspot-crm Object
     * @param schema            the JSON schema representing this profile field
     * @param specFilterType    the Query SpecFilter Type for this field
     * @param metadata          the metadata associated to this hubspot-crm Object field
     */
    ProfileField(boolean isObjectReference, boolean isChild, JSONObject schema, String specFilterType,
            JSONObject metadata) {
        _schema = schema;
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
     * Indicate whether this field represents a hubspot-crm SubQuery field or not
     *
     * @return {@code true} if the field represents a SubQuery, {@code false} otherwise
     */
    protected boolean isSubQuery() {
        return SUBQUERY.equals(_metadata.getString(TYPE));
    }

    /**
     * Get the JSON schema representing this profile field
     *
     * @return the profile field JSON schema
     */
    JSONObject getSchema() {
        return _schema;
    }
}