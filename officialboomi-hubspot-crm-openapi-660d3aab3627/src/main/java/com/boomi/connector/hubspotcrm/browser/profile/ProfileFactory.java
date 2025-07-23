// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.hubspotcrm.browser.profile;

import com.boomi.connector.hubspotcrm.browser.HubspotcrmObjectMetadataRetriever;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmOperationType;
import com.boomi.util.StringUtil;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.stream.IntStream;

/**
 * Factory class to create JSON Profiles for Hubspotcrm Operations
 */
public abstract class ProfileFactory {

    private static final String STRING = "string";
    private static final String NUMBER = "number";
    private static final String BOOLEAN = "boolean";
    private static final String ARRAY = "array";
    private static final String ITEMS = "items";
    private static final String OBJECT = "object";
    private static final String PROPERTIES = "properties";
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
    private static final String ENUMERATION = "enumeration";
    private static final String EXACTMATCHSTRING = "exactmatchstring";
    private static final String TYPE = "type";
    private static final String FORMAT = "format";
    private static final String FIELDS = "results";
    private static final String SCHEMA = "$schema";
    private static final String SCHEMA_HEADER = "http://json-schema.org/schema#";

    protected final HubspotcrmOperationType _operationType;
    protected final HubspotcrmObjectMetadataRetriever _metadataRetriever;

    /**
     * Get a profile factory instance for Create and Update operations
     *
     * @param operationType     the operation type indicating if it is a Create or Update operation
     * @param metadataRetriever the metadataRetriever to obtain the entity information for the operation profile
     * @return an instance of {@link ProfileFactory}
     */
    public static ProfileFactory getProfileFactory(HubspotcrmOperationType operationType,
            HubspotcrmObjectMetadataRetriever metadataRetriever) {
        return new ApiProfileFactory(operationType, metadataRetriever);
    }

    ProfileFactory(HubspotcrmOperationType operationType, HubspotcrmObjectMetadataRetriever metadataRetriever) {
        _operationType = operationType;
        _metadataRetriever = metadataRetriever;
    }

    /**
     * Build the profile for the given object type ID
     *
     * @param objectTypeId representing the Hubspotcrm Object name
     * @return a string representing the JSON profile schema
     */
    public String getJsonProfile(String objectTypeId, HubspotcrmOperationType operationType, String object) {
        JSONObject profile = new JSONObject();
        profile.put(SCHEMA, SCHEMA_HEADER);

        JSONObject root = new JSONObject();
        profile.put(object, root);
        JSONObject prop = new JSONObject();
        JSONObject objectProperties = new JSONObject();
        JSONObject subObjectProperties = new JSONObject();
        if (HubspotcrmOperationType.CREATE.equals(operationType)) {
            getAssociationProfile(prop, objectProperties);
        }
        prop.put(PROPERTIES, objectProperties);
        root.put(PROPERTIES, prop);
        root.put(TYPE, OBJECT);
        objectProperties.put(TYPE, OBJECT);
        objectProperties.put(PROPERTIES, subObjectProperties);
        addFieldsToProfile(objectTypeId, subObjectProperties, StringUtil.EMPTY_STRING, 0, StringUtil.EMPTY_STRING);

        return profile.toString();
    }

    private static void getAssociationProfile(JSONObject prop, JSONObject mainObjectProperties) {
        JSONObject association = new JSONObject();
        association.put(TYPE, ARRAY);
        JSONObject associationProp = new JSONObject();
        associationProp.put(TYPE, OBJECT);
        association.put(ITEMS, associationProp);
        JSONObject associationPropItemprop = new JSONObject();
        prop.put(PROPERTIES, mainObjectProperties);
        JSONObject associationto = new JSONObject();
        JSONObject associationtoprop = new JSONObject();
        JSONObject associationtopropitem = new JSONObject();
        associationtopropitem.put(TYPE, STRING);
        associationtoprop.put("id", associationtopropitem);
        associationto.put(TYPE, OBJECT);
        associationto.put(PROPERTIES, associationtoprop);
        associationPropItemprop.put("to", associationto);
        JSONObject associationtypes = new JSONObject();
        JSONObject associationtypesitem = new JSONObject();
        JSONObject associationtypesitemprop = new JSONObject();
        JSONObject associationtypesitempropid = new JSONObject();
        JSONObject associationtypesitemproptype = new JSONObject();

        associationtypesitem.put(PROPERTIES, associationtypesitemprop);
        associationtypesitem.put(TYPE, OBJECT);
        associationtypes.put(TYPE, ARRAY);
        associationtypes.put(ITEMS, associationtypesitem);
        associationPropItemprop.put("types", associationtypes);
        associationtypesitempropid.put(TYPE, STRING);
        associationtypesitemproptype.put(TYPE, STRING);
        associationtypesitemprop.put("associationCategory", associationtypesitempropid);
        associationtypesitemprop.put("associationTypeId", associationtypesitemproptype);
        associationProp.put(PROPERTIES, associationPropItemprop);
        prop.put("associations", association);
    }

    /**
     * Override this method to add the fields corresponding to the given Object Type ID to the profile being generated.
     *
     * @param objectTypeId           representing the Hubspotcrm Object name
     * @param profile                the profile being constructed
     * @param path                   the current base path of the fields
     * @param depth                  the current depth of the fields
     * @param lookupRelationshipName the current relationship name of the fields
     */
    abstract void addFieldsToProfile(String objectTypeId, JSONObject profile, String path, int depth,
            String lookupRelationshipName);

    void addFieldToProfile(JSONObject profile, ProfileField field) {
        String name = field.getName();
        if (field.isSubQuery() || name == null) {
            return;
        }

        profile.put(name, field.getSchema());
    }

    /**
     * Get the fields associated with the given Object Type ID to be included in the profile
     *
     * @param objectTypeId representing the Hubspotcrm Object name
     * @return an {@link Iterable} of {@link ProfileField} to be added to the profile
     */
    Iterable<ProfileField> getProfileFields(String objectTypeId) {

        JSONObject metadata = _metadataRetriever.getObjectMetadata(objectTypeId);
        return getProfileFields(metadata);
    }

    /**
     * Retrieves the profile fields from the given metadata JSONObject.
     *
     * @param metadata a JSONObject containing the metadata for the profile fields
     * @return an Iterable of ProfileField objects representing the profile fields
     */
    private static Iterable<ProfileField> getProfileFields(JSONObject metadata) {
        JSONArray fields = metadata.getJSONArray(FIELDS);
        return buildProfileFieldsIterable(fields);
    }

    /**
     * Returns an Iterable of ProfileField by mapping each index in the JSON array to a ProfileField object
     */
    private static Iterable<ProfileField> buildProfileFieldsIterable(JSONArray metadata) {
        return () ->
                IntStream.range(0, metadata.length())
                .mapToObj(i -> getProfileField(metadata.getJSONObject(i)))
                .iterator();
    }

    private static ProfileField getProfileField(JSONObject fieldMetadata) {
        String typeName = fieldMetadata.getString(TYPE).toLowerCase();
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
            case ENUMERATION:
            default:
                jsonSchemaTypeName = STRING;
                break;
        }

        schema.put(TYPE, jsonSchemaTypeName);
        schema.putOpt(FORMAT, format);

        return new ProfileField(isObjectReference, isChild, schema, specFilterType, fieldMetadata);
    }
}