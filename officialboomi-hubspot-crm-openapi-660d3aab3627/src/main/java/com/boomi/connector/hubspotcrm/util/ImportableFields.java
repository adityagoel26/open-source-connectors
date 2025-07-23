// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.hubspotcrm.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ui.AllowedValue;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.ValueCondition;
import com.boomi.connector.api.ui.VisibilityCondition;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

/**
 * Map of importable fields, keyed by their ID
 */
public class ImportableFields extends TreeMap<String, ImportableFields.DeserializableBrowseField> {

    private static final long serialVersionUID = 20241008L;
    private static final JsonMapper OBJECT_MAPPER = JsonMapper.builder().disable(
            MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS).build();

    public ImportableFields() {
        super(Comparator.naturalOrder());
    }

    /**
     * Get the field with the given ID or throw an exception if not found
     *
     * @param fieldID the ID of the field to get
     * @return the field with the given ID
     * @throws ConnectorException if the field is not found
     */
    public BrowseField getField(String fieldID) {
        BrowseField browseField = get(fieldID);
        if (browseField == null) {
            throw new ConnectorException("Field not found: " + fieldID);
        }

        return browseField;
    }

    /**
     * Parse importable fields from their JSON definition in the given input stream
     *
     * @param stream containing the JSON definition of the importable fields
     * @return the  importable fields
     * @throws ConnectorException if the input stream cannot be parsed
     */
    public static ImportableFields from(InputStream stream) {
        try {
            return OBJECT_MAPPER.readValue(stream, ImportableFields.class);
        } catch (IOException e) {
            throw new ConnectorException("cannot parse importable fields", e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    /**
     * This class is a deserializable version of {@link BrowseField} that allows setting the protected fields. This
     * is required to parse the JSON definition in cloud runtimes with a restrictive security policy.
     */
    public static class DeserializableBrowseField extends BrowseField {

        private static final long serialVersionUID = 20240827L;

        /**
         * Sets the allowed values for the field
         *
         * @param allowedValues the allowed values
         */
        public void setAllowedValues(List<AllowedValue> allowedValues) {
            CollectionUtil.addAll(allowedValues, super.getAllowedValues());
        }

        /**
         * Sets the visibility conditions for the field
         *
         * @param visibilityConditions the visibility conditions
         */
        public void setVisibilityConditions(List<DeserializableVisibilityCondition> visibilityConditions) {
            CollectionUtil.addAll(visibilityConditions, super.getVisibilityConditions());
        }
    }

    public static class DeserializableVisibilityCondition extends VisibilityCondition {

        private static final long serialVersionUID = 20241008L;

        public void setValueConditions(List<DeserializableValueCondition> valueConditions) {
            CollectionUtil.addAll(valueConditions, super.getValueConditions());
        }
    }

    public static class DeserializableValueCondition extends ValueCondition {

        private static final long serialVersionUID = 20241008L;

        public void setValues(List<String> values) {
            CollectionUtil.addAll(values, super.getValues());
        }
    }
}
