// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ui.AllowedValue;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.api.ui.DisplayType;
import com.boomi.connector.veeva.operation.query.VeevaQueryOperation;
import com.boomi.util.ClassUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Helper class with utils methods for creating the importable fields for Query operation
 */
public class QueryImportableFieldsHelper {

    private static final String IMPORTABLE_FIELDS_PROPERTIES = "/fields/importable-fields.properties";
    private static final String DOCUMENTS_VERSION_FILTER_OPTIONS_FIELD_ID = "documents_version_filter_options";
    private static final String DOCUMENTS = "documents";
    private static final String LIST_ITEMS_PATH = "list_items_path";
    private static final String LIST_ITEMS_RECURSIVE = "list_items_recursive";

    private QueryImportableFieldsHelper() {
    }

    /**
     * Add the required importable fields to the given {@link ObjectDefinitions} based on the Object Type ID
     *
     * @param definitions  where the importable fields will be added
     * @param objectTypeId the Object Type ID used to decide which importable fields should be added
     */
    static void addImportableFields(ObjectDefinitions definitions, String objectTypeId) {
        Properties fieldsProperties = loadImportableFieldProperties();

        if (VeevaQueryOperation.GET_ITEMS_AT_PATH_LIST_OBJECT_TYPE.equals(objectTypeId)) {
            definitions.withOperationFields(createImportableField(LIST_ITEMS_PATH, fieldsProperties));
            definitions.withOperationFields(createImportableField(LIST_ITEMS_RECURSIVE, fieldsProperties));
        } else {
            definitions.withOperationFields(createImportableField("page_size", fieldsProperties),
                    createImportableField("maximum_documents", fieldsProperties),
                    createImportableField("find_keyword_search", fieldsProperties),
                    createImportableField("custom_vql_statement", fieldsProperties));

            if (DOCUMENTS.equals(objectTypeId)) {
                definitions.withOperationFields(buildDocumentVersionFilterOptions(fieldsProperties));
            }
        }
    }

    /**
     * Load and return the {@link BrowseField}s configuration from a properties file
     *
     * @return a {@link Properties} containing the configuration associated to the importable fields
     */
    static Properties loadImportableFieldProperties() {
        InputStream stream = null;
        try {
            stream = ClassUtil.getResourceAsStream(IMPORTABLE_FIELDS_PROPERTIES);
            Properties properties = new Properties();
            properties.load(stream);
            return properties;
        } catch (IOException e) {
            throw new ConnectorException("Cannot load file: " + IMPORTABLE_FIELDS_PROPERTIES, e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    /**
     * Default visibility for testing purposes
     */
    static BrowseField buildDocumentVersionFilterOptions(Properties properties) {
        BrowseField versionFilterOptionsField = createImportableField(DOCUMENTS_VERSION_FILTER_OPTIONS_FIELD_ID,
                properties);
        List<AllowedValue> allowedValues = new ArrayList<>();
        for (String option : properties.getProperty(DOCUMENTS_VERSION_FILTER_OPTIONS_FIELD_ID + ".allowed_values")
                .split(",")) {
            String[] optionParts = option.split(":");
            allowedValues.add(new AllowedValue().withValue(optionParts[0]).withLabel(optionParts[1]));
        }
        versionFilterOptionsField.withAllowedValues(allowedValues);
        return versionFilterOptionsField;
    }

    /**
     * Build a {@link BrowseField} from the {@code fieldProperties} provided and its {@code fieldId}
     *
     * @param fieldId          used to extract the field properties
     * @param fieldsProperties containing the properties associated with the fields
     * @return an importable field
     */
    private static BrowseField createImportableField(String fieldId, Properties fieldsProperties) {
        DataType type = DataType.fromValue(fieldsProperties.getProperty(fieldId + ".type"));
        String id = fieldsProperties.getProperty(fieldId + ".id");
        String label = fieldsProperties.getProperty(fieldId + ".label");
        String helpText = fieldsProperties.getProperty(fieldId + ".help_text");
        String defaultValue = fieldsProperties.getProperty(fieldId + ".default_value", StringUtil.EMPTY_STRING);
        String displayType = fieldsProperties.getProperty(fieldId + ".display_type", StringUtil.EMPTY_STRING);

        return new BrowseField().withType(type).withId(id).withLabel(label).withHelpText(helpText).withOverrideable(
                true).withDefaultValue(defaultValue).withDisplayType(
                !StringUtil.isEmpty(displayType) ? DisplayType.valueOf(displayType) : null);
    }
}
