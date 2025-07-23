// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.api.ui.DisplayType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

class QueryImportableFieldsHelperTest {

    @Test
    void buildDocumentVersionFilterOptionsTest() {
        Properties fieldsProperties = QueryImportableFieldsHelper.loadImportableFieldProperties();
        BrowseField field = QueryImportableFieldsHelper.buildDocumentVersionFilterOptions(fieldsProperties);

        Assertions.assertEquals(DataType.STRING, field.getType());
        Assertions.assertEquals("VERSION_FILTER_OPTIONS", field.getId());
        Assertions.assertEquals("Version Filter Options", field.getLabel());
        Assertions.assertEquals("LATEST_VERSION", field.getDefaultValue());
        Assertions.assertEquals("Choose which document versions to return.", field.getHelpText());
        Assertions.assertEquals("ALL_MATCHING_VERSIONS", field.getAllowedValues().get(1).getValue());
        Assertions.assertEquals("LATEST_MATCHING_VERSION", field.getAllowedValues().get(2).getValue());
    }

    @Test
    void importableFieldsForQueryOperation() {

        ObjectDefinitions definitions = new ObjectDefinitions();
        String objectTypeId = "testObjectType";
        QueryImportableFieldsHelper.addImportableFields(definitions, objectTypeId);

        List<BrowseField> operationFields = definitions.getOperationFields();
        for (BrowseField operationField : operationFields) {
            if (operationField.getId().equals("CUSTOM_TERMS")) {
                Assertions.assertEquals(DisplayType.TEXTAREA, operationField.getDisplayType());
            }
        }
        Assertions.assertEquals(4, operationFields.size());
    }
}
