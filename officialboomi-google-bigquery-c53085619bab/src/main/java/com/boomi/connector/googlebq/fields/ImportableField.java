//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.fields;

import com.boomi.connector.api.ui.AllowedValue;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.api.ui.DisplayType;
import com.boomi.connector.api.ui.ValueCondition;
import com.boomi.connector.api.ui.VisibilityCondition;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import java.util.List;

class ImportableField {

    private static final String RUN_SQL_AFTER_LOAD_FIELD_ID = "runSqlAfterLoad";

    private static final ValueCondition RUN_SQL_VALUE_CONDITION = (ValueCondition) new ValueCondition().withValues(
            "true").withFieldId(RUN_SQL_AFTER_LOAD_FIELD_ID);

    private static final ValueCondition NOT_RUN_SQL_VALUE_CONDITION = (ValueCondition) new ValueCondition().withValues(
            "false").withFieldId(RUN_SQL_AFTER_LOAD_FIELD_ID);

    private static final VisibilityCondition RUN_SQL_VC =
            (VisibilityCondition) new VisibilityCondition().withValueConditions(RUN_SQL_VALUE_CONDITION);

    private static final VisibilityCondition NOT_RUN_SQL_VC =
            (VisibilityCondition) new VisibilityCondition().withValueConditions(NOT_RUN_SQL_VALUE_CONDITION);

    protected static final ImportableField DESTINATION_TABLE_FOR_LOAD = new ImportableField(DataType.STRING,
            "destinationTableForLoad", "Destination Table For Load", "The destination table to load the data into.",
            NOT_RUN_SQL_VC);

    protected static final ImportableField TEMPORARY_TABLE_FOR_LOAD = new ImportableField(DataType.STRING,
            "temporaryTableForLoad", "Temporary Table For Load", "The temporary table to load the data into.",
            RUN_SQL_VC);

    protected static final ImportableField TABLE_SCHEMA = new ImportableField(DataType.STRING, "tableSchema",
            "Table Schema",
            "Specify the JSON schema for the destination table. The default value matches the table schema obtained "
                    + "from BigQuery for the table selected as the object type. If Generic Table is selected as the "
                    + "object type, no table schema appears by default.", DisplayType.TEXTAREA);

    protected static final ImportableField RUN_SQL_AFTER_LOAD = new ImportableField(DataType.BOOLEAN,
            RUN_SQL_AFTER_LOAD_FIELD_ID, "Run SQL After Load",
            "Controls the execution of a SQL command after the data is loaded into the table. If selected, a SQL "
                    + "command executes. If cleared, a SQL command does not execute.", "true");

    protected static final ImportableField SQL_COMMAND = new ImportableField(DataType.STRING, "sqlCommand",
            "SQL Command",
            "Specify the SQL command to execute after loading the data into the table. Any row that does not match "
                    + "with the pre-defined template query is added as a new row. To ensure compatibility, customize "
                    + "the SQL based on your requirements.", DisplayType.TEXTAREA, RUN_SQL_VC);

    protected static final ImportableField TARGET_TABLE_FOR_QUERY = new ImportableField(DataType.STRING,
            "targetTableForQuery", "Target Table For Query",
            "Specify the table to use as the target of the SQL command. The default value depends upon the selected "
                    + "object type. If Generic Table is selected as the object type, the default is empty. Otherwise,"
                    + " the name of the table selected as the object type appears as the default value.", RUN_SQL_VC);

    protected static final ImportableField USE_LEGACY_SQL = new ImportableField(DataType.BOOLEAN, "useLegacySql",
            "Use Legacy SQL",
            "Controls the use of BigQuery's legacy SQL dialect. If selected, BigQuery's legacy SQL dialect is used "
                    + "for the query. If cleared, BigQuery's standard SQL is used.", RUN_SQL_VC, "true");

    private static final AllowedValue DELETE_ALWAYS_VALUE = new AllowedValue().withValue("DELETE_ALWAYS_VALUE")
            .withLabel("Delete always");
    private static final AllowedValue DELETE_IF_SUCCESS_VALUE = new AllowedValue().withValue("DELETE_IF_SUCCESS_VALUE")
            .withLabel("Delete if query is successful");
    private static final AllowedValue NO_DELETE_VALUE = new AllowedValue().withValue("NO_DELETE_VALUE").withLabel(
            "Do not delete table");
    private static final List<AllowedValue> ALLOWED_VALUES = CollectionUtil.asList(DELETE_ALWAYS_VALUE,
            DELETE_IF_SUCCESS_VALUE, NO_DELETE_VALUE);

    protected static final ImportableField DELETE_TEMP_TABLE_AFTER_QUERY = new ImportableField(DataType.STRING,
            "deleteTempTableAfterQuery", "Delete Temporary Table After Query",
            "Controls the deletion of the temporary table, if the destination for the load needs to be deleted upon "
                    + "successful completion of the Query job. Select Delete always to always delete the temporary "
                    + "table. Select Delete if query is successful to delete the temporary table upon a successful "
                    + "Query job. Select Do not delete table to indicate the temporary table should not be deleted.",
            DisplayType.LIST, RUN_SQL_VC, "DELETE_ALWAYS_VALUE");

    private final DataType _dataType;
    private final String _id;
    private final String _label;
    private final String _helpText;
    private final DisplayType _displayType;
    private final VisibilityCondition _visibilityCondition;
    private final String _defaultValue;

    ImportableField(DataType dataType, String id, String label, String helpText, String defaultValue) {
        this(dataType, id, label, helpText, null, null, defaultValue);
    }

    ImportableField(DataType dataType, String id, String label, String helpText, DisplayType displayType) {
        this(dataType, id, label, helpText, displayType, null);
    }

    ImportableField(DataType dataType, String id, String label, String helpText,
            VisibilityCondition visibilityCondition) {
        this(dataType, id, label, helpText, null, visibilityCondition, null);
    }

    ImportableField(DataType dataType, String id, String label, String helpText, DisplayType displayType,
            VisibilityCondition visibilityCondition) {
        this(dataType, id, label, helpText, displayType, visibilityCondition, null);
    }

    ImportableField(DataType dataType, String id, String label, String helpText,
            VisibilityCondition visibilityCondition, String defaultValue) {
        this(dataType, id, label, helpText, null, visibilityCondition, defaultValue);
    }

    ImportableField(DataType dataType, String id, String label, String helpText, DisplayType displayType,
            VisibilityCondition visibilityCondition, String defaultValue) {
        _dataType = dataType;
        _id = id;
        _label = label;
        _helpText = helpText;
        _displayType = displayType;
        _visibilityCondition = visibilityCondition;
        _defaultValue = defaultValue;
    }

    BrowseField toBrowseField() {
        BrowseField browseField = new BrowseField().withType(_dataType).withId(_id).withLabel(_label);
        browseField.setHelpText(_helpText);
        browseField.setDisplayType(_displayType);
        browseField.withVisibilityConditions(_visibilityCondition);
        browseField.setDefaultValue(_defaultValue);

        return browseField;
    }

    BrowseField toBrowseField(String defaultValue) {
        BrowseField field = toBrowseField();
        if (StringUtil.isNotBlank(defaultValue)) {
            field.withDefaultValue(defaultValue);
        }
        return field;
    }

    BrowseField withAllowedValues() {
        BrowseField field = toBrowseField();
        field.withAllowedValues(ALLOWED_VALUES);
        return field;
    }
}
