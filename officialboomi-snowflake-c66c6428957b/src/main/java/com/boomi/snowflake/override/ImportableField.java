// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.override;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.api.ui.DisplayType;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;

import java.util.ArrayList;
import java.util.Collection;

/**
 * ImportableField class for Import of Browse Fields
 */
public class ImportableField {

    /** Creates a BrowseField for schema name.*/
   private static final BrowseField SCHEMA = createBrowseFields(SnowflakeOverrideConstants.SCHEMA, "Schema Name",
           "Schema name, Case-sensitive if written between double quotation marks.", null,
           true);

   /** Creates a BrowseField for database name.*/
   private static final BrowseField DB = createBrowseFields(SnowflakeOverrideConstants.DATABASE,
            "Database Name", "Database name, Case-sensitive if written between double quotation marks.",
            null, true);

    /**
     * Private constructor to prevent instantiation.
     * @throws ConnectorException Always thrown when invoked.
     */
    private ImportableField() {
        throw new ConnectorException("Unable to instantiate class");
    }

    /**
     * Creates a BrowseField object with the specified parameters.
     *
     * @param id The unique identifier for the BrowseField.
     * @param label The display label for the BrowseField.
     * @param helpText The help text associated with the BrowseField.
     * @param displayType The display type of the BrowseField.
     * @param overrideable Indicates whether the BrowseField can be overridden.
     * @return A new BrowseField object configured with the provided parameters.
     */
    private static BrowseField createBrowseFields(String id, String label, String helpText, DisplayType displayType,
            boolean overrideable){
        BrowseField browseField = new BrowseField().withType(DataType.STRING).withId(id).withLabel(label);
        browseField.setHelpText(helpText);
        browseField.setDisplayType(displayType);
        browseField.setOverrideable(overrideable);
        return browseField;

    }

    /**
     * Creates a collection of {@link BrowseField} with pre-defined default values.
     *
     * @param dbName     the default value for the database field. If {@code null} or empty, the field will be
     *                   created without a default value.
     * @param schemaName the default value for the schema field. If {@code null} or empty, the field will be created
     *                   without a default value.
     * @return a list of {@link BrowseField} objects, each initialized with the provided default values.
     */
    public static Collection<BrowseField> getOverridableFields(String dbName, String schemaName) {
        Collection<BrowseField> importableFields = new ArrayList<>();
        importableFields.add(DB.withDefaultValue(dbName));
        importableFields.add(SCHEMA.withDefaultValue(schemaName));
        return importableFields;
    }
}
