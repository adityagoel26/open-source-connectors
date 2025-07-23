// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.ui.AllowedValue;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.pool.AdapterFactory;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.ClassUtil;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * BaseBrowser implementation for JMS Connector. This class also implements {@link ConnectionTester}, instantiating a
 * JMS Client to test the connection with the service.
 */
public class JMSBrowser extends BaseBrowser implements ConnectionTester {

    private static final Logger LOG = LogUtil.getLogger(JMSBrowser.class);

    private static final String SCHEMA_SEND_OUTPUT = "/schemas/send-output.json";
    private static final String IMPORTABLE_FIELDS_PROPERTIES = "/fields/importable-fields.properties";

    private static final ObjectType DYNAMIC_DESTINATION = new ObjectType().withLabel("Dynamic Destination").withId(
            JMSConstants.DYNAMIC_DESTINATION_ID);

    private final AdapterFactory _adapterFactory;

    JMSBrowser(JMSConnection<BrowseContext> connection) {
        this(connection, new AdapterFactory(connection.getAdapterSettings()));
    }

    JMSBrowser(JMSConnection<BrowseContext> connection, AdapterFactory adapterFactory) {
        super(connection);
        _adapterFactory = adapterFactory;
    }

    /**
     * Load and return the Send Output Document Schema from a JSON file
     *
     * @return a String representation of the Send Output Document Schema
     */
    private static String loadSendOutputSchema() {
        try {
            return JSONUtil.loadSchemaFromResource(SCHEMA_SEND_OUTPUT).toString();
        } catch (IOException e) {
            throw new ConnectorException("Cannot load schema file: " + SCHEMA_SEND_OUTPUT, e);
        }
    }

    static BrowseField buildDestinationField(Properties properties, OperationType operationType) {
        return createImportableField(JMSConstants.PROPERTY_DESTINATION, properties, operationType);
    }

    static BrowseField buildDestinationTypeField(Properties properties, String excludeTypeValue) {
        BrowseField destinationTypeField = createImportableField(JMSConstants.PROPERTY_DESTINATION_TYPE, properties,
                null);
        List<AllowedValue> destinationTypeAllowedValues = new ArrayList<>();
        for (String option : properties.getProperty("destination_type.options").split(",")) {
            String[] optionParts = option.split(":");
            if (!StringUtil.equalsIgnoreCase(optionParts[0], excludeTypeValue)) {
                AllowedValue allowedValue = new AllowedValue();
                allowedValue.setValue(optionParts[0]);
                allowedValue.setLabel(optionParts[1]);
                destinationTypeAllowedValues.add(allowedValue);
            }
        }

        destinationTypeField.withAllowedValues(destinationTypeAllowedValues);
        destinationTypeField.withDefaultValue(CollectionUtil.getFirst(destinationTypeAllowedValues).getValue());

        return destinationTypeField;
    }

    /**
     * Build a {@link BrowseField} from the {@code fieldProperties} provided and its {@code fieldId}
     *
     * @param fieldId          used to extract the field properties
     * @param fieldsProperties containing the properties associated with the fields
     * @param operation
     * @return an importable field
     */
    private static BrowseField createImportableField(String fieldId, Properties fieldsProperties,
            OperationType operation) {
        String id = fieldsProperties.getProperty(fieldId + ".id");
        String label = fieldsProperties.getProperty(fieldId + ".label");
        DataType type = DataType.fromValue(fieldsProperties.getProperty(fieldId + ".type"));

        String helpTextKey = operation == null ? (fieldId + ".help_text") : (fieldId + "." + operation + ".help_text");
        String helpText = fieldsProperties.getProperty(helpTextKey);

        return new BrowseField().withId(id).withLabel(label).withType(type).withHelpText(helpText).withOverrideable(
                true);
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
     * @return a list of object types containing a single 'Dynamic Destination' element.
     */
    @Override
    public ObjectTypes getObjectTypes() {
        return new ObjectTypes().withTypes(DYNAMIC_DESTINATION);
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectType, Collection<ObjectDefinitionRole> roles) {
        Properties fieldsProperties = loadImportableFieldProperties();
        ObjectDefinitions definitions = new ObjectDefinitions();
        OperationType operationType = getContext().getOperationType();
        ObjectDefinition objectDefinition = new ObjectDefinition();
        switch (operationType) {
            case CREATE:
                addSendDefinitions(definitions, objectDefinition, false, roles);
                definitions.withOperationFields(buildDestinationField(fieldsProperties, operationType),
                        buildDestinationTypeField(fieldsProperties, DestinationType.ADT_MESSAGE.name()));
                break;
            case QUERY:
            case LISTEN:
                addGetDefinitions(definitions, objectDefinition, false, roles);
                definitions.withOperationFields(buildDestinationField(fieldsProperties, operationType));

                break;
            default:
                throw new UnsupportedOperationException("invalid operation type: " + operationType);
        }
        return definitions;
    }

    static void addSendDefinitions(ObjectDefinitions definitions, ObjectDefinition objectDefinition,
            boolean isProfileRequired, Iterable<ObjectDefinitionRole> roles) {
        for (ObjectDefinitionRole role : roles) {
            switch (role) {
                case INPUT:
                    if (!isProfileRequired) {
                        objectDefinition.withInputType(ContentType.BINARY).withOutputType(ContentType.NONE);
                    }
                    definitions.withDefinitions(objectDefinition);
                    break;
                case OUTPUT:
                    definitions.withDefinitions(new ObjectDefinition().withJsonSchema(loadSendOutputSchema())
                            .withElementName(JSONUtil.FULL_DOCUMENT_JSON_POINTER).withInputType(ContentType.NONE)
                            .withOutputType(ContentType.JSON));
                    break;
                default:
                    throw new UnsupportedOperationException("unknown role: " + role);
            }
        }
    }

    void addGetDefinitions(ObjectDefinitions definitions, ObjectDefinition objectDefinition, boolean isProfileRequired,
            Iterable<ObjectDefinitionRole> roles) {
        for (ObjectDefinitionRole role : roles) {

            switch (role) {
                case INPUT:
                    definitions.withDefinitions(
                            new ObjectDefinition().withInputType(ContentType.NONE).withOutputType(ContentType.NONE));
                    break;
                case OUTPUT:
                    if (!isProfileRequired) {
                        objectDefinition.withInputType(ContentType.NONE).withOutputType(ContentType.BINARY);
                    }

                    if (OperationType.QUERY == getContext().getOperationType()) {
                        objectDefinition.withFieldSpecFields(newFilter(JMSConstants.PROPERTY_MESSAGE_SELECTOR));
                    }
                    definitions.withDefinitions(objectDefinition);
                    break;
                default:
                    throw new UnsupportedOperationException("unknown role: " + role);
            }
        }
    }

    private static FieldSpecField newFilter(String filterName) {
        return new FieldSpecField().withName(filterName).withFilterable(true);
    }

    /**
     * Tests the connection by instantiating a JMS Adapter.
     * <p>
     * The details of what is being done when instantiating a particular JMS Adapter, can be reviewed in the javadoc of
     * that particular class.
     */
    @Override
    public void testConnection() {
        GenericJndiBaseAdapter adapter = null;
        try {
            adapter = _adapterFactory.makeObject();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Test Connection failed", e);
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(adapter);
        }
    }

    @Override
    public JMSConnection<BrowseContext> getConnection() {
        return (JMSConnection<BrowseContext>) super.getConnection();
    }
}
