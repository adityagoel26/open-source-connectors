// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.annotation.DataType;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.boomi.util.json.schema.ComplexTypeBuilder;
import com.boomi.util.json.schema.ElementTypeBuilder;
import com.boomi.util.json.schema.ObjectTypeBuilder;
import com.boomi.util.json.schema.SchemaBuilder;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Browser implementation to load object types and  object definition for Big Query Load operation.
 * The object types for streaming operation are a set of tables.
 * To build a  request profile for streaming operation  a getTable request is executed
 * to get the schema for a particular table. A request profile is built for this
 * schema. No response profile is built for streaming operation
 *
 */
public class StreamingOpBrowser extends BaseTableListBrowser {
    private static final String NODE_FIELDS = "fields";
    private static final String NODE_NAME = "name";
    private static final String NODE_TYPE = "type";
    private static final String NODE_MODE = "mode";
    private static final String FIELD_FORMAT_DATE = "yyyy-MM-dd";
    private static final String FIELD_FORMAT_TIME = "HH:mm:ss";
    private static final String FIELD_FORMAT_DATETIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String FIELD_FORMAT_FLOAT = "#.################";

    public StreamingOpBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        super(conn);
    }

    @Override
    public ObjectTypes getObjectTypes() {
        List<ObjectType> resources = getTableObjectTypes();

        if (resources.isEmpty()) {
            throw new ConnectorException(ERROR_NO_RESOURCES);
        }
        return new ObjectTypes().withTypes(resources);
    }

    /**
     * Builds object definitions for Streaming custom operation. Streaming operation does not support
     * response profile. Request profile is composed of table fields of the table selected as object type.
     * {@link TableResource#getTable(String)} returns the meta data of a table. The meta data is
     * then converted to to json schema by a {@link SchemaBuilder}.
     *
     * @param objectTypeId
     *         a String value for the id for the select object type
     * @param roles
     *         a {@link Collection} of {@link ObjectDefinitionRole}
     * @return an {@link ObjectDefinitions} instance.
     */
    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        try {
            List<ObjectDefinition> definitions = new ArrayList<>();

            for (ObjectDefinitionRole role : roles) {
                if (role == ObjectDefinitionRole.INPUT) {
                    TableResource tableResource = new TableResource(getConnection());
                    JsonNode node = tableResource.getTable(objectTypeId);

                    JsonNode schema = node.path(NODE_SCHEMA);

                    if (schema.isMissingNode()) {
                        throw new ConnectorException("Unable to retrieve schema for the table. No schema found");
                    }

                    SchemaBuilder builder = JSONUtil.newSchemaBuilder().withTitle(objectTypeId);
                    generateSchemaFromFields(builder, schema);
                    definitions.add(JSONUtil.newJsonDefinition(role, builder.getSchema()));
                }
            }
            return new ObjectDefinitions().withDefinitions(definitions);
        }
        catch (Exception e) {
            throw new ConnectorException("Unable to generate object definitions", e);
        }

    }

    /**
     * Builds a json schema by adding the fields present in a {@link JsonNode} as
     * a property in json schema.
     *
     * @param builder
     *         a {@link ComplexTypeBuilder} instance.
     * @param schemaNode
     *         a {@link JsonNode} instance.
     */
    private static void generateSchemaFromFields(ComplexTypeBuilder<?> builder, JsonNode schemaNode) {

        JsonNode fields = schemaNode.path(NODE_FIELDS);
        if (fields.isArray()) {
            for (JsonNode field : fields) {
                String fieldName = field.path(NODE_NAME).asText();
                if (StringUtil.isNotEmpty(fieldName)) {
                    ElementTypeBuilder<?> propertyBuilder = builder.buildProperty(fieldName);
                    buildElementDefinition(propertyBuilder, field);
                }
            }
        }
        else {
            throw new ConnectorException("No fields found when building schema");
        }
    }

    /**
     * Builds a definition for an added property in a json schema
     *
     * @param propertyBuilder
     *         a {@link ComplexTypeBuilder} instance.
     * @param field
     *         a {@link JsonNode} instance.
     */
    private static void buildElementDefinition(ElementTypeBuilder<?> propertyBuilder, JsonNode field) {
        String fieldType = field.path(NODE_TYPE).asText();
        switch (fieldType) {
        case GoogleBqConstants.FIELD_TYPE_BYTES:
        case GoogleBqConstants.FIELD_TYPE_INTEGER:
        case GoogleBqConstants.FIELD_TYPE_IN64:
            propertyBuilder.buildInteger();
            break;
        case GoogleBqConstants.FIELD_TYPE_FLOAT:
        case GoogleBqConstants.FIELD_TYPE_FLOAT64:
            propertyBuilder.buildNumber().withBoomiDataType(DataType.NUMBER, FIELD_FORMAT_FLOAT);
            break;
        case GoogleBqConstants.FIELD_TYPE_BOOLEAN:
        case GoogleBqConstants.FIELD_TYPE_BOOL:
            propertyBuilder.buildBoolean();
            break;
        case GoogleBqConstants.FIELD_TYPE_RECORD:
            buildRecordType(propertyBuilder, field);
            break;
        case GoogleBqConstants.FIELD_TYPE_STRUCT:
            ObjectTypeBuilder<?> objectBuilder = propertyBuilder.buildObject();
            generateSchemaFromFields(objectBuilder, field);
            break;
        case GoogleBqConstants.FIELD_TYPE_DATE:
            propertyBuilder.buildString().withBoomiDataType(DataType.DATE_TIME, FIELD_FORMAT_DATE);
            break;
        case GoogleBqConstants.FIELD_TYPE_TIME:
            propertyBuilder.buildString().withBoomiDataType(DataType.DATE_TIME, FIELD_FORMAT_TIME);
            break;
        case GoogleBqConstants.FIELD_TYPE_DATETIME:
            propertyBuilder.buildString().withBoomiDataType(DataType.DATE_TIME, FIELD_FORMAT_DATETIME);
            break;
        case GoogleBqConstants.FIELD_TYPE_STRING:
        case GoogleBqConstants.FIELD_TYPE_TIMESTAMP:
        default:
            propertyBuilder.buildString();
        }
    }

    private static void buildRecordType(ElementTypeBuilder<?> propertyBuilder, JsonNode field){
        String fieldType = field.path(NODE_MODE).asText();
        ObjectTypeBuilder<?> objectBuilder;
        if (GoogleBqConstants.FIELD_MODE_REPEATED.equals(fieldType)) {
            objectBuilder = propertyBuilder.buildArray().buildDefinition().buildObject();
        }
        else {
            objectBuilder = propertyBuilder.buildObject();
        }
        generateSchemaFromFields(objectBuilder, field);
    }
}
