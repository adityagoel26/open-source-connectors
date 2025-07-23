// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.discovery.BigQueryDiscoveryDocumentUtil;
import com.boomi.util.CollectionUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static com.boomi.util.StringUtil.EMPTY_STRING;

public class UpdateOpBrowser extends BaseTableListBrowser {

    private static final String BQ_SCHEMA_TABLE = "Table";
    private static final String NODE_PROPERTIES = "properties";

    /**
     * Create a new instance of {@link UpdateOpBrowser}.
     *
     * @param conn
     *         a {@link GoogleBqBaseConnection<BrowseContext>} instance.
     */
    public UpdateOpBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        super(conn);
    }

    @Override
    public ObjectTypes getObjectTypes() {
        final String resourceType = getResourceType();
        String dynamicLabel = GoogleBqConstants.DYNAMIC_OBJECT_ID + " " + resourceType;

        ObjectTypes objectTypes = new ObjectTypes();
        objectTypes.withTypes(new ObjectType().withId(GoogleBqConstants.DYNAMIC_OBJECT_ID).withLabel(dynamicLabel));
        objectTypes.withTypes(buildObjectTypes(new CollectionUtil.Filter<JsonNode>() {
            @Override
            public boolean accept(JsonNode resource) {
                return resourceType.equalsIgnoreCase(resource.path(NODE_TABLE_TYPE).asText());
            }
        }));
        return objectTypes;
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        List<ObjectDefinition> definitions = new ArrayList<>();
        String resourceType = getResourceType();
        String outputElement = BigQueryDiscoveryDocumentUtil.getSchemaChild(BQ_SCHEMA_TABLE);

        for (ObjectDefinitionRole role : roles) {
            definitions.add(new ObjectDefinition().withOutputType(role == OUTPUT ? ContentType.JSON : ContentType.NONE)
                    .withInputType(role == INPUT ? ContentType.JSON : ContentType.NONE)
                    .withElementName(role == INPUT ? JSONUtil.FULL_DOCUMENT_JSON_POINTER : outputElement)
                    .withJsonSchema(role == INPUT ? getInputSchema(objectTypeId, resourceType) : getOutputSchema()));
        }

        return new ObjectDefinitions().withDefinitions(definitions);
    }

    private String getResourceType() {
        return getContext().getOperationProperties().getProperty(GoogleBqConstants.PROP_RESOURCE_TYPE, EMPTY_STRING);
    }

    private static String getOutputSchema() {
        JsonNode discoveryDocument = BigQueryDiscoveryDocumentUtil.getBqDiscoveryDocReplacingSelfReferences();
        return discoveryDocument.toString();
    }

    private static String getInputSchema(String objectTypeId, String resourceType) {
        JsonNode schema = loadResourceSchema(resourceType);
        if (!GoogleBqConstants.DYNAMIC_OBJECT_ID.equals(objectTypeId)) {
            JsonNode props = schema.path(NODE_PROPERTIES);
            if (props.isObject()) {
                ((ObjectNode) props).remove(GoogleBqConstants.NODE_TABLE_REFERENCE);
            }
        }
        return schema.toString();
    }

}
