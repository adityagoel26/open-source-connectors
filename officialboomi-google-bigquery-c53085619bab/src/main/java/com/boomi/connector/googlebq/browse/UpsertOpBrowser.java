// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.discovery.BigQueryDiscoveryDocumentUtil;
import com.boomi.connector.googlebq.fields.ImportableFieldsFactory;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;

public class UpsertOpBrowser extends BaseTableListBrowser {

    private static final String UPSERT_RESULT = "upsert_result";
    private static final String GENERIC_TABLE_ID = "GENERIC";
    private static final String GENERIC_TABLE_LABEL = "Generic Table";

    /**
     * Create a new instance of {@link UpsertOpBrowser}.
     *
     * @param conn
     *         a {@link GoogleBqBaseConnection<BrowseContext>} instance.
     */
    public UpsertOpBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        super(conn);
    }

    @Override
    public ObjectTypes getObjectTypes() {
        String datasetId = getConnection().getDatasetId();
        ObjectTypes objectTypes = new ObjectTypes();
        objectTypes.withTypes(new ObjectType().withId(GENERIC_TABLE_ID).withLabel(GENERIC_TABLE_LABEL));
        if (StringUtil.isNotEmpty(datasetId)) {
            objectTypes.withTypes(getTableObjectTypes());
        }
        return objectTypes;
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        ObjectDefinitions definitions = new ObjectDefinitions();
        Collection<ObjectDefinition> objectDefinitions = new ArrayList<>();
        ImportableFieldsFactory factory = new ImportableFieldsFactory(getConnection());

        JsonNode discoveryDocument = BigQueryDiscoveryDocumentUtil.addUpsertResultSchemaToDiscoverDoc();
        String elementName = BigQueryDiscoveryDocumentUtil.getSchemaChild(UPSERT_RESULT);
        for (ObjectDefinitionRole role : roles) {
            if (role == ObjectDefinitionRole.OUTPUT) {
                objectDefinitions.add(new ObjectDefinition().withOutputType(ContentType.JSON)
                        .withElementName(elementName).withJsonSchema(discoveryDocument.toString()));
            } else if (role == ObjectDefinitionRole.INPUT) {
                objectDefinitions.add(new ObjectDefinition().withInputType(ContentType.BINARY));
            }
        }
        definitions.withDefinitions(objectDefinitions).withOperationFields(factory.importableFields(objectTypeId));

        return definitions;
    }
}
