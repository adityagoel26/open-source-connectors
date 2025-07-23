// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.googlebq.GoogleBqObjectType;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.discovery.BigQueryDiscoveryDocumentUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;

/**
 * Browser implementation to load {@link ObjectTypes} and {@link ObjectDefinitions} for BigQuery Run Job operation. The
 * object types are the different job types that can be created. Request profile is supported for object definition
 * which represents the job configuration for a particular job type.
 */
public class RunJobOpBrowser extends GoogleBqBrowser {

    private static final String BQ_SCHEMA_JOB = "Job";

    public RunJobOpBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        super(conn);
    }

    /**
     * Object types are the various job types that can be created in Google BigQuery. The different job types are load,
     * query, extract and copy.
     *
     * @return an {@link ObjectTypes} instance
     */
    @Override
    public ObjectTypes getObjectTypes() {
        return new ObjectTypes().withTypes(GoogleBqObjectType.QUERY.toObjectType()).withTypes(
                GoogleBqObjectType.LOAD.toObjectType()).withTypes(GoogleBqObjectType.EXTRACT.toObjectType()).withTypes(
                GoogleBqObjectType.COPY.toObjectType());
    }

    /**
     * Creates a request profile for a particular job configuration. Schema for Job resource is used to create the
     * {@link ObjectDefinition}. The schema is retrieved from google discovery document for big query. Only the job
     * configuration selected as job type/object type is added to the profile. All other configuration is removed from
     * the schema by {@link BigQueryDiscoveryDocumentUtil#removeOtherJobTypes(JsonNode, String)}. The "Job" schema in
     * discovery document contains self referenced schemas. This throws an exception when platform tries to generate a
     * profile. The self referenced schemas are therefore replaced by a single level nesting of the self referenced
     * schema. This is done by {@link BigQueryDiscoveryDocumentUtil#replaceSelfReferences(JsonNode)}
     *
     * @param objectTypeId
     *         a String value for the id for the select object type
     * @param roles
     *         a {@link Collection} of {@link ObjectDefinitionRole}
     * @return a new {@link ObjectDefinitions} instance.
     */
    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        List<ObjectDefinition> definitions = new ArrayList<>();

        JsonNode discoveryDocument = BigQueryDiscoveryDocumentUtil.getBqDiscoveryDocForJobDefinition(objectTypeId);

        String elementName = BigQueryDiscoveryDocumentUtil.getSchemaChild(BQ_SCHEMA_JOB);

        for (ObjectDefinitionRole role : roles) {
            definitions.add(new ObjectDefinition().withOutputType(role == OUTPUT ? ContentType.JSON : ContentType.NONE)
                    .withInputType(role == INPUT ? ContentType.JSON : ContentType.NONE).withElementName(elementName)
                    .withJsonSchema(discoveryDocument.toString()));
        }
        return new ObjectDefinitions().withDefinitions(definitions);
    }
}
