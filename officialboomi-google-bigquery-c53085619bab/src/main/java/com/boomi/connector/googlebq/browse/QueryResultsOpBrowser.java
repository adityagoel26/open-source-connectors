//Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.googlebq.GoogleBqObjectType;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.util.json.JSONUtil;

import java.util.Collection;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;

/**
 * Browser implementation to load {@link ObjectTypes} and {@link ObjectDefinitions} for
 * BigQuery GetQueryResults jobs.
 *
 */
public class QueryResultsOpBrowser extends GoogleBqBrowser {

    private static final String JOB_INPUT_RESOURCE = "job_input";

    /**
     * Create a new instance of {@link QueryResultsOpBrowser}.
     *
     * @param conn
     *         a {@link GoogleBqBaseConnection<BrowseContext>} instance.
     */
    public QueryResultsOpBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        super(conn);
    }

    /**
     * @return an {@link ObjectTypes} instance
     */
    @Override
    public ObjectTypes getObjectTypes() {
        return new ObjectTypes().withTypes(GoogleBqObjectType.QUERY.toObjectType());
    }

    /**
     * @return an {@link ObjectDefinitions} instance
     */
    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        ObjectDefinitions definitions = new ObjectDefinitions();
        for (ObjectDefinitionRole role : roles) {
            definitions.withDefinitions(new ObjectDefinition()
                    .withOutputType(role == OUTPUT ? ContentType.BINARY : ContentType.NONE)
                    .withInputType(role == INPUT ? ContentType.JSON : ContentType.NONE)
                    .withElementName(role == INPUT ? JSONUtil.FULL_DOCUMENT_JSON_POINTER : null)
                    .withJsonSchema(role == INPUT ? loadResourceSchema(JOB_INPUT_RESOURCE).toString() : null));
        }

        return definitions;
    }
}
