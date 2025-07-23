// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.browse;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.operation.batch.Batch;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.util.CollectionUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * Common abstraction layer to provide Browser support to operations that list the tables retrieved from the Google
 * BigQuery API aObject Types generated from  resources
 * .
 *
 */
abstract class BaseTableListBrowser extends GoogleBqBrowser {
    private static final String ERROR_CANNOT_GENERATE = "Unable to generate object types";
    protected static final String ERROR_NO_RESOURCES = "Unable to find any tables";
    protected static final String NODE_SCHEMA = "schema";
    protected static final String NODE_TABLE_TYPE = "type";

    private final TableResource _tableResource;

    /**
     * Create a new instance of {@link BaseTableListBrowser}.
     *
     * @param conn
     *         {@link GoogleBqBaseConnection<BrowseContext>} encapsulating the context.
     * @param tableResource
     *         {@link TableResource} instance.
     */
    BaseTableListBrowser(GoogleBqBaseConnection<BrowseContext> conn, TableResource tableResource) {
        super(conn);
        _tableResource = tableResource;
    }

    /**
     * Create a new instance of {@link BaseTableListBrowser}.
     *
     * @param conn
     *         {@link GoogleBqBaseConnection<BrowseContext>} encapsulating the context.
     */
    BaseTableListBrowser(GoogleBqBaseConnection<BrowseContext> conn) {
        this(conn, new TableResource(conn));
    }

    /**
     * Builds a List of object types containing big query table names. The tables are listed based on browseProjectId
     * and datasetId browse properties set when performing browse operation. Big query table names are retrieved by
     * executing {@link TableResource#listTables(String, CollectionUtil.Filter)}. Object type id builds the suffix
     * url to perform {@link TableResource#getTable(String)} and
     * {@link com.boomi.connector.googlebq.resource.TableDataResource#insertAll(Batch)}
     * request url. The suffix contains datasetId and tableId
     *
     * @return a new {@link Set} instance with{@link ObjectType} items .
     */
    List<ObjectType> buildObjectTypes(CollectionUtil.Filter<JsonNode> filter) {
        String datasetId = getConnection().getDatasetId();

        try {
            SortedSet<String> resources = _tableResource.listTables(datasetId, filter);
            List<ObjectType> objectTypes = new ArrayList<>(resources.size());
            for (String resource : resources) {
                String id = String.format(GoogleBqConstants.DATASET_TABLES_URL_SUFFIX, datasetId, resource);
                objectTypes.add(new ObjectType().withId(id).withLabel(resource));
            }
            return objectTypes;
        } catch (Exception e) {
            throw new ConnectorException(ERROR_CANNOT_GENERATE, e);
        }
    }

    List<ObjectType> getTableObjectTypes() {
        return buildObjectTypes(new CollectionUtil.Filter<JsonNode>() {
            @Override
            public boolean accept(JsonNode resource) {
                return GoogleBqConstants.RESOURCE_TYPE_TABLE.equalsIgnoreCase(resource.path(NODE_TABLE_TYPE).asText());
            }
        });
    }
}
