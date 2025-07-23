// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.UUID;

public abstract class JsonJobFactory {

    private static final String CONFIGURATION_FIELD = "configuration";
    private static final String JOB_REFERENCE_FIELD = "jobReference";
    private static final String ENGINE_FIELD = "engine";
    private static final String LOCATION_FIELD = "location";
    private static final String JOB_ID_FIELD = "jobId";
    private static final String BIGQUERY_ATTR = "BIGQUERY";
    private static final String US_ATTR = "US";
    protected static final String PROJECT_ID_FIELD = "projectId";
    protected final String _projectId;

    JsonJobFactory(String projectId) {
        _projectId = projectId;
    }

    private JsonNode createJobReferenceNode() {
        ObjectNode jobReferenceNode = JSONUtil.newObjectNode();
        jobReferenceNode.put(ENGINE_FIELD, BIGQUERY_ATTR);
        jobReferenceNode.put(PROJECT_ID_FIELD, _projectId);
        jobReferenceNode.put(LOCATION_FIELD, US_ATTR);
        jobReferenceNode.put(JOB_ID_FIELD, UUID.randomUUID().toString());

        return jobReferenceNode;
    }

    /**
     * Create a full representation of a Job
     *
     * @return {@link JsonNode}
     */
    public JsonNode toJsonNode() {
        ObjectNode jobNode = JSONUtil.newObjectNode();
        jobNode.set(CONFIGURATION_FIELD, createConfigurationNode());
        jobNode.set(JOB_REFERENCE_FIELD, createJobReferenceNode());
        return jobNode;
    }

    /**
     * Creates a "configuration" node for the implementing Jobs, using as Parameters key-value the properties set in the
     * Operation.
     *
     * @return {@link JsonNode} with the operations properties as a JSON format.
     */
    public abstract JsonNode createConfigurationNode();
}
