// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.discovery;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.GoogleBqObjectType;
import com.boomi.restlet.client.RequestUtil;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.restlet.data.Method;
import org.restlet.data.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class holds an instance of the Google Discovery document and supports methods that can be performed on the
 * discovery document instance.
 *
 * @author Rohan Jain
 */
public class BigQueryDiscoveryDocumentUtil {

    private static final String BQ_SCHEMAS = "schemas";
    private static final String BQ_SCHEMA_POINTER = "/" + BQ_SCHEMAS + "/%s";

    private static final Set<String> JOB_SELF_REF_SCHEMAS = CollectionUtil.asImmutableSet("QueryParameterType",
            "QueryParameterValue", "TableFieldSchema");

    private static final String ERROR_RETRIEVE_DISCOVERY = "Unable to retrieve discovery document.";

    private static final String SCHEMA_JOB_CONFIGURATION = "JobConfiguration";
    private static final String SCHEMA_JOB_CONFIGURATION_TABLE_COPY = "JobConfigurationTableCopy";
    private static final String FIELD_DESTINATION_EXPIRATION_TIME = "destinationExpirationTime";
    private static final String JSON_NON_STANDARD_TYPE_ANY = "any";

    private static final String SCHEMA_LOAD = "load";
    private static final String SCHEMA_QUERY = "query";
    private static final String SCHEMA_COPY = "copy";
    private static final String SCHEMA_EXTRACT = "extract";
    private static final String SCHEMA_JOB = "Job";
    private static final String SCHEMA_UPDATE_RESULT = "upsert_result";
    private static final String SCHEMA_DELETE = "delete";
    private static final String NODE_STATUS = "status";
    private static final String CONFIGURATION_SCHEMA = "configuration";
    private static final String LOAD_COPY_JOB_SCHEMA = "LoadCopyJob";
    private static final String LOAD_COPY_CONFIGURATION_SCHEMA = "LoadCopyConfiguration";

    private BigQueryDiscoveryDocumentUtil() {
    }

    /**
    * Add a Schema "upsertResult" to the Discovery Document response.
    * Gets a BigQuery discovery document with removed "Job" schemas Load and Query by calling
    * {@link BigQueryDiscoveryDocumentUtil#getBqDiscoveryDocForJobDefinition}.
    * Add to the response a new "upsertResult" Schema, inserted into the Schema "schemas" with new reference
    * to the schemas Job Load and Query, and a "status" schema.
    *
    * @return {@link JsonNode} with the discovery document.
    */
    public static JsonNode addUpsertResultSchemaToDiscoverDoc() {
        JsonNode discoveryDocument = getDiscoveryDocument();
        handleSelfReferencedSchemas(discoveryDocument);

        ObjectNode schemasObjectNode = (ObjectNode) discoveryDocument.path(BQ_SCHEMAS);
        addNodesToSchema(schemasObjectNode);

        removeOtherJobTypes(discoveryDocument, SCHEMA_QUERY);

        ObjectNode upsertResponse = schemasObjectNode.putObject(SCHEMA_UPDATE_RESULT);
        upsertResponse.put(JSONUtil.SCHEMA_TYPE, JSONUtil.NodeType.OBJECT.value());

        ObjectNode propertiesSchema = upsertResponse.putObject(JSONUtil.SCHEMA_PROPERTIES);
        ObjectNode loadSchema = propertiesSchema.putObject(SCHEMA_LOAD);

        loadSchema.put(JSONUtil.$REF, LOAD_COPY_JOB_SCHEMA);

        ObjectNode querySchema = propertiesSchema.putObject(SCHEMA_QUERY);
        querySchema.put(JSONUtil.$REF, SCHEMA_JOB);

        ObjectNode deleteSchema = propertiesSchema.putObject(SCHEMA_DELETE);
        deleteSchema.put(JSONUtil.SCHEMA_TYPE, JSONUtil.NodeType.OBJECT.value());
        ObjectNode deleteProperties = deleteSchema.putObject(JSONUtil.SCHEMA_PROPERTIES);

        ObjectNode statusNode = deleteProperties.putObject(NODE_STATUS);
        statusNode.put(JSONUtil.SCHEMA_TYPE, JSONUtil.NodeType.STRING.value());
        return discoveryDocument;
    }

    /**
     * Insert into the "schemas" Schema a copy of "Job" and "JobConfiguration" with deletes schemas "extract", "copy"
     * and "query", in order to have one job configuration (load)
     * @param schemasObjectNode
     */
    private static void addNodesToSchema(ObjectNode schemasObjectNode) {
        ObjectNode jobCopyNode = schemasObjectNode.path(SCHEMA_JOB).deepCopy();
        jobCopyNode.put(JSONUtil.SCHEMA_ID, LOAD_COPY_JOB_SCHEMA);

        ObjectNode jobConfigurationCopyNode = schemasObjectNode.path(SCHEMA_JOB_CONFIGURATION).deepCopy();
        jobConfigurationCopyNode.put(JSONUtil.SCHEMA_ID, LOAD_COPY_CONFIGURATION_SCHEMA);

        ((ObjectNode) jobCopyNode.path(JSONUtil.SCHEMA_PROPERTIES).path(CONFIGURATION_SCHEMA)).put(JSONUtil.$REF,
                LOAD_COPY_CONFIGURATION_SCHEMA);

        ObjectNode schemaPropertiesNode = (ObjectNode) jobConfigurationCopyNode.path(JSONUtil.SCHEMA_PROPERTIES);
        schemaPropertiesNode.remove(SCHEMA_EXTRACT);
        schemaPropertiesNode.remove(SCHEMA_COPY);
        schemaPropertiesNode.remove(SCHEMA_QUERY);

        schemasObjectNode.set(LOAD_COPY_CONFIGURATION_SCHEMA, jobConfigurationCopyNode);
        schemasObjectNode.set(LOAD_COPY_JOB_SCHEMA, jobCopyNode);
    }

    /**
     * Gets a BigQuery discovery document which can be used for a "Job" object definition of a particular job type. The
     * discovery document is retrieved and job types except the job type sent as a parameter are removed. The self
     * referenced schemas are present for a job configuration are removed as well.
     *
     * @param jobType
     * @return
     */
    public static JsonNode getBqDiscoveryDocForJobDefinition(String jobType) {
        JsonNode discoveryDocument = getDiscoveryDocument();
        removeOtherJobTypes(discoveryDocument, jobType);
        handleSelfReferencedSchemas(discoveryDocument);
        return discoveryDocument;
    }

    /**
     * Gets a BigQuery discovery document and replaces the self referenced schema names passed as a parameter to this
     * method by a singe nested level.
     *
     * @return
     */
    public static JsonNode getBqDiscoveryDocReplacingSelfReferences() {
        JsonNode discoveryDocument = getDiscoveryDocument();
        handleSelfReferencedSchemas(discoveryDocument);
        return discoveryDocument;
    }

    /**
     * Gets a new google big query discovery document by executing {@link RequestUtil#executeRequest(Request,
     * com.boomi.util.StreamUtil.Parser)}
     *
     * @return
     */
    private static JsonNode getDiscoveryDocument() {
        try {
            Request request = new Request(Method.GET, GoogleBqConstants.BIG_QUERY_DISCOVERY_REST_URL);
            return RequestUtil.executeRequest(request, JSONUtil.JSON_PARSER);
        } catch (IOException e) {
            throw new ConnectorException(ERROR_RETRIEVE_DISCOVERY, e);
        }
    }

    /**
     * This method would remove every job type configuration from "JobConfiguration" schema except for a given job type.
     * The input is a big query discovery document containing "schemas".
     *
     * @param document
     * @param jobType
     */
    private static void removeOtherJobTypes(JsonNode document, String jobType) {
        JsonNode schemas = document.path(BQ_SCHEMAS);

        JsonNode jobConfigProps = schemas.path(SCHEMA_JOB_CONFIGURATION);

        Set<String> supportedTypes = getJobTypes(jobConfigProps);

        if (!supportedTypes.remove(jobType.toLowerCase())) {
            throw new ConnectorException("Job type not found in discovery document");
        }

        if (jobConfigProps instanceof ObjectNode) {
            ((ObjectNode) jobConfigProps.path(JSONUtil.SCHEMA_PROPERTIES)).remove(supportedTypes);
        }

        if (GoogleBqObjectType.COPY.name().equalsIgnoreCase(jobType)) {
            fixDestinationExpirationTimeType(schemas);
        }
    }

    /**
     * BigQuery returns an "any" type for the <code>destinationExpirationTime</code> field, which causes an error during
     * profile processing hence the value is replaced by "string"
     */
    private static void fixDestinationExpirationTimeType(JsonNode schemas) {
        JsonNode typeNode = schemas.path(SCHEMA_JOB_CONFIGURATION_TABLE_COPY).path(JSONUtil.SCHEMA_PROPERTIES).path(
                FIELD_DESTINATION_EXPIRATION_TIME);
        if (JSON_NON_STANDARD_TYPE_ANY.equalsIgnoreCase(typeNode.path(JSONUtil.SCHEMA_TYPE).textValue())) {
            ((ObjectNode) typeNode).put(JSONUtil.SCHEMA_TYPE, JSONUtil.NodeType.STRING.value());
        }
    }

    /**
     * Returns a set of supported job types present in big query discovery document. Big Query jobs are identified in
     * schema if the JobConfiguration node contains a property for which a reference name starts with JobConfiguration
     * prefix.
     * <p>
     * Example - "copy": { "$ref": "JobConfigurationTableCopy", "description": "[Pick one] Copies a table." }
     *
     * @param jobConfiguration
     * @return
     */
    private static Set<String> getJobTypes(JsonNode jobConfiguration) {
        Set<String> jobTypes = new HashSet<>();

        for (Map.Entry<String, JsonNode> property : JSONUtil.fieldsIterable(
                jobConfiguration.path(JSONUtil.SCHEMA_PROPERTIES))) {
            String referenceName = property.getValue().path(JSONUtil.$REF).asText();
            if (StringUtil.startsWithIgnoreCase(referenceName, SCHEMA_JOB_CONFIGURATION)) {
                jobTypes.add(property.getKey());
            }
        }

        return jobTypes;
    }

    /**
     * Returns the path to the schema subnode with the specified name.
     *
     * @param child
     *         s String name of the "schema" subnode to locate.
     */
    public static String getSchemaChild(String child) {
        return String.format(BQ_SCHEMA_POINTER, child);
    }

    /**
     * Replace self referenced schemas in google discovery document by a single level nesting of the self referenced
     * schema. If a schema is not found in google discovery document it is ignored.
     *
     * @param discoveryDocument
     *         a {@link JsonNode} instance holding the nodes to replace
     */
    private static void handleSelfReferencedSchemas(JsonNode discoveryDocument) {
        for (String schema : JOB_SELF_REF_SCHEMAS) {
            JsonNode selfReferencedSchema = discoveryDocument.path(BQ_SCHEMAS).path(schema);
            if (selfReferencedSchema != null) {
                replaceSelfReferences(selfReferencedSchema);
            }
        }
    }

    /**
     * This method removes self references within the schema and adds the referenced schema by a single level.
     * <p>
     * It will first remove a self reference.
     * <p>
     * After removing the self reference the "type" field needs to be present to make the schema valid. If there is no
     * "type" field an object type field is added. Simple type properties present in the given selfReferencedSchema node
     * are added to this object. This node will be the leaf node in the self reference free version of schema.
     * <p>
     * Next step is to clone the self reference free node and add the clone node in place of self references.
     * <p>
     * Example usage - "arrayType": { "      $ref": "QueryParameterType", "description": "[Optional] The type of the
     * array's elements, if this is an array." },
     * <p>
     * In the above example if QueryParameterType is a self reference schema then schema "QueryParameterType" should be
     * sent as selfReferencedSchema attribute to this method.
     *
     * @param selfReferencedSchema
     */
    public static void replaceSelfReferences(JsonNode selfReferencedSchema) {

        List<JsonNode> selfRefNodes = new ArrayList<>();
        String selfRefSchemaId = selfReferencedSchema.path(JSONUtil.SCHEMA_ID).textValue();

        if (StringUtil.isEmpty(selfRefSchemaId)) {
            throw new ConnectorException("Schema in discovery document missing an id field");
        }

        loadSelfReferences(selfReferencedSchema, selfRefNodes, selfRefSchemaId);
        Map<String, JsonNode> simpleProps = getSimpleTypeProperties(selfReferencedSchema);

        for (JsonNode node : selfRefNodes) {
            //remove self reference
            ((ObjectNode) node).remove(JSONUtil.$REF);
            //need to make sure a type property is present otherwise
            //schema validation on platform throws an NPE
            if (node.path(JSONUtil.SCHEMA_TYPE).isMissingNode()) {
                addLeaf((ObjectNode) node, simpleProps);
            }
        }
        //clone the self reference free node
        ObjectNode cloneNode = selfReferencedSchema.deepCopy();
        cloneNode.remove(JSONUtil.SCHEMA_ID);
        //add the cloned self reference free node in place of previous self references
        for (JsonNode node : selfRefNodes) {
            ((ObjectNode) node).setAll(cloneNode);
        }
    }

    /**
     * Adds a leaf node to a given self referenced node. The leaf node would contain the properties provided in a given
     * property name -> {@link JsonNode} map.
     *
     * @param node
     * @param simpleProps
     */
    private static void addLeaf(ObjectNode node, Map<String, JsonNode> simpleProps) {
        //this node would be added as a leaf node when a clone is created
        node.put(JSONUtil.SCHEMA_TYPE, JSONUtil.NodeType.OBJECT.value());
        if (simpleProps.size() > 0) {
            //the leaf node will only contain simple type fields
            ObjectNode a = node.putObject(JSONUtil.SCHEMA_PROPERTIES);
            a.setAll(simpleProps);
        }
    }

    /**
     * Recursively traverses over the {@link JsonNode} to get all self referenced fields/properties contained within the
     * node
     *
     * @param node
     * @param list
     * @param refNode
     */
    private static void loadSelfReferences(JsonNode node, List<JsonNode> list, String refNode) {
        for (JsonNode field : node) {
            if (refNode.equals(field.path(JSONUtil.$REF).textValue())) {
                list.add(field);
            } else if (field.isObject() || field.isArray()) {
                loadSelfReferences(field, list, refNode);
            }
        }
    }

    /**
     * Returns a map of simple type properties present in a given node. Simple type properties include string, number,
     * integer, boolean. The map returned contains key as the property name and the value as the {@link JsonNode} for
     * that property.
     *
     * @param node
     * @return
     */
    private static Map<String, JsonNode> getSimpleTypeProperties(JsonNode node) {

        Map<String, JsonNode> simpleProps = new HashMap<>();
        for (Map.Entry<String, JsonNode> property : JSONUtil.fieldsIterable(node.path(JSONUtil.SCHEMA_PROPERTIES))) {
            JsonNode nodeValue = property.getValue();
            if (isSimpleTypeProperty(nodeValue)) {
                simpleProps.put(property.getKey(), nodeValue);
            }
        }
        return simpleProps;
    }

    /**
     * Returns true if the given property is of type string, number, integer, boolean or null.
     *
     * @param node
     * @return
     */
    private static boolean isSimpleTypeProperty(JsonNode node) {

        JsonNode type = node.path(JSONUtil.SCHEMA_TYPE);
        if (type.isMissingNode()) {
            return false;
        }
        String nodeType = node.path(JSONUtil.SCHEMA_TYPE).textValue();
        return JSONUtil.NodeType.STRING.value().equals(nodeType) || JSONUtil.NodeType.BOOLEAN.value().equals(nodeType)
                || JSONUtil.NodeType.INTEGER.value().equals(nodeType) || JSONUtil.NodeType.NUMBER.value().equals(
                nodeType) || JSONUtil.NodeType.NULL.value().equals(nodeType);
    }
}