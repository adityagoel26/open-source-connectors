// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Expression;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.GroupingOperator;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HttpClientFactory;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.Args;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.splitter.JsonArraySplitter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.boomi.connector.hubspotcrm.util.HubspotcrmConstant;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of a search operation for the HubSpot CRM connector.
 * This class handles search queries against the HubSpot API with support for
 * filtering, pagination, and various search operators.
 */
public class HubspotcrmSearchOperation extends BaseQueryOperation {

    /**
     * @param conn The HubspotcrmOperationConnection instance
     */
    public HubspotcrmSearchOperation(HubspotcrmOperationConnection conn) {
        super(conn);
    }

    /**
     * Creates a new ObjectNode for building JSON requests.
     *
     * @return A new ObjectNode instance
     */
    private static ObjectNode createObjectNode() {
        return ExecutionUtils.getObjectMapper().createObjectNode();
    }

    /**
     * Executes a search operation for the HubspotCRM connector.
     * This method processes the query request, sends it to the HubspotCRM API,
     * and handles the response.
     *
     * @param queryRequest      The query request containing filter data and other query parameters.
     * @param operationResponse The operation response object to be populated with the query results.
     * @throws ConnectorException If an IO error occurs during the execution of the query.
     */
    @Override
    protected void executeQuery(QueryRequest queryRequest, OperationResponse operationResponse) {
        FilterData filterData = queryRequest.getFilter();
        List<Map.Entry<String, String>> headers = this.getConnection().getHeaders();
        InputStream inputPayload = null;

        try (CloseableHttpClient client = HttpClientFactory.createHttpClient()) {
            int limit = parseLimit(filterData);
            int numberOfIterations = calculateNumberOfIterations(limit);
            int after = HubspotcrmConstant.ZERO;

            for (int i = HubspotcrmConstant.ZERO; i <= numberOfIterations; i++) {
                int effectiveLimit = determineEffectiveLimit(limit, i, numberOfIterations);
                inputPayload = convertToInputStream(filterData, effectiveLimit, HubspotcrmConstant.QUERY_LIMIT * after);
                after++;

                int count = processHttpResponse(client, inputPayload, this.getConnection(), headers, operationResponse,
                        filterData);

                if (shouldBreakLoop(count, effectiveLimit, limit)) {
                    break;
                }
            }
        } catch (ConnectorException | IOException | NumberFormatException connectorException) {
            handleExecutionException(connectorException, filterData, operationResponse);
        } finally {
            IOUtil.closeQuietly(inputPayload);
        }
    }

    private static int parseLimit(FilterData filterData) {
        String limitString = filterData.getDynamicOperationProperties().getProperty(HubspotcrmConstant.LIMIT);
        Args.notEmpty(limitString, HubspotcrmConstant.MISSING_LIMIT_VALUE_ERROR);
        try {
            int limit = Integer.parseInt(limitString);
            if (limit > HubspotcrmConstant.MAX_QUERY_LIMIT) {
                throw new IllegalArgumentException(HubspotcrmConstant.EXCEEDS_MAX_LIMIT_ERROR);
            }
            return limit;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid limit value: " + limitString, e);
        }
    }

    private static int calculateNumberOfIterations(int limit) {
        if (limit <= HubspotcrmConstant.ZERO) {
            return HubspotcrmConstant.ONE;
        }
        return limit / HubspotcrmConstant.QUERY_LIMIT;
    }

    private static int determineEffectiveLimit(int limit, int currentIteration, int numberOfIterations) {
        return (currentIteration == numberOfIterations) ? (limit % HubspotcrmConstant.QUERY_LIMIT)
                : HubspotcrmConstant.QUERY_LIMIT;
    }

    private int processHttpResponse(CloseableHttpClient client, InputStream inputStream,
            HubspotcrmOperationConnection hubspotcrmOperationConnection, List<Map.Entry<String, String>> headers,
            OperationResponse operationResponse, FilterData filterData) throws IOException {
        try (CloseableHttpResponse response = executeRequest(client, inputStream, hubspotcrmOperationConnection,
                headers, filterData.getLogger())) {
            return handleResponse(operationResponse, response, filterData);
        }
    }

    private CloseableHttpResponse executeRequest(CloseableHttpClient client, InputStream inputStream,
            HubspotcrmOperationConnection hubspotcrmOperationConnection, List<Map.Entry<String, String>> headers,
            Logger logger) throws IOException {
        return ExecutionUtils.execute(HttpPost.METHOD_NAME, getPath(), client, inputStream,
                hubspotcrmOperationConnection.getUrl(), headers, logger);
    }

    private static boolean shouldBreakLoop(int count, int effectiveLimit, int limit) {
        return count < effectiveLimit || limit <= HubspotcrmConstant.ZERO;
    }

    private static void handleExecutionException(Exception e, FilterData filterData,
            OperationResponse operationResponse) {
        filterData.getLogger().log(Level.SEVERE, () -> "Failed executing search operation: " + e.getMessage());
        operationResponse.addErrorResult(filterData, OperationStatus.FAILURE,
                HubspotcrmConstant.CONNECTOR_STATUS_ERROR_CODE, HubspotcrmConstant.SEARCH_OPERATION_FAILED_MSG, e);
    }

    /**
     * Handles the HTTP response and processes the results.
     *
     * @param operationResponse The response object to populate
     * @param response          The HTTP response from the server
     * @param filterData        The filter data for the operation
     * @return The number of results processed
     * @throws IOException If there's an error reading the response
     */
    private static int handleResponse(OperationResponse operationResponse, CloseableHttpResponse response,
            FilterData filterData) throws IOException {

        if (isSuccessfulResponse(response)) {
            return processSuccessfulResponse(operationResponse, response, filterData);
        } else {
            handleFailureResponse(operationResponse, response, filterData);
            return HubspotcrmConstant.ZERO;
        }
    }

    /**
     * Checks if the HTTP response is successful (status code 200).
     *
     * @param response
     * @return
     */
    private static boolean isSuccessfulResponse(CloseableHttpResponse response) {
        return HttpStatus.SC_OK == response.getStatusLine().getStatusCode();
    }

    /**
     * Processes a successful response from the API.
     *
     * @param operationResponse The response object to populate
     * @param response          The HTTP response from the server
     * @param filterData        The filter data for the operation
     * @return The number of results processed
     * @throws IOException If there's an error reading the response
     */
    private static int processSuccessfulResponse(OperationResponse operationResponse, CloseableHttpResponse response,
            FilterData filterData) throws IOException {
        int count = HubspotcrmConstant.ZERO;
        try (JsonArraySplitter jsonArraySplitter = new JsonArraySplitter(response.getEntity().getContent(),
                HubspotcrmConstant.RESULTS)) {
            for (Payload payload : jsonArraySplitter) {
                ExecutionUtils.addSuccessResult(operationResponse, filterData, StringUtil.toString(HttpStatus.SC_OK),
                        StringUtil.toString(OperationStatus.SUCCESS), payload);
                count++;
            }
            operationResponse.finishPartialResult(filterData);
        }
        return count;
    }

    /**
     * Handles a failed response from the API.
     *
     * @param operationResponse The response object to populate
     * @param response          The HTTP response from the server
     * @param filterData        The filter data for the operation
     * @throws IOException If there's an error reading the response
     */
    private static void handleFailureResponse(OperationResponse operationResponse, CloseableHttpResponse response,
            FilterData filterData) throws IOException {
        operationResponse.addResult(filterData, OperationStatus.FAILURE, response.getStatusLine().getReasonPhrase(),
                StringUtil.toString(OperationStatus.FAILURE),
                ResponseUtil.toPayload(response.getEntity().getContent()));
    }

    /**
     * Converts filter data and pagination parameters into an InputStream containing JSON query parameters.
     * This method constructs a JSON structure that includes filtering, sorting, field selection,
     * and pagination parameters for the Hubspot CRM API query.
     * <p>Example JSON output:
     * <pre>
     * {
     *     "limit": 100,
     *     "after": 200,
     *     "properties": ["email", "firstname", "lastname"],
     *     "sorts": [
     *         {"propertyName": "createdate", "direction": "DESCENDING"}
     *     ],
     *     "filterGroups": [
     *         {
     *             "filters": [
     *                 {"propertyName": "email", "operator": "EQ", "value": "test@example.com"}
     *             ]
     *         }
     *     ]
     * }
     * </pre>
     */
    private InputStream convertToInputStream(FilterData filterData, Integer limit, Integer after) {
        ObjectNode rootNode = createObjectNode();
        try {
            // Build the query parameters
            addPaginationParameters(rootNode, limit, after);
            addSelectedFields(rootNode);
            addSortCriteria(rootNode, filterData.getFilter());
            addFilterExpression(rootNode, filterData.getFilter(), ExecutionUtils.getObjectMapper());
            // Convert to InputStream
            return new ByteArrayInputStream(
                    ExecutionUtils.getObjectMapper().writeValueAsString(rootNode).getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException jsonProcessingException) {
            throw new ConnectorException(HubspotcrmConstant.QUERY_PARAMETER_FAILED_MSG, jsonProcessingException);
        }
    }

    /**
     * Adds pagination parameters to the query JSON.
     *
     * @param rootNode The root JSON node
     * @param limit    Maximum number of records to return
     * @param after    Pagination offset
     */
    private static void addPaginationParameters(ObjectNode rootNode, Integer limit, Integer after) {
        if (limit != null && limit > HubspotcrmConstant.ZERO) {
            rootNode.put(HubspotcrmConstant.LIMIT, limit);
        }
        if (after != null && after > HubspotcrmConstant.ZERO) {
            rootNode.put(HubspotcrmConstant.AFTER, after);
        }
    }

    /**
     * Adds selected fields to the query JSON.
     *
     * @param rootNode The root JSON node
     */
    private void addSelectedFields(ObjectNode rootNode) {
        List<String> selectedFields = getContext().getSelectedFields();
        ArrayNode propertiesArray = rootNode.putArray(HubspotcrmConstant.PROPERTIES);
        selectedFields.stream().map(field -> field.split(HubspotcrmConstant.SLASH,
                HubspotcrmConstant.EXPECTED_AMOUNT_OF_ELEMENTS_IN_OBJECT_TYPE)).filter(
                parts -> parts.length > HubspotcrmConstant.ONE).map(parts -> parts[1]).forEach(propertiesArray::add);
    }

    /**
     * Adds filter expressions to the query JSON.
     *
     * @param rootNode     The root JSON node
     * @param queryFilter  Query filter containing filter expressions
     * @param objectMapper ObjectMapper instance for JSON processing
     */
    private static void addFilterExpression(ObjectNode rootNode, QueryFilter queryFilter, ObjectMapper objectMapper) {
        Expression expression = queryFilter != null ? queryFilter.getExpression() : null;
        if (expression != null) {
            handleFilter(expression, rootNode, objectMapper);
        }
    }

    /**
     * Handles the addition of sort fields to the query JSON structure.
     * Currently, supports single field sorting only.
     *
     * @param queryFilter The query filter containing sort specifications
     * @param rootNode    The root JSON node to add sort fields to
     * @throws ConnectorException if multiple sort fields are specified
     */
    private static void addSortCriteria(ObjectNode rootNode, QueryFilter queryFilter) {
        List<Sort> sortFields = validateSortFields(queryFilter.getSort());
        if (!sortFields.isEmpty()) {
            ArrayNode sortFieldsArray = rootNode.putArray(HubspotcrmConstant.SORTS);
            addSortField(sortFields.get(0), sortFieldsArray);
        }
    }

    /**
     * Validates the sort fields list to ensure only single field sorting.
     *
     * @param sortFields List of sort specifications
     * @return The validated list of sort fields
     * @throws ConnectorException if multiple sort fields are specified
     */
    private static List<Sort> validateSortFields(List<Sort> sortFields) {
        if (sortFields == null || sortFields.isEmpty()) {
            return Collections.emptyList();
        }
        if (sortFields.size() > HubspotcrmConstant.ONE) {
            throw new ConnectorException(HubspotcrmConstant.SORT_FIELD_SUPPORT_MSG);
        }
        return sortFields;
    }

    /**
     * Adds a single sort field specification to the sort fields array.
     *
     * @param sort            The sort specification to add
     * @param sortFieldsArray The array node to add the sort field to
     * @throws ConnectorException if the sort field is invalid
     */
    private static void addSortField(Sort sort, ArrayNode sortFieldsArray) {
        try {
            ObjectNode sortNode = sortFieldsArray.addObject();
            String propertyName = ExecutionUtils.extractPropertyName(sort.getProperty());
            sortNode.put(HubspotcrmConstant.PROPERTY_NAME, propertyName);
            sortNode.put(HubspotcrmConstant.DIRECTION, sort.getSortOrder());
        } catch (IllegalArgumentException illegalArgumentException) {
            throw new ConnectorException(HubspotcrmConstant.SORT_FIELD_SPECIFICATION_MSG, illegalArgumentException);
        }
    }

    /**
     * Handles the addition of filter expressions to the query JSON structure.
     *
     * @param expression   The filter expression to handle
     * @param rootNode     The root JSON node to add filter expressions to
     * @param objectMapper ObjectMapper instance for JSON processing
     */
    private static void handleFilter(Expression expression, ObjectNode rootNode, ObjectMapper objectMapper) {
        ArrayNode filterGroupsArray = rootNode.putArray(HubspotcrmConstant.FILTER_GROUPS);
        handleExpression(expression, filterGroupsArray, objectMapper);
    }

    /**
     * Handles the addition of filter expressions to the query JSON structure.
     *
     * @param expression        The filter expression to handle
     * @param filterGroupsArray The array node to add filter expressions to
     * @param objectMapper      ObjectMapper instance for JSON processing
     */
    private static void handleExpression(Expression expression, ArrayNode filterGroupsArray,
            ObjectMapper objectMapper) {
        if (expression instanceof SimpleExpression) {
            handleSimpleExpression((SimpleExpression) expression, filterGroupsArray, objectMapper);
        } else if (expression instanceof GroupingExpression) {
            handleGroupingExpression((GroupingExpression) expression, filterGroupsArray, objectMapper);
        }
    }

    /**
     * Handles the addition of a simple filter expression to the query JSON structure.
     *
     * @param filterGroupsArray The array node to add the filter expression to
     * @param objectMapper      ObjectMapper instance for JSON processing
     */
    private static void handleSimpleExpression(SimpleExpression expression, ArrayNode filterGroupsArray,
            ObjectMapper objectMapper) {
        ObjectNode groupNode = createObjectNode();
        ArrayNode filtersArray = addFilterNode(groupNode);
        ObjectNode filterNode = handleSimpleExpression(expression, objectMapper);
        filtersArray.add(filterNode);
        filterGroupsArray.add(groupNode);
    }

    /**
     * Adds a filter array node to the provided group node.
     *
     * @param groupNode The ObjectNode to which the filter array will be added.
     *                  This node represents a group of filtering criteria.
     * @return ArrayNode A new array node that can be used to add filter conditions.
     * Returns the array associated with the "filters" key in the group node.
     */
    private static ArrayNode addFilterNode(ObjectNode groupNode) {
        return groupNode.withArray(HubspotcrmConstant.FILTERS);
    }

    /**
     * Handles the addition of a grouping expression to the query JSON structure.
     *
     * @param groupingExpression The grouping expression to handle
     * @param filterGroupsArray  The array node to add the grouping expression to
     * @param objectMapper       ObjectMapper instance for JSON processing
     */
    private static void handleGroupingExpression(GroupingExpression groupingExpression, ArrayNode filterGroupsArray,
            ObjectMapper objectMapper) {
        GroupingOperator operator = groupingExpression.getOperator();

        if (operator == GroupingOperator.AND) {
            handleAndGroupingExpression(groupingExpression, filterGroupsArray, objectMapper);
        } else if (operator == GroupingOperator.OR) {
            handleOrGroupingExpression(groupingExpression, filterGroupsArray, objectMapper);
        }
    }

    /**
     * Handles the addition of an AND grouping expression to the query JSON structure.
     *
     * @param groupingExpression The AND grouping expression to handle
     * @param filterGroupsArray  The array node to add the AND grouping expression to
     * @param objectMapper       ObjectMapper instance for JSON processing
     */
    private static void handleAndGroupingExpression(GroupingExpression groupingExpression, ArrayNode filterGroupsArray,
            ObjectMapper objectMapper) {
        ObjectNode groupNode = createObjectNode();
        ArrayNode filtersArray = addFilterNode(groupNode);

        for (Expression nestedExpression : groupingExpression.getNestedExpressions()) {
            if (nestedExpression instanceof SimpleExpression) {
                ObjectNode filterNode = handleSimpleExpression((SimpleExpression) nestedExpression, objectMapper);
                filtersArray.add(filterNode);
            } else {
                // Recursively handle nested GroupingExpression
                handleExpression(nestedExpression, filterGroupsArray, objectMapper);
            }
        }
        // Only add the group node if it contains filters
        if (!filtersArray.isEmpty()) {
            filterGroupsArray.add(groupNode);
        }
    }

    /**
     * Handles the addition of an OR grouping expression to the query JSON structure.
     *
     * @param groupingExpression The OR grouping expression to handle
     * @param filterGroupsArray  The array node to add the OR grouping expression to
     * @param objectMapper       ObjectMapper instance for JSON processing
     */
    private static void handleOrGroupingExpression(GroupingExpression groupingExpression, ArrayNode filterGroupsArray,
            ObjectMapper objectMapper) {
        for (Expression nestedExpression : groupingExpression.getNestedExpressions()) {
            handleExpression(nestedExpression, filterGroupsArray, objectMapper);
        }
    }

    /**
     * Handles the processing of a simple expression and converts it into a JSON object node.
     * This method processes different types of operators (BETWEEN, IN, NOT_IN, HAS_PROPERTY,
     * NOT_HAS_PROPERTY) and their corresponding values to create a structured filter object.
     *
     * @param simpleExpression The simple expression to be processed containing the property,
     *                         operator, and arguments for the filter condition
     * @param objectMapper     The Jackson ObjectMapper instance used for creating JSON nodes
     * @return ObjectNode A JSON object node containing the processed expression with the following structure:
     * {
     * "propertyName": "field_name",
     * "operator": "operator_type",
     * "value": "value"  // For default cases
     * }
     */
    private static ObjectNode handleSimpleExpression(SimpleExpression simpleExpression, ObjectMapper objectMapper) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        String propertyName = ExecutionUtils.extractPropertyName(simpleExpression.getProperty());
        objectNode.put(HubspotcrmConstant.PROPERTY_NAME, propertyName);

        String operator = simpleExpression.getOperator();
        switch (operator) {
            case HubspotcrmConstant.BETWEEN:
                getBetweenObject(simpleExpression, objectNode);
                break;
            case HubspotcrmConstant.IN:
            case HubspotcrmConstant.NOT_IN:
                getInAndNotInObject(simpleExpression, objectNode);
                break;
            case HubspotcrmConstant.HAS_PROPERTY:
            case HubspotcrmConstant.NOT_HAS_PROPERTY:
                break;
            default:
                objectNode.put(HubspotcrmConstant.VALUE, simpleExpression.getArguments().get(0));
                break;
        }

        return objectNode.put(HubspotcrmConstant.OPERATOR, operator);
    }

    /**
     * Processes IN and NOT_IN operators by converting a comma-separated string of values
     * into an array node within the provided object node.
     *
     * @param simpleExpression The simple expression containing the arguments to be processed.
     *                         Expected to contain a comma-separated string as its first argument.
     * @param objectNode       The object node to which the processed array will be added under
     *                         the "values" key.
     */
    private static void getInAndNotInObject(SimpleExpression simpleExpression, ObjectNode objectNode) {
        String argument = validateArguments(simpleExpression);
        ArrayNode valuesArray = objectNode.putArray(HubspotcrmConstant.VALUES);
        Arrays.stream(argument.split(HubspotcrmConstant.COMMA)).map(String::trim).forEach(valuesArray::add);
    }

    /**
     * Processes a BETWEEN operator by splitting a comma-separated range value into lower and upper bounds.
     * The method extracts two values from a single argument string and adds them to the object node
     * as 'value' (lower bound) and 'highValue' (upper bound).
     *
     * @param simpleExpression The simple expression containing the range values.
     *                         Expected format of first argument: "lowerBound,upperBound"
     * @param objectNode       The object node to which the processed values will be added.
     *                         Will contain two fields: "value" (lower bound) and "highValue" (upper bound)
     * @throws ConnectorException if the argument string is not properly formatted
     *                            (missing comma or no value after comma)
     */
    private static void getBetweenObject(SimpleExpression simpleExpression, ObjectNode objectNode) {

        String argument = validateArguments(simpleExpression);
        int commaIndex = argument.indexOf(HubspotcrmConstant.COMMA);
        if (commaIndex == HubspotcrmConstant.NEGATIVE_ONE || commaIndex == argument.length() - HubspotcrmConstant.ONE) {
            throw new ConnectorException(HubspotcrmConstant.BETWEEN_VALUE_FORMAT_MSG);
        }

        objectNode.put(HubspotcrmConstant.VALUE, argument.substring(0, commaIndex).trim());
        objectNode.put(HubspotcrmConstant.HIGH_VALUE, argument.substring(commaIndex + HubspotcrmConstant.ONE).trim());
    }

    /**
     * Retrieves the HubSpot CRM operation connection.
     * This method overrides the parent class's getConnection method to provide
     * a more specific return type for HubSpot CRM operations.
     */
    @Override
    public HubspotcrmOperationConnection getConnection() {
        return (HubspotcrmOperationConnection) super.getConnection();
    }

    /**
     * Extracts the API path segment from the object type ID.
     * This method specifically handles paths for search operations.
     *
     * @return String The API path segment (e.g., "/crm/v3/objects/contacts/search")
     * <p>
     * Example:
     * Input from getContext().getObjectTypeId():
     * "POST::/crm/v3/objects/contacts/search"
     * ↓
     * Returns: "/crm/v3/objects/contacts/search"
     */

    private String getPath() {
        ///crm/v3/objects/contacts/search
        return getContext().getObjectTypeId().split("::")[1];
    }

    /**
     * Validates the arguments list and returns the first argument.
     *
     * @param simpleExpression The simple expression containing the arguments to be validated.
     * @return The first argument as a string.
     * @throws IllegalArgumentException if the arguments list is null or empty,
     *                                  or if the first argument is null.
     */
    private static String validateArguments(SimpleExpression simpleExpression) {
        // Validate arguments list
        List<String> arguments = simpleExpression.getArguments();
        if (arguments == null || arguments.isEmpty() || arguments.get(0).isEmpty()) {
            throw new IllegalArgumentException(HubspotcrmConstant.ARGUMENTS_EMPTY_ERROR);
        }
        return arguments.get(0).trim();
    }
}
