// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.connector.api.JsonPayloadUtil;
import com.boomi.connector.api.Payload;
import com.boomi.connector.googlebq.operation.upsert.strategy.DeleteStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.LoadJobStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.QueryJobStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Helper class to facilitate the creation of the Payload used in the multiple step on Upsert operation
 */
public class PayloadResponseBuilder {

    private static final String ERROR_MESSAGE_TEMPLATE = "Error on step %s %s. ";
    private final ObjectNode _responseNode = JSONUtil.newObjectNode();
    private StringBuilder _errorMessage;
    private String _code;

    /**
     * Add a new entry to a {@link ObjectNode} response, using as field name the node name, and as value the result
     * content from the {@link BaseStrategyResult}. If the result is not success, and error is added calling method
     * {@link PayloadResponseBuilder#withResult(String, BaseStrategyResult)} using the result error message as value.
     *
     * @param nodeName
     * @param result
     */
    public void withResult(String nodeName, BaseStrategyResult result) {
        if (!result.isSuccess()) {
            addErrorMessage(nodeName, result.getErrorMessage());
        }
        _code = result.getCode();
        _responseNode.set(nodeName, result.getContent());
    }

    /**
     * Add a new entry,as an error, to a {@link ObjectNode} response, using as field name the node name, and the given
     * message as value.
     *
     * @param nodeName
     * @param message
     */
    public void withException(String nodeName, String message) {
        _responseNode.set(nodeName, buildErrorMessageNode(message));
        _code = StringUtil.EMPTY_STRING;
        addErrorMessage(nodeName, message);
    }

    /**
     * Create a {@link Payload} using as Nodes the response from the Strategies {@link LoadJobStrategy}, {@link
     * QueryJobStrategy} and {@link DeleteStrategy}
     *
     * @return Creates a new payload using the provided {@link ObjectNode}
     */
    public Payload toPayload() {
        return JsonPayloadUtil.toPayload(_responseNode);
    }

    /**
     * Return a {@link String} with an error message on which step fail on the operation.
     *
     * @return an error message.
     */
    public String getMessage() {
        return (_errorMessage != null) ? _errorMessage.toString() : StringUtil.EMPTY_STRING;
    }

    /**
     * Add to a templated message using as input the given message.
     *
     * @param message
     */
    private void addErrorMessage(String nodeName, String message) {
        if (_errorMessage == null) {
            _errorMessage = new StringBuilder();
        }
        _errorMessage.append(String.format(ERROR_MESSAGE_TEMPLATE, nodeName, message));
    }

    /**
     * Return last given code from the implemented strategies.
     *
     * @return given {@link String} code
     */
    public String getCode() {
        return _code;
    }

    /**
     * Creates an error {@link JsonNode} using as input a {@link String} message
     *
     * @param message
     * @return a new instance of {@link JsonNode} with a predefined message
     */
    private static JsonNode buildErrorMessageNode(String message) {
        ObjectNode node = JSONUtil.newObjectNode();
        node.put("errorMessage", message);
        return node;
    }
}
