// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy.result;

import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.restlet.data.Response;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Concrete implementation of {@link BaseStrategyResult} to create an Error Response.
 */
public class ErrorResponseStrategyResult extends BaseStrategyResult {

    private static final Logger LOG = LogUtil.getLogger(ErrorResponseStrategyResult.class);
    private static final String ERROR_NODE = "error";
    private static final String MESSAGE_NODE = "message";
    private static final String CODE_NODE = "code";

    private ErrorResponseStrategyResult(JsonNode content, String errorMessage, String code) {
        super(content, false, errorMessage, code);
    }

    /**
     * Create a new instance of {@link ErrorResponseStrategyResult} using the given {@link Response}
     *
     * @param response
     * @return new instance of ErrorResponseStrategyResult
     */
    public static ErrorResponseStrategyResult create(Response response) {
        return create(response, ResponseUtil.getMessage(response));
    }

    /**
     * Create a new instance of {@link ErrorResponseStrategyResult} using the given {@link Response} and a default
     * message if response has not any
     *
     * @param response
     * @param defaultMessage
     * @return new instance of ErrorResponseStrategyResult
     */
    public static ErrorResponseStrategyResult create(Response response, String defaultMessage) {
        JsonNode payload = extractPayload(response, defaultMessage);
        String code = payload.path(ERROR_NODE).path(CODE_NODE).asText();
        String errorMessage = payload.path(ERROR_NODE).path(MESSAGE_NODE).asText(defaultMessage);

        return new ErrorResponseStrategyResult(payload, errorMessage, code);
    }

    private static JsonNode extractPayload(Response response, String defaultMessage) {
        JsonNode payload;
        try {
            payload = JsonResponseUtil.extractPayload(response);
        } catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
            payload = buildErrorMessageNode(defaultMessage);
        }
        return payload;
    }

    private static JsonNode buildErrorMessageNode(String message) {
        ObjectNode node = JSONUtil.newObjectNode();
        node.put("errorMessage", message);
        return node;
    }
}
