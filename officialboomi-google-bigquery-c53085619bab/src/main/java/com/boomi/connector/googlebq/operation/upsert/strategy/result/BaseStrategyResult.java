// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy.result;

import com.boomi.connector.googlebq.operation.job.Job;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Base strategy in charge of construct a Payload response using as input a {@link JsonNode},a {@link String} error
 * message and code if is given, and {@link Boolean} with the service status.
 */
public class BaseStrategyResult {

    private final JsonNode _content;
    private final boolean _isSuccess;
    private final String _errorMessage;
    private final String _code;

    /**
     * Class constructor specifying the content, success, error message and code if any.
     *
     * @param content
     * @param isSuccess
     * @param errorMessage
     * @param code
     */
    BaseStrategyResult(JsonNode content, boolean isSuccess, String errorMessage, String code) {
        _content = content;
        _isSuccess = isSuccess;
        _errorMessage = errorMessage;
        _code = code;
    }

    /**
     * Create a new instance of {@link BaseStrategyResult} with the given {@link Job}
     *
     * @param job
     * @return new instance of BaseStrategyResult
     */
    public static BaseStrategyResult createSuccessResult(Job job) {
        return new BaseStrategyResult(job.getJob(), job.isDoneAndSuccessful(), job.getErrorResultMessage(),
                job.getCode());
    }

    /**
     * Create a new instance of {@link BaseStrategyResult} with the given {@link JsonNode} as content.
     *
     * @param node
     * @param code
     * @return new instance of BaseStrategyResult
     */
    public static BaseStrategyResult createDeleteResult(JsonNode node, String code) {
        return new BaseStrategyResult(node, true, StringUtil.EMPTY_STRING, code);
    }

    /**
     * Return the given {@link JsonNode} with the payload response from the called service. Or and specific error node
     * if the service fails.
     *
     * @return a {@link JsonNode} with the given content.
     */
    public JsonNode getContent() {
        return _content;
    }

    /**
     * Return true if the service called response is successful, otherwise, return false.
     *
     * @return
     */
    public boolean isSuccess() {
        return _isSuccess;
    }

    /**
     * Return the given code from the service response. If no code is on the response, an empty {@link String} is
     * returned.
     *
     * @return
     */
    public String getCode() {
        return _code;
    }

    /**
     * Return the given code from the service response. If no code is on the response, an empty {@link String} is
     * returned.
     *
     * @return
     */
    public String getErrorMessage() {
        return _errorMessage;
    }
}
