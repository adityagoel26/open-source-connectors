// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import java.io.InputStream;

public class BatchUpdatedResponse {

    private final String _errorMessage;
    private final InputStream _response;

    public BatchUpdatedResponse(String errorMessage) {
        this(StreamUtil.EMPTY_STREAM, errorMessage);
    }

    public BatchUpdatedResponse(InputStream response) {
        this(response, StringUtil.EMPTY_STRING);
    }

    private BatchUpdatedResponse(InputStream response, String errorMessage) {
        _response = response;
        _errorMessage = errorMessage;
    }

    public boolean hasErrors() {
        return StringUtil.isNotEmpty(_errorMessage);
    }

    public String getErrorMessage() {
        return _errorMessage;
    }

    public InputStream getResponse() {
        return _response;
    }
}
