// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.util.JSONUtils;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JSONValueSafeExtractor implements Closeable {

    private static final Integer TOTAL_LIMIT = 1000000;
    private final ClassicHttpResponse _response;
    private final List<String> _targetValues;
    private final Map<String, String> _retValues;
    private final JsonParser _parser;
    private int _curTotalLength;

    public JSONValueSafeExtractor(ClassicHttpResponse response, List<String> targetValues) throws IOException {
        _response = response;
        _parser = JSONUtils.getJsonFactory().createParser(SalesforceResponseUtil.getContent(_response));
        _targetValues = Collections.unmodifiableList(targetValues);
        _retValues = new HashMap<>();
        _curTotalLength = 0;

        initialize();
    }

    private boolean moveCursorToNextToken() throws IOException {
        JsonToken token = _parser.nextToken();

        while (token != null) {
            if (token == JsonToken.FIELD_NAME && _targetValues.contains(_parser.getCurrentName())) {
                return true;
            }
            token = _parser.nextToken();
        }
        return false;
    }

    private void initialize() throws IOException {
        while (moveCursorToNextToken()) {
            String key = _parser.getCurrentName();
            JsonToken token = _parser.nextToken();
            if (token.id() > JsonToken.FIELD_NAME.id()) {
                // value token
                String val = _parser.getValueAsString();
                if (val != null && _curTotalLength + val.length() <= TOTAL_LIMIT) {
                    _curTotalLength += val.length();
                    _retValues.put(key, val);
                } else {
                    break;
                }
            }
        }
    }

    public boolean containsKey(String key) {
        return _retValues.containsKey(key);
    }

    public String getValue(String key) {
        return _retValues.get(key);
    }

    public Map<String, String> getValuesMap() {
        return Collections.unmodifiableMap(_retValues);
    }

    @Override
    public void close() throws IOException {
        _parser.close();
        _response.close();
    }
}
