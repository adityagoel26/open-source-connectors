// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.util.JSONUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SobjectListExtractor implements Closeable {
    private static final Integer TOTAL_LIMIT = 1000000;
    private final JsonParser _parser;
    private final List<String> _targetValues;
    private final Map<String, String> _retValues;
    private int _curTotalLength;
    private String _fieldName;

    public SobjectListExtractor(InputStream inputStream, List<String> targetValues) throws IOException {
        _parser = JSONUtils.getJsonFactory().createParser(inputStream);
        _targetValues = Collections.unmodifiableList(targetValues);
        _retValues = new HashMap<>();
        _curTotalLength = 0;

        waitForSObjects();
    }

    private void waitForSObjects() throws IOException {
        do {
            _fieldName = _parser.nextFieldName();
        } while (_parser.hasCurrentToken() && !"sobjects".equals(_fieldName));
        // skip array start
        _parser.nextToken();
    }

    public boolean parseSObject() throws IOException {
        _retValues.clear();
        JsonToken token;
        int openCount = 0;
        do {
            token = _parser.nextToken();
            _fieldName = _parser.getCurrentName();
            if (token == JsonToken.START_OBJECT) {
                openCount++;
            } else if (token == JsonToken.END_OBJECT) {
                openCount--;
            } else if (_targetValues.contains(_fieldName)) {
                check(token);
            }
        } while (token != null && openCount != 0);
        return token != null && !_retValues.isEmpty();
    }

    private void check(JsonToken token) throws IOException {
        if (token.id() > JsonToken.FIELD_NAME.id()) {
            // value token
            String val = _parser.getValueAsString();
            if (val != null && _curTotalLength + val.length() <= TOTAL_LIMIT) {
                _curTotalLength += val.length();
                _retValues.put(_fieldName, val);
            }
        }
    }

    public String getValue(String key) {
        return _retValues.get(key);
    }

    @Override
    public void close() throws IOException {
        _parser.close();
    }
}
