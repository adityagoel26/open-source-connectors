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

public class SobjectModelExtractor implements Closeable {
    private static final Integer TOTAL_LIMIT = 1000000;
    private final JsonParser _parser;
    private final List<String> _targetValues;
    private final Map<String, String> _retValues;
    private int _curTotalLength;
    private String _fieldName;
    private boolean _inFields;
    private boolean _inChildren;

    public SobjectModelExtractor(InputStream inputStream, List<String> targetValues) throws IOException {
        _parser = JSONUtils.getJsonFactory().createParser(inputStream);
        _retValues = new HashMap<>();
        _curTotalLength = 0;
        _targetValues = Collections.unmodifiableList(targetValues);

        waitForArray();
    }

    private void waitForArray() throws IOException {
        do {
            _fieldName = _parser.nextFieldName();
            if ("childRelationships".equals(_fieldName)) {
                _inChildren = true;
                // skip start array
                _parser.nextToken();
                break;
            } else if ("fields".equals(_fieldName)) {
                _inFields = true;
                _inChildren = false;
                // skip start array
                _parser.nextToken();
                break;
            }
        } while (_parser.hasCurrentToken());
    }

    public boolean parseSObject() throws IOException {
        _retValues.clear();
        JsonToken token;
        int openObjectCount = 0;
        int openArrayCount = 0;
        do {
            token = _parser.nextToken();
            _fieldName = _parser.getCurrentName() == null ? _fieldName : _parser.getCurrentName();

            if (token == JsonToken.START_ARRAY) {
                ++openArrayCount;
            } else if (token == JsonToken.END_ARRAY) {
                --openArrayCount;
                if (openArrayCount == -1) {
                    // moved from Children to Fields
                    waitForArray();
                    openArrayCount = 0;
                    token = _parser.nextToken();
                    _fieldName = _parser.getCurrentName();
                }
            }

            if (token == JsonToken.START_OBJECT) {
                openObjectCount++;
            } else if (token == JsonToken.END_OBJECT) {
                openObjectCount--;
            } else if (_targetValues.contains(_fieldName)) {
                check(token);
            }
        } while (token != null && openObjectCount != 0);
        return token != null && !_retValues.isEmpty();
    }

    private void check(JsonToken token) throws IOException {
        if (token.id() > JsonToken.FIELD_NAME.id()) {
            // value token
            String val = _parser.getValueAsString();
            if (val != null && _curTotalLength + val.length() <= TOTAL_LIMIT) {
                _curTotalLength += val.length();
                if (!val.equals("null")) {
                    _retValues.put(_fieldName, val);
                }
            }
        }
    }

    public String getValue(String key) {
        return _retValues.get(key);
    }

    public boolean isFieldModel() {
        return _inFields;
    }

    public boolean isChildModel() {
        return _inChildren;
    }

    @Override
    public void close() throws IOException {
        _parser.close();
    }
}
