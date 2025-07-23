// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.operation.query;

import com.boomi.connector.api.PayloadMetadata;
import com.boomi.util.StringUtil;
import com.boomi.util.json.splitter.JsonSplitter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.InputStream;

public class VQLResponseSplitter extends JsonSplitter {

    private static final String RESPONSE_STATUS_PATH = "/responseStatus";

    private String _nextPageElementValue;
    private String _itemPath;
    private final String _nextPageElementPath;
    private final StringBuilder _pathPointer;
    private boolean _hasError;

    /**
     * we use regex to qualify a next page element but that doesn't allow for a qualifier
     * For example, the json below requires an xpath-like expression: /link/[relation=='next']/url
     * "link": [ {
     * "relation": "self",
     * "url": "http://hapi.fhir.org/baseR4/Patient"
     * }, {
     * "relation": "next",
     * "url": "http://hapi.fhir.org/baseR4?_getpages=4397d8e6-4cf0-47f2-b20d-80cbf6c8aa59&_getpagesoffset=20
     * &_count=20&_pretty=true&_bundletype=searchset"
     * } ],
     * Split documents at a specific path base on the itemPathRegEx path value
     * Also capture the next page link based a simple path value
     * nextPageElementPath is optional if offset/page size pagination is used
     *
     * @param inputStream
     * @param itemPath
     * @param nextPageElementPath
     * @param metadata
     * @throws IOException
     */
    public VQLResponseSplitter(InputStream inputStream, String itemPath, String nextPageElementPath,
            PayloadMetadata metadata) throws IOException {
        super(inputStream, metadata);
        this._nextPageElementPath = nextPageElementPath;

        this._itemPath = itemPath;
        _pathPointer = new StringBuilder();
    }

    //used for next URL or hasmore pagination
    public String getNextPageElementValue() {
        return _nextPageElementValue;
    }

    /**
     * We want to parse all the entries but grab the resource child object.
     * "entry": [ {
     * "fullUrl": "http://hapi.fhir.org/baseR4/Patient/1092472",
     * "resource": {
     * "resourceType": "Patient",
     *
     * @return
     * @throws IOException
     */
    @Override
    protected JsonToken findNextNodeStart() throws IOException {
        JsonParser jsonParser = this.getParser();
        JsonToken element = null;

        // /entry/*/resource will work but not sure about the end of line match with d+$ ... lose the $ when * in
        // middle of string?
        element = jsonParser.nextToken();

        while (element != null) {
            if (element == JsonToken.FIELD_NAME) {
                pushElement(jsonParser.getCurrentName());
            } else if (element == JsonToken.END_ARRAY) {
                popElement();
                popElement();
            }

            String name = jsonParser.getCurrentName();
            // SUCCESS and WARNING response status are expected to hold the content we need
            if (((element == JsonToken.VALUE_STRING) && _pathPointer.toString().contentEquals(RESPONSE_STATUS_PATH))
                    && (!"SUCCESS".contentEquals(jsonParser.getValueAsString())) && (!"WARNING".contentEquals(
                    jsonParser.getValueAsString()))) {
                _hasError = true;
                this._itemPath = "/errors/*";
            }

            if ((name != null && !StringUtil.isEmpty(_nextPageElementPath)) && _nextPageElementValue == null && (
                    (element == JsonToken.VALUE_STRING) && _pathPointer.toString().contentEquals(
                            _nextPageElementPath))) {
                _nextPageElementValue = jsonParser.getValueAsString();
            }
            if (element == JsonToken.START_OBJECT && _pathPointer.toString().contentEquals(_itemPath)) {
                return element;
            }
            if (element.name().startsWith("VALUE_") || element == JsonToken.END_OBJECT) {
                popElement();
            }
            if (element == JsonToken.START_ARRAY) {
                pushElement("*");
            }

            element = jsonParser.nextToken();
        }
        return null;
    }

    private void pushElement(String name) {
        _pathPointer.append("/");
        _pathPointer.append(name);
    }

    private void popElement() {
        int lastPos = _pathPointer.lastIndexOf("/");
        if (lastPos > -1) {
            _pathPointer.setLength(lastPos);
        }
    }

    public boolean hasError() {
        return _hasError;
    }
}
