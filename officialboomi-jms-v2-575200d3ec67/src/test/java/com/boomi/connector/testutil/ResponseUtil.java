// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.UpdateRequest;

import java.util.Iterator;

public final class ResponseUtil {

    private ResponseUtil() {
    }

    public static SimpleOperationResponse getResponse(Iterable<? extends SimpleTrackedData> documents) {
        SimpleOperationResponse response = new SimpleOperationResponse();
        for (SimpleTrackedData document : documents) {
            response.addTrackedData(document);
        }

        return response;
    }

    public static UpdateRequest toRequest(final Iterable<ObjectData> documents) {
        return new UpdateRequest() {
            @Override
            public Iterator<ObjectData> iterator() {
                return documents.iterator();
            }
        };
    }

    public static QueryRequest toRequest(final FilterData document) {
        return new QueryRequest() {
            @Override
            public FilterData getFilter() {
                return document;
            }
        };
    }
}
