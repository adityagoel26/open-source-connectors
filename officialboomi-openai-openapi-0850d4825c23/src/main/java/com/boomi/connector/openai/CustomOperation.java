// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.openai;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.openapi.OpenAPIOperation;
import com.boomi.util.CollectionUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class CustomOperation extends OpenAPIOperation {

    private static final String CUSTOM_HEADERS_PROPERTY = "customHeaders";

    protected CustomOperation(CustomOperationConnection connection) {
        super(connection);
    }

    /**
     * Adds custom headers from Connection page.
     *
     * @return Key value pairs of all headers in the request
     */
    @Override
    protected Iterable<Map.Entry<String, String>> getHeaders(ObjectData data) {
        Iterable<Map.Entry<String, String>> originalHeaders = super.getHeaders(data);


        Map<String, String> customHeaders = getConnection().getContext().getConnectionProperties().getCustomProperties(CUSTOM_HEADERS_PROPERTY);
        Iterator<Map.Entry<String ,String>> originalHeaderIterator = originalHeaders.iterator();
        ArrayList<Map.Entry<String, String>> headerList = new ArrayList<>();

        //add api key header
        String apiKey = getContext().getConnectionProperties().getProperty("apiKey");
        if (apiKey != null && !apiKey.isEmpty()) {
            headerList.add(new AbstractMap.SimpleEntry<>("Authorization", "Bearer " + apiKey));
        }

        //add other custom headers
        while (originalHeaderIterator.hasNext()) {
            headerList.add(originalHeaderIterator.next());
        }
        if (!CollectionUtil.isEmpty(customHeaders)) {
            for (Map.Entry<String, String> entry : customHeaders.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                headerList.add(new AbstractMap.SimpleEntry<>(key, value));
            }
        }

        return headerList;
    }
}
