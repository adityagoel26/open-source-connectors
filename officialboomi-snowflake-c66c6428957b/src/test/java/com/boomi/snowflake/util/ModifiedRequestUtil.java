// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.util;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.testutil.SimpleTrackedData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public final class ModifiedRequestUtil {

    private ModifiedRequestUtil() {
        // Prevent Initialization
    }

    public static List<SimpleTrackedData> getData(List<?> inputs) {
        List<SimpleTrackedData> data = new ArrayList<>();
        int index = 0;
        Iterator<?> it = inputs.iterator();
        while (it.hasNext()) {
            Object input = it.next();
            SimpleTrackedData tmp = new SimpleTrackedData(index++, input, new HashMap<String, String>());
            data.add(tmp);
        }
        return data;
    }

    // Sonar issue: java:S1452
    @SuppressWarnings("unchecked")
    public static <T extends TrackedData> Iterable<T> getTrackedDataIter(ModifiedSimpleOperationResponse response,
            List<SimpleTrackedData> inputs) {
        inputs.forEach(response::addTrackedData);
        return (Iterable<T>) inputs;
    }
}
