// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.util;

import com.boomi.util.StringUtil;

import org.restlet.data.Response;

public class StatusUtil {

    private StatusUtil() {

    }

    /**
     * Extract the status code from the given Response.
     *
     * @param response
     * @return
     */
    public static String getStatus(Response response) {
        return (response.getStatus() != null) ? String.valueOf(response.getStatus().getCode())
                : StringUtil.EMPTY_STRING;
    }
}
