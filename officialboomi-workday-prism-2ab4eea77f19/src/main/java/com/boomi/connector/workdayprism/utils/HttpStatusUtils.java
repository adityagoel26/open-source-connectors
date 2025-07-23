//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.ConnectorException;

import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;

/**
 * Holds constants value that will be accessed from different classes.
 *
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class HttpStatusUtils {

    private static final String ERROR_STATUS_NOT_PRESENT = "status not present.";

    private HttpStatusUtils() {
    }

    /**
     * Validates if the request was successful or not based on it's response {@link StatusLine} code.
     *
     * @param status
     *         a {@link StatusLine} instance coming from the API response.
     * @throws ConnectorException
     *         when the status is <code>null</code>.
     */
    public static boolean isSuccess(StatusLine status) {
        validateStatus(status);

        int code = status.getStatusCode();
        return (code >= HttpStatus.SC_OK) && (code <= HttpStatus.SC_NO_CONTENT);
    }

    /**
     * Validates if the request was a not found or not based on it's response {@link StatusLine} code.
     *
     * @param status
     *         a {@link StatusLine} instance coming from the API response.
     * @throws ConnectorException
     *         when the status is <code>null</code>.
     */
    public static boolean isNotFound(StatusLine status) {
        validateStatus(status);
        return HttpStatus.SC_NOT_FOUND == status.getStatusCode();
    }

    private static void validateStatus(StatusLine status) {
        if (status == null) {
            throw new ConnectorException(ERROR_STATUS_NOT_PRESENT);
        }
    }
}
