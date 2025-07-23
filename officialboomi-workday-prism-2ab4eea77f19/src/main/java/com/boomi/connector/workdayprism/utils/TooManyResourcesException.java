//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.ConnectorException;

/**
 * Exception that indicates that more resources than the Atom can handle were fetched from the service.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class TooManyResourcesException extends ConnectorException {

    private static final long serialVersionUID = 20180903L;

    /**
     * Creates a new TooManyResourcesException instance
     *
     * @param message
     *         a custom message for the exception
     */
    public TooManyResourcesException(String message) {
        super(message);
    }
}
