//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.connection;

import com.boomi.connector.api.OperationContext;

/**
 * Implementation of GoogleBqBaseConnection that receives an instance of OperationContext
 */
public class GoogleBqOperationConnection extends GoogleBqBaseConnection<OperationContext> {

    /**
     * Creates a new GoogleBqOperationConnection instance
     *
     * @param context
     *         a {@link OperationContext} instance.
     */
    public GoogleBqOperationConnection(OperationContext context) {
        super(context);
    }
}
