//Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.workdayprism;

import com.boomi.connector.api.OperationContext;

/**
 * Represents a connection for Workday Prism operations.
 * This class extends {@link PrismConnection} and provides an instance of {@link OperationContext}.
 */
public class PrismOperationConnection extends PrismConnection<OperationContext>{

    /**
     * Constructs a new {@code PrismOperationConnection} with the given operation context.
     *
     * @param context the {@link OperationContext} associated with this connection.
     */
    public PrismOperationConnection(OperationContext context) {
        super(context);
    }

}
