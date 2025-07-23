// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.constants;

public class OperationTypeConstants {

    /**
     * The Constant DYNAMIC_INSERT.
     */
    public static final String DYNAMIC_INSERT = "Dynamic Insert";
    /**
     * The Constant STANDARD_INSERT.
     */
    public static final String STANDARD_INSERT = "Standard Insert";
    /**
     * The Constant DYNAMIC_GET.
     */
    public static final String DYNAMIC_GET = "Dynamic Get";
    /**
     * The Constant STANDARD_GET.
     */
    public static final String STANDARD_GET = "Standard Get";
    /**
     * The Constant STOREDPROCEDUREWRITE.
     */
    public static final String STOREDPROCEDUREWRITE = "STOREDPROCEDUREWRITE";
    /**
     * The Constant DYNAMIC_UPDATE.
     */
    public static final String DYNAMIC_UPDATE = "Dynamic Update";
    /**
     * The Constant STANDARD_UPDATE.
     */
    public static final String STANDARD_UPDATE = "Standard Update";
    /**
     * The Constant DYNAMIC_DELETE.
     */
    public static final String DYNAMIC_DELETE = "Dynamic Delete";
    /**
     * The Constant STANDARD_DELETE.
     */
    public static final String STANDARD_DELETE = "Standard Delete";
    /**
     * The Constant GET.
     */
    public static final String GET = "GET";
    /**
     * The Constant CREATE.
     */
    public static final String CREATE = "CREATE";
    /**
     * The Constant DELETE.
     */
    public static final String DELETE = "DELETE";

    /**
     * START_TRANSACTION operation type string literal.
     */
    public static final String START_TRANSACTION = "START_TRANSACTION";

    /**
     * COMMIT operation type string literal.
     */
    public static final String COMMIT_TRANSACTION = "COMMIT_TRANSACTION";

    /**
     * ROLLBACK operation type string literal.
     */
    public static final String ROLLBACK_TRANSACTION = "ROLLBACK_TRANSACTION";

    private OperationTypeConstants() {
        // No instances needed.
    }
}

