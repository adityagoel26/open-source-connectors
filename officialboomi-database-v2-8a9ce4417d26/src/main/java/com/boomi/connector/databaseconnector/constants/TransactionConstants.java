// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.constants;

/**
 * The Class TransactionConstants
 */
public class TransactionConstants {

    /**
     * The Constant TRANSACTION_ID
     */
    public static final String TRANSACTION_ID = "transactionId";

    /*
     * The Constant TRANSACTION_STATUS
     */
    public static final String TRANSACTION_STATUS = "transactionStatus";

    /**
     * The Constant TRANSACTION_STARTED
     */
    public static final String TRANSACTION_STARTED = "Started";

    /**
     * The Constant TRANSACTION_COMMITTED
     */
    public static final String TRANSACTION_COMMITTED = "Committed";

    /**
     * The Constant TRANSACTION_IN_PROGRESS
     */
    public static final String TRANSACTION_IN_PROGRESS = "In Progress";

    /**
     * The Constant TRANSACTION_ROLLED_BACK
     */
    public static final String TRANSACTION_ROLLED_BACK = "Rolled Back";

    /**
     * The Constant TRANSACTION_STARTED_LOG_MESSAGE
     */
    public static final String TRANSACTION_LOG_MESSAGE = "Transaction ID is ";

    /**
     * String constant for Join Transaction flag label
     */
    public static final String JOIN_TRANSACTION = "joinTransaction";

    /**
     * String constant for Error Code ERR_ONGOING_TRAN
     */
    public static final String ERR_ONGOING_TRAN = "ERR_ONGOING_TRAN";

    /**
     * String constant for Error Code ERR_NO_EXISTING_TRAN
     */
    public static final String ERR_NO_EXISTING_TRAN = "ERR_NO_EXISTING_TRAN";

    /**
     * String constant for Error Code ERR_CACHE_KEY_NOT_FOUND
     */
    public static final String ERR_CACHE_KEY_NOT_FOUND = "ERR_CACHE_KEY_NOT_FOUND";


    /**
     * No instances needed.
     */
    private TransactionConstants() {}
}
