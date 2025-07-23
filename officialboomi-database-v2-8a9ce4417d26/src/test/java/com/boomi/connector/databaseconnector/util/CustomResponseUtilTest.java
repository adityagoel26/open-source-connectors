// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.testutil.ResponseUtil;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * This class is a test class for the CustomResponseUtil.
 * It uses JUnit for testing and Mockito for mocking dependencies.
 */
public class CustomResponseUtilTest {

    private final static Logger logger = Logger.getLogger(CustomResponseUtilTest.class.getName());
    private final static TransactionCacheKey transactionCacheKey = new TransactionCacheKey(ResponseUtil.EXECUTION_ID, null);
    private final static String TRANSACTION_STATUS_ERROR_MESSAGE = "Transaction status is not valid";
    private final static String TRANSACTION_ID_ERROR_MESSAGE = "Transaction id is not valid";

    /**
     *  This method test if the transactionCacheKey values and transactionStatus are added to map properly
     */
    @Test
    public void getInProgressTransactionPropertiesTest() {

        Map<String, String> map = CustomResponseUtil.getInProgressTransactionProperties(transactionCacheKey);

        Assert.assertEquals(TRANSACTION_STATUS_ERROR_MESSAGE, TransactionConstants.TRANSACTION_IN_PROGRESS, map.get(TransactionConstants.TRANSACTION_STATUS));
        Assert.assertEquals(TRANSACTION_ID_ERROR_MESSAGE, transactionCacheKey.toString(), map.get(TransactionConstants.TRANSACTION_ID));
    }

    /**
     *  This method test if log messages are captured properly.
     */
    @Test
    public void logInProgressTransactionPropertiesTest() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        StreamHandler streamHandler = new StreamHandler(outputStream, new SimpleFormatter());
        logger.addHandler(streamHandler);

        CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, logger);

        streamHandler.flush();
        String[] lines = outputStream.toString().split(System.lineSeparator());

        Assert.assertEquals(TRANSACTION_ID_ERROR_MESSAGE, logger.getHandlers()[0].getLevel() + ": Transaction Id: " + transactionCacheKey, lines[1]);
        Assert.assertEquals(TRANSACTION_STATUS_ERROR_MESSAGE, logger.getHandlers()[0].getLevel() + ": Transaction Status: In Progress", lines[3]);

    }
}

