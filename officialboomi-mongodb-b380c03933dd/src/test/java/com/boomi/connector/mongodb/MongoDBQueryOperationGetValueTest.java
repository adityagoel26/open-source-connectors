// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.ProfileUtils;
import com.boomi.connector.mongodb.util.QueryOperationUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class MongoDBQueryOperationGetValueTest {

    private static final MongoDBConnectorConnection connectorConnection = mock(MongoDBConnectorConnection.class);
    private static final MongoDBConnectorQueryOperation mongoDBConnectorQueryOperation =
            new MongoDBConnectorQueryOperation(connectorConnection);
    private static final QueryOperationUtil _queryOperator = QueryOperationUtil.EQUALS;
    private final String expectedValue;
    private final String fieldValue;
    private final String inputData;
    private final ProfileUtils profile;

    public MongoDBQueryOperationGetValueTest(String fieldValue, String inputData, ProfileUtils profile) {
        expectedValue = inputData;
        this.fieldValue = fieldValue;
        this.inputData = inputData;
        this.profile = profile;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParametersForGetValue() {
        return parametersValueForGetValue();
    }

    private static Collection<Object[]> parametersValueForGetValue() {
        Collection<Object[]> params = new ArrayList<>();

        String field = "$oid/$oid";
        String stringValue = "1a2b3c4d5f1a2b3c4d5f1a2b";
        ProfileUtils profileUtilsString = new ProfileUtils("{\"$oid\": \"name\"}");
        params.add(new Object[] { field, stringValue, profileUtilsString });

        String fieldInteger = "$numberInt/$numberInt";
        String integerValue = "1";
        ProfileUtils profileUtilsInteger = new ProfileUtils("{\"$numberInt\": 1}");
        params.add(new Object[] { fieldInteger, integerValue, profileUtilsInteger });

        String fieldNumDecimal = "$numberDecimal/$numberDecimal";
        String decimalValue = "1.0";
        ProfileUtils profileUtilsNumDecimal = new ProfileUtils("{\"$numberDecimal\": 1.0}");
        params.add(new Object[] { fieldNumDecimal, decimalValue, profileUtilsNumDecimal });

        String fieldLongNumber = "$numberLong/$numberLong";
        String longNumberValue = "123456789";
        ProfileUtils profileUtilsLongNumber = new ProfileUtils("{\"$numberLong\": 123456789}");
        params.add(new Object[] { fieldLongNumber, longNumberValue, profileUtilsLongNumber });

        String fieldDate = "$date/$date";
        String dateValue = "2023-03-09T14:05:15.953Z";
        ProfileUtils profileUtilsDate = new ProfileUtils("{\"$date\": Thu Mar 09 14:05:15 IST 2023}");
        params.add(new Object[] { fieldDate, dateValue, profileUtilsDate });
        return params;
    }

    @Test
    public void testGetValue() throws MongoDBConnectException, ParseException {

        String actualValue = mongoDBConnectorQueryOperation.getValue(fieldValue, inputData, _queryOperator, profile)
                .toString();

        if ("$date/$date".equals(fieldValue)) {
            String expectedResult = new SimpleDateFormat(MongoDBConstants.TIMEMASK).parse(expectedValue).toString();
            assertEquals(expectedResult, actualValue);
        } else {
            assertEquals(expectedValue, actualValue);
        }
    }
}
