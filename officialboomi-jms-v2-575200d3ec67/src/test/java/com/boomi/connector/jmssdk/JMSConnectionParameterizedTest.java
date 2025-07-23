// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.testutil.ConnectorTestContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class JMSConnectionParameterizedTest {

    private final BrowseContext _context;
    private final ExpectedResult _expectedResult;

    /**
     * Create a test case to validate the instantiation of a {@link JMSConnection}.
     * <p>
     * Note: the description value is passed to the annotation {@link org.junit.runners.Parameterized.Parameters} in
     * order to provide a description of the test being executed, but is not used anywhere else in the code of this
     * class.
     *
     * @param context        to pass as an argument when instantiating the {@link JMSConnection}
     * @param expectedResult after instantiating the {@link JMSConnection}
     * @param description    describing the test case
     */
    public JMSConnectionParameterizedTest(BrowseContext context, ExpectedResult expectedResult, String description) {
        _context = context;
        _expectedResult = expectedResult;
    }

    /**
     * Build a list of Test Cases to be executed by this class. The first parameter is a pre configured {@link
     * BrowseContext}, the second parameter is an {@link ExpectedResult} constant indicating if the test case is
     * expected to finish successfully or throwing an exception. The third parameter is an String containing a
     * description for the test case.
     *
     * @return the list of test cases.
     */
    @Parameterized.Parameters(name = "{2}")
    public static Iterable<Object[]> testCases() {
        List<Object[]> testCases = new ArrayList<>();

        // a properly configured context for JMS 2.0 that shouldn't cause any errors when instantiating a JMSConnection
        String properlyConfiguredV2TestCase = "Properly configured context should pass";
        BrowseContext properlyConfiguredV2Context =
                new JMSTestContext.Builder().withGenericService().withPoolDisabled().withVersion2().build();

        // a properly configured context for JMS 1.1 that shouldn't cause any errors when instantiating a JMSConnection
        String properlyConfiguredV1TestCase = "Properly configured context should pass";
        BrowseContext properlyConfiguredV1Context =
                new JMSTestContext.Builder().withGenericService().withPoolDisabled().withVersion1().build();

        // a context with an invalid Version
        String wrongVersionTestCase = "Context with invalid version should fail";
        ConnectorTestContext wrongVersionContext =
                new JMSTestContext.Builder().withPoolDisabled().withGenericService().build();
        wrongVersionContext.addConnectionProperty("version", "V3_0");

        // a context with an invalid server type
        String wrongServerTypeTestCase = "Context with invalid server type should fail";
        ConnectorTestContext wrongServiceContext =
                new JMSTestContext.Builder().withPoolDisabled().withVersion2().build();
        wrongServiceContext.addConnectionProperty("server_type", "INVALID_SERVICE");

        // a context with invalid pool config
        String wrongPoolSettingsTestCase = "Context with invalid pool settings should fail";
        ConnectorTestContext wrongPoolSettingsContext =
                new JMSTestContext.Builder().withPoolEnabled().withVersion2().build();
        wrongPoolSettingsContext.addConnectionProperty("pool_maximum_connections", -1);


        testCases.add(
                new Object[] { properlyConfiguredV2Context, ExpectedResult.SUCCESS, properlyConfiguredV2TestCase });
        testCases.add(
                new Object[] { properlyConfiguredV1Context, ExpectedResult.SUCCESS, properlyConfiguredV1TestCase });
        testCases.add(new Object[] { wrongVersionContext, ExpectedResult.FAIL, wrongVersionTestCase });
        testCases.add(new Object[] { wrongServiceContext, ExpectedResult.FAIL, wrongServerTypeTestCase });
        testCases.add(new Object[] { wrongPoolSettingsContext, ExpectedResult.FAIL, wrongPoolSettingsTestCase });

        return Collections.unmodifiableList(testCases);
    }

    @Test
    public void jmsConnectionInstantiationTest() {
        boolean gotAnError = false;

        try {
            new JMSConnection<>(_context);
        } catch (Exception e) {
            gotAnError = true;
            if (_expectedResult == ExpectedResult.SUCCESS) {
                Assert.fail("Instantiation failed: " + e.getMessage());
            }
        }

        if ((_expectedResult == ExpectedResult.FAIL) && !gotAnError) {
            Assert.fail("Instantiation should have failed");
        }
    }

    private enum ExpectedResult {
        SUCCESS, FAIL
    }
}
