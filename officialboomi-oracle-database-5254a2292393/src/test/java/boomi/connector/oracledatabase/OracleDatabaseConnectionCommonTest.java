// Copyright (c) 2023 Boomi, Inc.
package boomi.connector.oracledatabase;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Common Oracle Database Connection Tests
 */
@RunWith(Parameterized.class)
public class OracleDatabaseConnectionCommonTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Parameterized.Parameter()
    public String driverName;
    @Parameterized.Parameter(1)
    public Class<? extends Throwable> expectedException;
    @Parameterized.Parameter(2)
    public String expectedExceptionMessage;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> parameterizedData() {
        return Arrays.asList(new Object[][] {
                {"oracle.jdbc.driver.OracleDriver", null, null},
                {"oracle.jdbc.OracleDriver", null, null},
                {"com.invalid.driver.InvalidDriver", ConnectorException.class, "Failed Loading the class"},
                {"nonexistent.driver.NonExistentDriver", ConnectorException.class, "Failed Loading the class"},
                {"", ConnectorException.class, "Failed Loading the class"},
        });
    }

    /**
     * Tests to make sure that the driver is properly initialized when the connection pooling enabled
     */
    @Test
    public void testOracleDatabaseClassDriverLoadWhenConnectionPoolingEnabled() {

        BrowseContext browseContext = mock(BrowseContext.class);
        PropertyMap propertyMap = mock(PropertyMap.class);
        when(browseContext.getConnectionProperties()).thenReturn(propertyMap);
        when(propertyMap.getProperty(OracleDatabaseConstants.CLASSNAME, "")).thenReturn(driverName);
        OracleDatabaseConnection oracleDatabaseConstants = new OracleDatabaseConnection(browseContext);
        if (expectedException != null) {
            exceptionRule.expect(expectedException);
        }
        if (expectedExceptionMessage != null) {
            exceptionRule.expectMessage(expectedExceptionMessage);
        }
        assertNotNull(oracleDatabaseConstants.getSoloConnection());
    }
}