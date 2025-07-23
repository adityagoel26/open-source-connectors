// Copyright (c) 2023 Boomi, Inc.
package com.boomi.snowflake.wrappers;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.snowflake.util.SnowflakeContextIT;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SnowflakeWrapper.class})
public class SnowflakeWrapperTest {

    SnowflakeWrapper snowflakeWrapper;

    @Before
    public void setup() {
        SnowflakeContextIT testContext = new SnowflakeContextIT(OperationType.QUERY,"query");
        SnowflakeConnection connection = new SnowflakeConnection(testContext);
        ConnectionProperties properties = new ConnectionProperties(connection,
                new MutablePropertyMap(), "TEST_TABLE", Logger.getAnonymousLogger());
        ConnectionTimeFormat connectionTimeFormat = Mockito.mock(ConnectionTimeFormat.class);
        snowflakeWrapper = new SnowflakeWrapper(properties.getConnectionGetter(),connectionTimeFormat,
                properties.getLogger(), properties.getTableName());
    }

    @Test
    public void testSqlConstructWhereClause() {
        List<Map<String,String >> data = new ArrayList<>();
        data.add(map("TIMESTAMP",">=","2023-10-10 00:00:00"));
        data.add(map("TIMESTAMP","<=","2023-11-30 00:01:00"));

        String result = snowflakeWrapper.sqlConstructQueryWhereClause(data);
        assertTrue(result.contains("\"TIMESTAMP\">="));
        assertTrue(result.contains("\"TIMESTAMP\"<="));
        assertTrue(result.contains("AND"));
    }

    private Map<String, String> map(String property, String operator, String value) {
        Map<String,String> map = new HashMap<>();
        map.put("property",property);
        map.put("operator",operator);
        map.put("arguments",value);
        return map;
    }

    /**
     * This method sets boolean values in a PreparedStatement using reflection.
     *
     * @throws Exception if the method throws an exception
     */
    @Test
    public void setBooleanValuesTest() throws Exception {
        PreparedStatement statement = Mockito.mock(PreparedStatement.class);

        Whitebox.invokeMethod(snowflakeWrapper, "setBooleanValues", "0.0", statement, 1, "testKey");
        Mockito.verify(statement, Mockito.times(1)).setBoolean(1, false);

        Whitebox.invokeMethod(snowflakeWrapper, "setBooleanValues", "1.2", statement, 2, "testKey");
        Mockito.verify(statement, Mockito.times(1)).setBoolean(2, true);
    }
}