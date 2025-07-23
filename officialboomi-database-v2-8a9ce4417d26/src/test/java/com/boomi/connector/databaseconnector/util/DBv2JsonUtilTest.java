package com.boomi.connector.databaseconnector.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Class to test {@link DBv2JsonUtil}
 */
public class DBv2JsonUtilTest {

    /**
     * Test {@link DBv2JsonUtil#getObjectMapper()}
     * @throws IOException
     * @throws SQLException
     */
    @Test
    public void testGetObjectMapper() throws IOException, SQLException {
        ObjectMapper objectMapper = DBv2JsonUtil.getObjectMapper();
        Assert.assertNotNull(objectMapper);
    }

    /**
     * Test {@link DBv2JsonUtil#getBigDecimalObjectMapper()}
     * @throws IOException
     * @throws SQLException
     */
    @Test
    public void testGetBigDecimalObjectMapper() throws IOException, SQLException {
        ObjectMapper objectMapper = DBv2JsonUtil.getBigDecimalObjectMapper();
        JsonNode   json       = objectMapper.readTree("{\"COLDEF\":999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}");
        JsonNode   fieldValue = json.get("COLDEF");
        BigDecimal val        = fieldValue.decimalValue();
        BigDecimal num        = val;
        assertEquals(new BigDecimal("999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"), num);
    }

    /**
     * Test {@link DBv2JsonUtil#getObjectWriter()}
     * @throws IOException
     * @throws SQLException
     */
    @Test
    public void testGetObjectWriter() throws IOException, SQLException {
        ObjectWriter writer = DBv2JsonUtil.getObjectWriter();
        Assert.assertNotNull(writer);
    }

    /**
     * Test {@link DBv2JsonUtil#getObjectReader()}
     * @throws IOException
     * @throws SQLException
     */
    @Test
    public void testGetObjectReader() throws IOException, SQLException {
        ObjectReader reader = DBv2JsonUtil.getObjectReader();
        Assert.assertNotNull(reader);
    }

}
