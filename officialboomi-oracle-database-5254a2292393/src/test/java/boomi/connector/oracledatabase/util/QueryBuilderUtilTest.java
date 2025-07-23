// Copyright (c) 2025 Boomi, LP
package boomi.connector.oracledatabase.util;

import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.BLOB;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.BOOLEAN;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.DATE;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.DOUBLE;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.FLOAT;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.INTEGER;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.LONG;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.NVARCHAR;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.STRING;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.TIME;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.TIMESTAMP;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class QueryBuilderUtilTest {

    private static final String INPUT =
            "{\r\n" + "\"id\":\"123\",\r\n" + " \"double\":\"7.8368498\",\r\n" + "\"salary\":\"123.00\",\r\n"
                    + " \"Long\":\"1234\",\r\n" + "\"date\":\"2023-04-06\",\r\n" + " \"name\":\"abc\",\r\n"
                    + "\"dob\":\"2019-12-09\",\r\n" + "\"blob\":\"Hello\",\r\n" + "\"isQualified\":true,\r\n"
                    + " \"lapTime\":\"05:33:30\"\r\n" + "\r\n" + " }";
    private static final String INPUT_JSON_NULL_VALUE =
            "{\"id\":30,\"sno\":null,\"name\":null,\"startDate\":null,\"startTime\":null,\"distance\":null,"
                    + "\"salary\":null," + "\"bonus\":null,\"blob\":null,\"isQualified\":null,\"result\":\"Success\"}";
    private static final String START_TIME = "startTime";
    private static final String TIME_STAMP_REF = "05:33:30";
    private static final String SAMPLE_DATE = "2023-04-06";
    private static final float SAMPLE_FLOAT = 123.00f;
    private static final double SAMPLE_DOUBLE = 123.0d;
    private static final long SAMPLE_LONG = 1234L;
    private final PreparedStatement _preparedStatement = mock(PreparedStatement.class);
    private final Map<String, String> _dataTypes = new HashMap<>();
    private final BigDecimal _bigDecimal = new BigDecimal(123);
    private static final int count = 1;
    private final ObjectMapper _objectMapper = new ObjectMapper();

    @Test
    public void testCheckDataTypeIsIntegerWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("sno");
        _dataTypes.put("integer", INTEGER);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, INTEGER, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.INTEGER);
    }

    @Test
    public void testCheckDataTypeIsInteger() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("id");
        _dataTypes.put("integer", INTEGER);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, INTEGER, fieldName, count);
        verify(_preparedStatement, times(1)).setBigDecimal(1, _bigDecimal);
    }

    @Test
    public void testCheckDataTypeIsDateWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("startDate");
        _dataTypes.put("date", DATE);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, DATE, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.DATE);
    }

    @Test
    public void testCheckDataTypeIsDate() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("date");
        _dataTypes.put("date", DATE);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, DATE, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, SAMPLE_DATE);
    }

    @Test
    public void testCheckDataTypeIsStringWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("name");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.VARCHAR);
    }

    @Test
    public void testCheckDataTypeIsString() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("name");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "abc");
    }

    @Test
    public void testCheckDataTypeIsNvarcharWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("name");
        _dataTypes.put("nvarchar", NVARCHAR);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, NVARCHAR, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.NVARCHAR);
    }

    @Test
    public void testCheckDataTypeIsNvarchar() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("name");
        _dataTypes.put("nvarchar", NVARCHAR);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, NVARCHAR, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "abc");
    }

    @Test
    public void testCheckDataTypeIsTimeWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get(START_TIME);
        _dataTypes.put("time", TIME);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, TIME, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.TIME);
    }

    @Test
    public void testCheckDataTypeIsTime() throws SQLException, IOException {
        Time _sqlTime = new Time(05, 33, 30);
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("lapTime");
        _dataTypes.put("time", TIME);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, TIME, fieldName, count);
        verify(_preparedStatement, times(1)).setTime(1, Time.valueOf(_sqlTime.toString()));
    }

    @Test
    public void testCheckDataTypeIsBooleanWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("isQualified");
        _dataTypes.put("boolean", BOOLEAN);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, BOOLEAN, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.BOOLEAN);
    }

    @Test
    public void testCheckDataTypeIsBoolean() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("isQualified");
        _dataTypes.put("boolean", BOOLEAN);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, BOOLEAN, fieldName, count);
        verify(_preparedStatement, times(1)).setBoolean(1, true);
    }

    @Test
    public void testCheckDataTypeIsLongWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("distance");
        _dataTypes.put("long", LONG);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, LONG, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.BIGINT);
    }

    @Test
    public void testCheckDataTypeIsLong() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("Long");
        _dataTypes.put("long", LONG);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, LONG, fieldName, count);
        verify(_preparedStatement, times(1)).setLong(1, SAMPLE_LONG);
    }

    @Test
    public void testCheckDataTypeIsFloatWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("salary");
        _dataTypes.put("float", FLOAT);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, FLOAT, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.FLOAT);
    }

    @Test
    public void testCheckDataTypeIsFloat() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("salary");
        _dataTypes.put("float", FLOAT);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, FLOAT, fieldName, count);
        verify(_preparedStatement, times(1)).setFloat(1, SAMPLE_FLOAT);
    }

    @Test
    public void testCheckDataTypeIsDoubleWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("bonus");
        _dataTypes.put("double", DOUBLE);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, DOUBLE, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.DECIMAL);
    }

    @Test
    public void testCheckDataTypeIsDouble() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("salary");
        _dataTypes.put("double", DOUBLE);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, DOUBLE, fieldName, count);
        verify(_preparedStatement, times(1)).setDouble(1, SAMPLE_DOUBLE);
    }

    @Test
    public void testCheckDataTypeIsBlobWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get("blob");
        _dataTypes.put("BLOB", BLOB);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, BLOB, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.BLOB);
    }

    @Test
    public void testCheckDataTypeBlob() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("blob");
        _dataTypes.put("BLOB", BLOB);
        ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, BLOB, fieldName, count);
        verify(_preparedStatement, times(1)).setBlob(eq(1), inputStreamCaptor.capture());
    }

    @Test
    public void testCheckDataTypeIsTimestampWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT_JSON_NULL_VALUE);
        JsonNode fieldName = jsonNode.get(START_TIME);
        _dataTypes.put("timestamp", TIMESTAMP);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, TIMESTAMP, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.TIMESTAMP);
    }

    @Test
    public void testCheckDataTypeIsTimestamp() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree(INPUT);
        JsonNode fieldName = jsonNode.get("lapTime");
        _dataTypes.put("timestamp", TIMESTAMP);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, TIMESTAMP, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, TIME_STAMP_REF);
    }

    /**
     * Test case for a String with a Tab character (\t). This test ensures that
     * the tab character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithTab() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\tWorld\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\tWorld");
    }

    /**
     * Test case for a String with a Newline character (\n). This test ensures that
     * the newline character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithNewline() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\nWorld\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\nWorld");
    }

    /**
     * Test case for a String with a Carriage Return character (\r). This test ensures that
     * the carriage return character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithCarriageReturn() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\rWorld\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\rWorld");
    }

    /**
     * Test case for a String with a Form Feed character (\f). This test ensures that
     * the form feed character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithFormFeed() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\fWorld\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\fWorld");
    }

    /**
     * Test case for a String with a Backspace character (\b). This test ensures that
     * the backspace character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithBackspace() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\bWorld\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\bWorld");
    }

    /**
     * Test case for a String with a Double Quote (\"). This test ensures that
     * the double quote character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithDoubleQuote() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\\"World\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\"World");
    }

    /**
     * Test case for a String with a Backslash (\\). This test ensures that
     * the backslash character in the string is correctly handled and passed to the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithBackslash() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":\"Hello\\\\World\"}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1, "Hello\\World");
    }

    /**
     * Test case for a String with a null value. This test ensures that when a null
     * value is passed, it is handled by setting the parameter to NULL in the
     * PreparedStatement.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithNull() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":null}");
        JsonNode fieldName = jsonNode.get("field");
        _dataTypes.put("string", STRING);

        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.VARCHAR);
    }

    /**
     * Test case for a missing field (fieldName is null). This test ensures that
     * when the field is missing, the PreparedStatement should not attempt to set
     * a value for that field.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithMissingField() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{ }");
        JsonNode fieldName = jsonNode.get("field");

        _dataTypes.put("string", STRING);
        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(0)).setString(eq(1), anyString());
    }

    /**
     * Test case for a field explicitly set to null. This test ensures that when
     * a field is explicitly set to null, the PreparedStatement correctly sets the
     * parameter to NULL.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs during JSON parsing
     */
    @Test
    public void testStringWithExplicitNullField() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"field\":null}"); // Explicit null field
        JsonNode fieldName = jsonNode.get("field"); // Should be explicitly null

        _dataTypes.put("string", STRING);
        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setNull(1, Types.VARCHAR); // Ensure setNull is called
    }

    /**
     * Test case to verify handling of a nested JSON structure
     * containing escaped special characters.
     *
     * <p>This test ensures that:
     * - The JSON parser correctly extracts the nested field.
     * - Escaped characters are properly interpreted.
     * - The extracted value is correctly processed and passed to SQL.
     * - The PreparedStatement receives the expected string value.
     *
     * @throws SQLException if an error occurs while interacting with the database.
     * @throws IOException if an error occurs while parsing the JSON input.
     */
    @Test
    public void testNestedJsonWithEscapedCharacters() throws SQLException, IOException {
        JsonNode jsonNode = _objectMapper.readTree("{\"user\": {\"field\": \"Hello\\tWorld\\bNew\\nLine\\" +
                "rCarriage\\fFormFeed\'Quote\\\"DoubleQuote\\\\Backslash\"}}");
        JsonNode fieldName = jsonNode.get("user").get("field"); // Extract nested field
        _dataTypes.put("string", STRING);
        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);
        verify(_preparedStatement, times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Test case to verify handling of an XML input containing escaped special characters.
     *
     * <p>This test ensures that:
     * - The XML parser correctly extracts the field value.
     * - Escaped characters in the XML field are properly interpreted.
     * - The extracted value is correctly converted to JSON format.
     * - The PreparedStatement receives the expected string value.
     *
     * @throws Exception if an error occurs during XML parsing or JSON processing.
     */
    @Test
    public void testCheckDataTypeWithEscapedXMLCharacters() throws Exception {
        // XML input with escaped characters
        String xmlInput = "<root><field>Hello\tWorld\nNewLine\rCarriageReturn\'Quote\"DoubleQuote\\Backslash</field></root>";
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(new InputSource(new StringReader(xmlInput)));

        // Extract the XML field value
        String extractedValue = doc.getElementsByTagName("field").item(0).getTextContent();

        // Properly escape extracted value for JSON
        String jsonInput = "{ \"xmlField\": " + _objectMapper.writeValueAsString(extractedValue) + " }";
        JsonNode fieldName = _objectMapper.readTree(jsonInput).get("xmlField");
        _dataTypes.put("string", STRING);
        QueryBuilderUtil.checkDataType(_preparedStatement, _dataTypes, STRING, fieldName, count);

        // Verify that the escaped characters are passed as expected
        verify(_preparedStatement, times(1)).setString(1, extractedValue);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithNullPattern() {
        String result = QueryBuilderUtil.replaceWithSqlWildCards(null);
        Assert.assertNull(result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithEmptyPattern() {
        String pattern = "";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertNull(result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithNoWildcards() {
        String pattern = "someString";
        String expected = "someString";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSingleWildcard() {
        String pattern = "*";
        String expected = "%";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithLeadingWildcard() {
        String pattern = "*someString";
        String expected = "%someString";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSingleWildcardPercentage() {
        String pattern = "%";
        String expected = "%";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithTrailingWildcard() {
        String pattern = "someString*";
        String expected = "someString%";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithWildcardInMiddle() {
        String pattern = "some*String";
        String expected = "some%String";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithMultipleWildcards() {
        String pattern = "some*String*With*Wildcards";
        String expected = "some%String%With%Wildcards";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSpacePattern() {
        String pattern = " ";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertNull(result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSpecialCharacters() {
        String pattern = "!@#$%^&*()";
        String expected = "!@#$%^&%()";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithTab() {
        String pattern = "abc\tdef";
        String expected = "abc\tdef";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithNewLineCharacters() {
        String pattern = "abc\n";
        String expected = "abc";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        assertEquals(expected, result);
    }
}