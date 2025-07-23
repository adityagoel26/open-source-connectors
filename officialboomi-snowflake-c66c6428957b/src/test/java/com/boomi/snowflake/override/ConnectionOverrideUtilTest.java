// Copyright (c) 2025 Boomi, LP.
package com.boomi.snowflake.override;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.StringUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ConnectionOverrideUtilTest {

    private final PropertyMap propertyMap;
    private final boolean expected;
    private final PropertyMap inputProperties;
    private final PropertyMap operationProperties;
    private final PropertyMap expectedOutputProperties;
    private final DynamicPropertyMap dynamicProperties;

    private final PropertyMap operationPropertiesForDynamic;

    private final PropertyMap expectedDynamicOutputProperties;

    private final PropertyMap expectedOutputConnectionProperties;

    public ConnectionOverrideUtilTest(PropertyMap propertyMap, boolean expected, PropertyMap inputProperties,
            PropertyMap operationProperties, PropertyMap expectedOutputProperties,
            DynamicPropertyMap dynamicPropertyMap, PropertyMap operationPropertiesForDynamic,
            PropertyMap expectedDynamicOutputProperties, PropertyMap expectedOutputConnectionProperties) {
        this.propertyMap = propertyMap;
        this.expected = expected;
        this.inputProperties = inputProperties;
        this.operationProperties = operationProperties;
        this.expectedOutputProperties = expectedOutputProperties;
        this.dynamicProperties = dynamicPropertyMap;
        this.operationPropertiesForDynamic = operationPropertiesForDynamic;
        this.expectedDynamicOutputProperties = expectedDynamicOutputProperties;
        this.expectedOutputConnectionProperties = expectedOutputConnectionProperties;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {
                        createPropertyMapWithOverride(true), true, createPropertyMap("db", "schema"),
                        createPropertyMap("new db", "new schema"), createPropertyMap("new db", "new schema"),
                        createDynamicPropertyMap("new dynamic db", "new dynamic schema"),
                        createPropertyMap("new db", "new schema"),
                        createPropertyMap("new dynamic db", "new dynamic schema"),
                        createPropertyMap("new dynamic db", "new dynamic schema") }, {
                createPropertyMapWithOverride(false), false, createPropertyMap("db", "schema"),
                createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING),
                createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING),
                createDynamicPropertyMap("new dynamic db", "new dynamic schema"),
                createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING),
                createPropertyMap("new dynamic db", "new dynamic schema"),
                createPropertyMap("new dynamic db", "NEW DYNAMIC SCHEMA") }, {
                createPropertyMapWithOverride(null), false, createPropertyMap("db", "schema"),
                createPropertyMap(null, null), createPropertyMap("db", "schema"),
                createDynamicPropertyMap("new dynamic db", "new dynamic schema"), createPropertyMap(null, null),
                createPropertyMap(null, null), createPropertyMap("new dynamic db", "NEW DYNAMIC SCHEMA") }, {
                createPropertyMapWithOverride(Boolean.FALSE), false,
                createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING),
                createPropertyMap("new db", "new schema"), createPropertyMap("new db", "new schema"),
                createDynamicPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING),
                createPropertyMap("new db", "new schema"),
                createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING),
                createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING) }, {
                createPropertyMapWithOverride(Boolean.TRUE), true, createPropertyMap("db", "schema"),
                createPropertyMap(null, "new schema"), createPropertyMap("db", "new schema"),
                createDynamicPropertyMap("new dynamic db", StringUtil.EMPTY_STRING),
                createPropertyMap(null, "new schema"), createPropertyMap(null, StringUtil.EMPTY_STRING),
                createPropertyMap("new dynamic db", StringUtil.EMPTY_STRING) } });
    }

    /**
     * Tests if the connection settings override flag is correctly evaluated.
     */
    @Test
    public void testIsConnectionSettingsOverride() {
        Assert.assertEquals(expected, ConnectionOverrideUtil.isConnectionSettingsOverride(propertyMap));
    }

    /**
     * Tests if the connection properties are correctly overridden.
     */
    @Test
    public void testOverrideConnectionProperties() {
        ConnectionOverrideUtil.overrideConnectionProperties(inputProperties, operationProperties);
        Assert.assertEquals(expectedOutputProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
                inputProperties.getProperty(SnowflakeOverrideConstants.DATABASE));
        Assert.assertEquals(expectedOutputProperties.getProperty(SnowflakeOverrideConstants.SCHEMA),
                inputProperties.getProperty(SnowflakeOverrideConstants.SCHEMA));
    }

    /**
     * Tests if operation properties are correctly updated with dynamic values.
     */
    @Test
    public void testOverrideOperationConnectionPropertiesWithDynamicValues() {
        ConnectionOverrideUtil.overrideOperationConnectionPropertiesWithDynamicValues(dynamicProperties,
                operationPropertiesForDynamic);
        Assert.assertEquals(expectedDynamicOutputProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
                operationPropertiesForDynamic.getProperty(SnowflakeOverrideConstants.DATABASE));
        Assert.assertEquals(expectedDynamicOutputProperties.getProperty(SnowflakeOverrideConstants.SCHEMA),
                operationPropertiesForDynamic.getProperty(SnowflakeOverrideConstants.SCHEMA));
    }

    /**
     * Tests if the connection's catalog and schema are updated correctly
     * when overriding with dynamic properties.
     */
    @Test
    public void testoverrideConnectionWithDynamicProperties() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("oldDB");
        Mockito.when(mockConnection.getSchema()).thenReturn("oldSchema");

        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(mockConnection, dynamicProperties);

        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(mockConnection).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection).setSchema(schemaCaptor.capture());

        Assert.assertEquals(expectedOutputConnectionProperties.getProperty(SnowflakeOverrideConstants.DATABASE)
                        .toUpperCase(), catalogCaptor.getValue());
        Assert.assertEquals(expectedOutputConnectionProperties.getProperty(SnowflakeOverrideConstants.SCHEMA)
                        .toUpperCase(), schemaCaptor.getValue());
    }

    /**
     * Creates a `PropertyMap` with the specified database and schema values.
     * Returns a map with the provided database and schema properties.
     */
    public static PropertyMap createPropertyMap(String dbValue, String schemaValue) {
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put(SnowflakeOverrideConstants.DATABASE, dbValue);
        propertyMap.put(SnowflakeOverrideConstants.SCHEMA, schemaValue);
        return propertyMap;
    }

    /**
     * Creates a `DynamicPropertyMap` with the specified database and schema values.
     * Returns a map with the provided database and schema properties.
     */
    public static DynamicPropertyMap createDynamicPropertyMap(String dbValue, String schemaValue) {
        MutableDynamicPropertyMap dynamicProperty = new MutableDynamicPropertyMap();
        dynamicProperty.addProperty(SnowflakeOverrideConstants.DATABASE, dbValue);
        dynamicProperty.addProperty(SnowflakeOverrideConstants.SCHEMA, schemaValue);
        return dynamicProperty;
    }

    /**
     * Creates a {@link PropertyMap} and adds a connection override if {@code override} is not {@code null}.
     *
     * @param override a {@code Boolean} indicating whether to add the connection override property.
     * @return a {@link PropertyMap} with or without the override property based on {@code override}.
     */
    public static PropertyMap createPropertyMapWithOverride(Boolean override) {
        PropertyMap propertyMap = new MutablePropertyMap();
        if(null != override){
            propertyMap.put(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE, override);
        }
        return propertyMap;
    }

    /**
     * Tests the overriding of connection properties with dynamic values,
     * ensuring case sensitivity for database and schema names.
     */
    @Test
    public void testOverrideConnectionWithDynamicPropertiesWithCaseSensitive() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("OLD DB");
        Mockito.when(mockConnection.getSchema()).thenReturn("OLD SCHEMA");
        DynamicPropertyMap dynamicProperties=  createDynamicPropertyMap("\"new dynamic db\"",
                "\"new dynamic schema\"");
        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(mockConnection, dynamicProperties);
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(mockConnection).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection).setSchema(schemaCaptor.capture());

        Assert.assertEquals("new dynamic db",
                catalogCaptor.getValue());
        Assert.assertEquals("new dynamic schema",
                schemaCaptor.getValue());
    }

    /**
     * Tests the overriding of connection properties with dynamic values,
     * ensuring case insensitivity for schema names.
     */
    @Test
    public void testOverrideConnectionWithDynamicPropertiesWithoutCaseSensitive() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("OLD DB");
        Mockito.when(mockConnection.getSchema()).thenReturn("OLD SCHEMA");
        DynamicPropertyMap dynamicProperties=  createDynamicPropertyMap("new dynamic db",
                "new dynamic schema");
        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(mockConnection, dynamicProperties);
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(mockConnection).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection).setSchema(schemaCaptor.capture());

        Assert.assertEquals("NEW DYNAMIC DB",
                catalogCaptor.getValue());
        Assert.assertEquals("NEW DYNAMIC SCHEMA",
                schemaCaptor.getValue());
    }

    /**
     * Tests overriding the connection's schema with case-sensitive dynamic properties.
     * Verifies that the correct values are set in the mock connection.
     */
    @Test
    public void testOverrideConnectionWithDynamicPropertiesSchemaWithCaseSensitive() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("OLD DB");
        Mockito.when(mockConnection.getSchema()).thenReturn("OLD SCHEMA");
        DynamicPropertyMap dynamicProperties=  createDynamicPropertyMap("new dynamic db",
                "\"new dynamic schema\"");
        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(mockConnection, dynamicProperties);
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(mockConnection).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection).setSchema(schemaCaptor.capture());

        Assert.assertEquals("NEW DYNAMIC DB",
                catalogCaptor.getValue());
        Assert.assertEquals("new dynamic schema",
                schemaCaptor.getValue());
    }

    /**
     * Tests overriding the connection's catalog with case-sensitive dynamic properties.
     * Ensures the correct values are applied to the mock connection.
     */
    @Test
    public void testOverrideConnectionWithDynamicPropertiesDBWithCaseSensitive() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("OLD DB");
        Mockito.when(mockConnection.getSchema()).thenReturn("OLD SCHEMA");
        DynamicPropertyMap dynamicProperties=  createDynamicPropertyMap("\"new dynamic db\"",
                "new dynamic schema");
        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(mockConnection, dynamicProperties);
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(mockConnection).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection).setSchema(schemaCaptor.capture());

        Assert.assertEquals("new dynamic db",
                catalogCaptor.getValue());
        Assert.assertEquals("NEW DYNAMIC SCHEMA",
                schemaCaptor.getValue());
    }

    /**
     * Verifies that {@code isOverrideEnabled} returns {@code true} when both database and schema properties are non-null.
     */
    @Test
    public void testIsOverrideEnabledWhenDbAndSchemaNotNull(){
        PropertyMap propertyMap1 = createPropertyMap("new db", "new schema");
        Assert.assertEquals(true, ConnectionOverrideUtil.isOverrideEnabled(propertyMap1));
    }

    /**
     * Verifies that {@code isOverrideEnabled} returns {@code true} when both database and schema properties contain only blanks.
     */
    @Test
    public void testIsOverrideEnabledWhenDbAndSchemaBlank(){
        PropertyMap propertyMap1 = createPropertyMap(" ", " ");
        Assert.assertEquals(true, ConnectionOverrideUtil.isOverrideEnabled(propertyMap1));
    }

    /**
     * Verifies that {@code isOverrideEnabled} returns {@code false} when neither database nor schema properties are set.
     */
    @Test
    public void testIsOverrideEnabledWithoutDbAndSchema(){
        PropertyMap propertyMap1 = new MutablePropertyMap();
        Assert.assertEquals(false, ConnectionOverrideUtil.isOverrideEnabled(propertyMap1));
    }

    /**
     * Verifies that {@code isOverrideEnabled} returns {@code true} when both database and schema properties are empty strings.
     */
    @Test
    public void testIsOverrideEnabledWhenDbAndSchemaEmpty(){
        PropertyMap propertyMap1 = createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING);
        Assert.assertEquals(true, ConnectionOverrideUtil.isOverrideEnabled(propertyMap1));
    }

    /**
     * Tests the normalizeString method with various input cases.
     * Verifies correct handling of quotes, empty strings, and uppercase conversion.
     */
    @Test
    public void testNormalizeString() {
        Assert.assertEquals("Test",ConnectionOverrideUtil.normalizeString("\"Test\""));
        Assert.assertEquals("TEST",ConnectionOverrideUtil.normalizeString("Test"));
        Assert.assertEquals("", ConnectionOverrideUtil.normalizeString(""));
        Assert.assertEquals(" ", ConnectionOverrideUtil.normalizeString(" "));
        Assert.assertEquals("T", ConnectionOverrideUtil.normalizeString("\"Te"));
        Assert.assertEquals("", ConnectionOverrideUtil.normalizeString("e\""));
        Assert.assertNull(ConnectionOverrideUtil.normalizeString(null));
    }

    /**
     * Tests overriding the connection's catalog with case-sensitive dynamic properties.
     * Ensures the correct values are applied to the mock connection.
     */
    @Test
    public void testOverrideConnectionWithDynamicPropertiesAsNullDBWithCaseSensitive() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("OLD DB");
        Mockito.when(mockConnection.getSchema()).thenReturn("OLD SCHEMA");
        ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(mockConnection, null);
        Mockito.verify(mockConnection,Mockito.times(0)).getCatalog();
        Mockito.verify(mockConnection,Mockito.times(0)).getSchema();
    }

    /**
     * Test the behavior of `overrideConnectionForDb` when the provided database name differs from the existing catalog.
     * Verifies that the catalog is updated to the new database name when they don't match.
     */
    @Test
    public void testOverrideConnectionForDb() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("DB");
        ConnectionOverrideUtil.overrideConnectionForDb(mockConnection,"NEW DB");
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection).setCatalog(catalogCaptor.capture());
        Assert.assertEquals("NEW DB",
                catalogCaptor.getValue());
    }

    /**
     * Test the behavior of `overrideConnectionForDb` when the database catalog matches the provided value.
     * Verifies that the catalog is not changed if the database name matches the existing catalog.
     */
    @Test
    public void testOverrideConnectionForDbWithSameValue() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        Mockito.when(mockConnection.getCatalog()).thenReturn("DB");
        ConnectionOverrideUtil.overrideConnectionForDb(mockConnection,"DB");
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection,Mockito.times(0)).setCatalog(catalogCaptor.capture());
    }

    /**
     * Test the behavior of `isOverrideEnabled` when the database is null but schema is provided.
     * Verifies that override is not enabled when the database is null.
     */
    @Test
    public void testIsOverrideEnabledWhenSchema(){
        PropertyMap propertyMap1 =  createPropertyMap(null,"SCHEMA");
        Assert.assertEquals(false, ConnectionOverrideUtil.isOverrideEnabled(propertyMap1));
    }

    /**
     * Test the behavior of `isOverrideEnabled` when the database is provided but schema is null.
     * Verifies that override is not enabled when the schema is null.
     */
    @Test
    public void testIsOverrideEnabledWhenDB(){
        PropertyMap propertyMap1 =  createPropertyMap("DB",null);
        Assert.assertEquals(false, ConnectionOverrideUtil.isOverrideEnabled(propertyMap1));
    }

    /**
     * Tests the behavior of the doReset method in ConnectionOverrideUtil.
     * Verifies that setCatalog and setSchema are called once with correct arguments.
     */
    @Test
    public void testDoReset() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        Whitebox.invokeMethod(ConnectionOverrideUtil.class, "doReset",
                mockConnection, "DB", "SCHEMA");
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection, Mockito.times(1)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection, Mockito.times(1)).setSchema(schemaCaptor.capture());
        Assert.assertEquals("DB", catalogCaptor.getValue());
        Assert.assertEquals("SCHEMA", schemaCaptor.getValue());
    }

    /**
     * Tests the behavior of the doReset method when only the catalog is provided.
     * Verifies that setCatalog is called once, and setSchema is never called.
     */
    @Test
    public void testDoResetWithDb() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        Whitebox.invokeMethod(ConnectionOverrideUtil.class,
                "doReset",mockConnection,"\"db\"","");
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection,Mockito.times(1)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection,Mockito.never()).setSchema(schemaCaptor.capture());
        Assert.assertEquals("db",catalogCaptor.getValue());
    }

    /**
     * Tests the behavior of the doReset method when only the schema is provided.
     * Verifies that setCatalog and setSchema are both called with the correct values.
     */
    @Test
    public void testDoResetWithSchema() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        Whitebox.invokeMethod(ConnectionOverrideUtil.class,
                "doReset",mockConnection,"","\"Schema\"");
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection,Mockito.times(1)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection,Mockito.times(1)).setSchema(schemaCaptor.capture());
        Assert.assertEquals("",catalogCaptor.getValue());
        Assert.assertEquals("Schema",schemaCaptor.getValue());
    }

    /**
     * Tests the behavior of the doReset method when neither catalog nor schema is provided.
     * Verifies that both setCatalog and setSchema are called with empty strings.
     */
    @Test
    public void testDoResetWithNoDbNoSchema() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        Whitebox.invokeMethod(ConnectionOverrideUtil.class, "doReset",mockConnection,"","");
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection,Mockito.times(1)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection,Mockito.times(1)).setSchema(schemaCaptor.capture());
        Assert.assertEquals("",catalogCaptor.getValue());
        Assert.assertEquals("",schemaCaptor.getValue());
    }

    /**
     * Verifies that both database catalog and schema are updated when new values differ from current values.
     *
     * @throws SQLException if database operations fail
     */
    @Test
    public void shouldUpdateBothDatabaseAndSchema_WhenBothValuesAreDifferent() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        String newDatabase = "newDatabase";
        String newSchema = "newSchema";
        Mockito.when(mockConnection.getCatalog()).thenReturn("oldDatabase");
        Mockito.when(mockConnection.getSchema()).thenReturn("oldSchema");

        ConnectionOverrideUtil.updateConnection(mockConnection, newDatabase, newSchema);

        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection, Mockito.times(1)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection, Mockito.times(1)).setSchema(schemaCaptor.capture());
    }

    /**
     * Verifies that only database catalog is updated when schema is null.
     *
     * @throws SQLException if database operations fail
     */
    @Test
    public void shouldUpdateOnlyDatabase_WhenSchemaIsNull() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        String newDatabase = "newDatabase";
        Mockito.when(mockConnection.getCatalog()).thenReturn("oldDatabase");
        Mockito.when(mockConnection.getSchema()).thenReturn("oldSchema");

        ConnectionOverrideUtil.updateConnection(mockConnection, newDatabase, null);

        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection, Mockito.times(1)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection, Mockito.times(0)).setSchema(schemaCaptor.capture());
    }

    /**
     * Verifies that only schema is updated when database is null.
     *
     * @throws SQLException if database operations fail
     */
    @Test
    public void shouldUpdateOnlySchema_WhenDatabaseIsNull() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        String newSchema = "newSchema";
        Mockito.when(mockConnection.getCatalog()).thenReturn("oldDatabase");
        Mockito.when(mockConnection.getSchema()).thenReturn("oldSchema");

        ConnectionOverrideUtil.updateConnection(mockConnection, null, newSchema);

        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection, Mockito.times(0)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection, Mockito.times(1)).setSchema(schemaCaptor.capture());
    }

    /**
     * Verifies that only schema is updated when database remains unchanged.*
     *
     * @throws SQLException if database operations fail
     */
    @Test
    public void shouldUpdateOnlySchema_WhenDatabaseRemainsUnchanged() throws SQLException {
        Connection mockConnection = Mockito.mock(Connection.class);
        String newDatabase = "oldDatabase";
        String newSchema = "newSchema";
        Mockito.when(mockConnection.getCatalog()).thenReturn("oldDatabase");
        Mockito.when(mockConnection.getSchema()).thenReturn("oldSchema");

        ConnectionOverrideUtil.updateConnection(mockConnection, newDatabase, newSchema);

        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(mockConnection, Mockito.times(0)).setCatalog(catalogCaptor.capture());
        Mockito.verify(mockConnection, Mockito.times(1)).setSchema(schemaCaptor.capture());
    }

}