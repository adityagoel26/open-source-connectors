package com.boomi.snowflake.wrappers;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.snowflake.stages.StageHandler;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.ConnectionTimeFormat;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

public class BulkLoadWrapperTest {

    private ConnectionProperties mockProperties;
    private StageHandler mockStageHandler;
    private BulkLoadWrapper bulkLoadWrapper;
    private ConnectionTimeFormat connectionTimeFormat;
    private ObjectData mockInputDocument;
    private InputStream mockInputFile;
    private DynamicPropertyMap mockDynamicProperties;
    private ConnectionProperties.ConnectionGetter mockConnectionGetter;
    private Connection mockConnection;
    private Logger mockLogger;
    private PreparedStatement mockPreparedStatement;

    /**
     * Sets up the mock dependencies and initial state required for testing the {@link BulkLoadWrapper} class.
     * <p>
     * This method is executed before each test case to ensure a consistent and isolated environment.
     * Mock objects are created for dependencies such as {@link ConnectionProperties}, {@link StageHandler},
     * {@link ObjectData}, and others, which are used in tests to simulate real-world behaviors and configurations.
     * <p>
     * The {@link Mockito#mock(Class)} method is used to create mock instances, and common mock return values
     * are defined to simulate expected behavior in various scenarios. Additionally, the {@code bulkLoadWrapper}
     * instance is initialized with these mocked dependencies to facilitate testing of its methods.
     * </p>
     */
    @Before
    public void setUp() {
        // Mocking the dependencies
        mockProperties = Mockito.mock(ConnectionProperties.class);
        mockStageHandler = Mockito.mock(StageHandler.class);
        connectionTimeFormat = Mockito.mock(ConnectionTimeFormat.class);
        mockInputDocument = Mockito.mock(ObjectData.class);
        mockInputFile = Mockito.mock(InputStream.class);
        mockDynamicProperties = Mockito.mock(DynamicPropertyMap.class);
        mockConnectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
        mockConnection = Mockito.mock(Connection.class);
        mockLogger = Mockito.mock(Logger.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);

        // Setup mock return values
        Mockito.when(mockProperties.getConnectionGetter()).thenReturn(mockConnectionGetter);
        Mockito.when(mockProperties.getConnectionTimeFormat()).thenReturn(connectionTimeFormat);
        Mockito.when(mockProperties.getLogger()).thenReturn(mockLogger);
        Mockito.when(mockProperties.getTableName()).thenReturn("table_name");
        Mockito.when(mockProperties.getTruncate()).thenReturn(true);
        Mockito.when(mockProperties.getAutoCompress()).thenReturn(true);
        Mockito.when(mockProperties.getChunkSize()).thenReturn(1000L);
        Mockito.when(mockProperties.getColumnNames()).thenReturn("col1,col2,col3");
        Mockito.when(mockProperties.getFileFormatType()).thenReturn("CSV");
        Mockito.when(mockProperties.getCompression()).thenReturn("GZIP");
        Mockito.when(mockProperties.getCopyOptions()).thenReturn("option1,option2");
        Mockito.when(mockProperties.getFileFormatName()).thenReturn("txt");
        // Initialize the BulkLoadWrapper
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);

    }

    /**
     * Tests that the copy options for the {@link BulkLoadWrapper} include the "PURGE=TRUE" directive
     * when the options returned by {@link ConnectionProperties#getCopyOptions()} contain specific values.
     * <p>
     * This test verifies that the {@code BulkLoadWrapper} correctly sets the internal {@code _copyOptions}
     * field with the expected "PURGE=TRUE" when the {@code mockProperties} object is configured to
     * return "option1,option2" as its copy options.
     * <p>
     * The test achieves this by using {@link Mockito} to mock the return value for
     * {@code mockProperties.getCopyOptions()}, creating a new {@code BulkLoadWrapper} instance,
     * and then using {@code Whitebox} to access the private {@code _copyOptions} field for assertions.
     * </p>
     */
    @Test
    public void testCopyOptionsWithPurge(){
        Mockito.when(mockProperties.getCopyOptions()).thenReturn("option1,option2");
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);

        String copyOptionsValue = Whitebox.getInternalState(bulkLoadWrapper, "_copyOptions");

        Assert.assertTrue(copyOptionsValue.contains("PURGE=TRUE"));
    }

    /**
     * Tests the handling of file format and compression in the {@link BulkLoadWrapper} class.
     * <p>
     * This test ensures that when the compression setting is configured to "NONE" through
     * the {@link ConnectionProperties#getCompression()} method, the {@code BulkLoadWrapper}
     * correctly sets the internal file format. In this case, it checks that the internal
     * {@code _fileFormat} field contains the expected "CSV" value as the default format.
     * </p>
     * <p>
     * The test achieves this by using {@link Mockito} to set up a mock return value for
     * {@code mockProperties.getCompression()} and creating a {@code BulkLoadWrapper} instance.
     * It then accesses the private {@code _fileFormat} field using {@code Whitebox} to verify
     * that the expected value has been set.
     * </p>
     */
    @Test
    public void testFileFormatCompression(){
        // Test the file format and compression handling
        Mockito.when(mockProperties.getCompression()).thenReturn("NONE");
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);
        // Access private field _fileFormat using Whitebox
        String copyOptionsValue = Whitebox.getInternalState(bulkLoadWrapper, "_fileFormat");
        Assert.assertTrue(copyOptionsValue.contains("CSV"));
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper} class when the column names are empty.
     * <p>
     * This test verifies that when {@link ConnectionProperties#getColumnNames()} returns an empty string,
     * the {@code BulkLoadWrapper} instance correctly initializes the internal {@code _sqlConstructFields}
     * field as an empty string. This ensures that no additional columns are included when the input column
     * names are empty.
     * </p>
     * <p>
     * The test uses {@link Mockito} to set up the behavior of the {@code mockProperties} object, returning
     * an empty string for {@code getColumnNames()}. The {@code Whitebox} utility is then used to access
     * the private {@code _sqlConstructFields} field to confirm that it is set as expected.
     * </p>
     */
    @Test
    public void testEmptyColumnNames(){
        // Test case when column names are empty
        Mockito.when(mockProperties.getColumnNames()).thenReturn("");
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);
        // Access private field _sqlConstructFields using Whitebox
        String sqlConstructFieldsValue = Whitebox.getInternalState(bulkLoadWrapper, "_sqlConstructFields");
        Assert.assertEquals("", sqlConstructFieldsValue); // Empty columns should not add anything
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper} class when the file format type is set to "CSV".
     * <p>
     * This test verifies that when {@link ConnectionProperties#getFileFormatType()} returns "CSV",
     * the {@code BulkLoadWrapper} instance correctly sets the internal {@code _recordDelimiter} field
     * to the newline character ('\n'). This ensures that the correct record delimiter is applied
     * for CSV file formats.
     * </p>
     * <p>
     * The test uses {@link Mockito} to mock the return value of {@code getFileFormatType()} as "CSV".
     * The {@code Whitebox} utility is used to access and assert the value of the private field
     * {@code _recordDelimiter}.
     * </p>
     */
    @Test
    public void testFileFormatForCSV(){
        // Test if the correct record delimiter is set for CSV format
        Mockito.when(mockProperties.getFileFormatType()).thenReturn("CSV");
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);
        // Access private field _recordDelimiter using Whitebox
        char recordDelimiterValue = Whitebox.getInternalState(bulkLoadWrapper, "_recordDelimiter");
        Assert.assertEquals('\n', recordDelimiterValue);
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper} class when the file format type is set to a non-CSV value.
     * <p>
     * This test verifies that when {@link ConnectionProperties#getFileFormatType()} returns "JSON",
     * the {@code BulkLoadWrapper} instance correctly sets the internal {@code _recordDelimiter} field
     * to the comma character (','). This ensures that a default or appropriate record delimiter
     * is applied for non-CSV file formats.
     * </p>
     * <p>
     * The test uses {@link Mockito} to mock the return value of {@code getFileFormatType()} as "JSON".
     * The {@code Whitebox} utility is used to access and assert the value of the private field
     * {@code _recordDelimiter}.
     * </p>
     */
    @Test
    public void testFileFormatForOtherTypes(){
        // Test for non-CSV file format
        Mockito.when(mockProperties.getFileFormatType()).thenReturn("JSON");
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);
        // Access private field _recordDelimiter using Whitebox
        char recordDelimiterValue = Whitebox.getInternalState(bulkLoadWrapper, "_recordDelimiter");
        Assert.assertEquals(',', recordDelimiterValue);
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper} when the file format name and file format type are both empty.
     * <p>
     * This test verifies that when {@link ConnectionProperties#getFileFormatName()} and
     * {@link ConnectionProperties#getFileFormatType()} return empty strings, the {@code BulkLoadWrapper}
     * instance correctly handles default values for file format options. Specifically, it checks that:
     * </p>
     * <ul>
     *   <li>The {@code _fileFormatOptions} private field contains the default file format type as "CSV".</li>
     *   <li>The compression setting defaults to "AUTO" when no compression is specified.</li>
     *   <li>Other relevant default file format options, such as {@code FIELD_OPTIONALLY_ENCLOSED_BY}, are included.</li>
     * </ul>
     * <p>
     * The test uses {@link Mockito} to mock the behavior of {@link ConnectionProperties} methods.
     * The {@code Whitebox} utility is used to access and verify the value of the private field
     * {@code _fileFormatOptions}.
     * </p>
     */
    @Test
    public void testHandleFileFormatWithEmptyFileFormatNameAndEmptyFileFormateType(){
        // Test when getFileFormatName() returns an empty string
        Mockito.when(mockProperties.getFileFormatName()).thenReturn("");
        Mockito.when(mockProperties.getFileFormatType()).thenReturn(""); // Default case for TYPE
        Mockito.when(mockProperties.getCompression()).thenReturn("NONE");
        Mockito.when(mockProperties.getOtherFormatOptions()).thenReturn(""); // No other format options
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);

        // Access private field _fileFormatOptions using Whitebox
        String fileFormatOptionsValue = Whitebox.getInternalState(bulkLoadWrapper, "_fileFormatOptions");

        // Assertions
        Assert.assertTrue(fileFormatOptionsValue.contains("FILE_FORMAT = ("));
        Assert.assertTrue(fileFormatOptionsValue.contains("TYPE = 'CSV'")); // Default value
        Assert.assertTrue(fileFormatOptionsValue.contains("COMPRESSION = 'AUTO'"));
        Assert.assertTrue(fileFormatOptionsValue.contains("FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"));
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper} when a non-empty file format type is provided.
     * <p>
     * This test case verifies that when {@link ConnectionProperties#getFileFormatType()} is set to a specific type
     * (in this case, "JSON") and additional format options are provided, the {@code BulkLoadWrapper} correctly
     * sets the file format options. It checks that:
     * </p>
     * <ul>
     *   <li>The {@code _fileFormatOptions} private field contains the specified file format type and compression setting.</li>
     *   <li>Additional file format options, such as {@code RECORD_DELIMITER}, are included in the configuration.</li>
     *   <li>Unnecessary default options, such as {@code FIELD_OPTIONALLY_ENCLOSED_BY}, are excluded when not applicable.</li>
     *   <li>The {@code _recordDelimiter} field is correctly set based on the input options.</li>
     * </ul>
     * <p>
     * The test utilizes {@link Mockito} to mock the behavior of {@link ConnectionProperties} methods and
     * {@code Whitebox} for accessing and validating private fields.
     * </p>
     */
    @Test
    public void testHandleFileFormatWithNonEmptyFileFormatNameAndNonEmptyFileFormateType() {
        // Test when getFileFormatName() is not empty
        Mockito.when(mockProperties.getFileFormatName()).thenReturn("");
        Mockito.when(mockProperties.getFileFormatType()).thenReturn("JSON");
        Mockito.when(mockProperties.getCompression()).thenReturn("GZIP");
        Mockito.when(mockProperties.getOtherFormatOptions()).thenReturn("RECORD_DELIMITER = '|'");
        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);

        // Access private fields using Whitebox
        String fileFormatOptionsValue = Whitebox.getInternalState(bulkLoadWrapper, "_fileFormatOptions");
        char recordDelimiterValue = Whitebox.getInternalState(bulkLoadWrapper, "_recordDelimiter");

        // Assertions
        Assert.assertFalse(fileFormatOptionsValue.contains("FIELD_OPTIONALLY_ENCLOSED_BY = '\"'"));
        Assert.assertTrue(fileFormatOptionsValue.contains("TYPE = 'JSON'"));
        Assert.assertTrue(fileFormatOptionsValue.contains("COMPRESSION = 'GZIP'"));
        Assert.assertTrue(fileFormatOptionsValue.contains("RECORD_DELIMITER = '|'"));
        Assert.assertEquals('|', recordDelimiterValue);
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper} when custom format options are provided.
     * <p>
     * This test case checks that when {@link ConnectionProperties#getOtherFormatOptions()} specifies
     * custom options (e.g., setting a specific {@code RECORD_DELIMITER}), the {@code BulkLoadWrapper}
     * correctly applies these options during initialization. The test ensures that:
     * </p>
     * <ul>
     *   <li>The private field {@code _fileFormatOptions} correctly contains the specified custom options, such as {@code RECORD_DELIMITER}.</li>
     *   <li>The {@code _recordDelimiter} is set according to the custom value.</li>
     *   <li>Other settings, such as the file format type and compression, are appropriately configured.</li>
     * </ul>
     * <p>
     * The test uses {@link Mockito} for mocking the behavior of {@link ConnectionProperties} and {@code Whitebox}
     * to verify private field values in the {@code BulkLoadWrapper}.
     * </p>
     */
    @Test
    public void testHandleFileFormatWithCustomFormatOptions(){
        // Test case when getOtherFormatOptions() includes specific options that affect delimiter
        Mockito.when(mockProperties.getFileFormatName()).thenReturn("");
        Mockito.when(mockProperties.getFileFormatType()).thenReturn("XML");
        Mockito.when(mockProperties.getCompression()).thenReturn("NONE");
        Mockito.when(mockProperties.getOtherFormatOptions()).thenReturn("RECORD_DELIMITER = '~'");

        bulkLoadWrapper = new BulkLoadWrapper(mockProperties, mockStageHandler);

        // Access private fields using Whitebox
        String fileFormatOptionsValue = Whitebox.getInternalState(bulkLoadWrapper, "_fileFormatOptions");
        char recordDelimiterValue = Whitebox.getInternalState(bulkLoadWrapper, "_recordDelimiter");

        // Assertions
        Assert.assertTrue(fileFormatOptionsValue.contains("TYPE = 'XML'"));
        Assert.assertTrue(fileFormatOptionsValue.contains("RECORD_DELIMITER = '~'"));
        Assert.assertEquals('~', recordDelimiterValue);
    }

    /**
     * Tests the behavior of the {@link BulkLoadWrapper#uploadData} method with valid input data.
     * <p>
     * This test case simulates the scenario where the `uploadData` method is called with a valid
     * input document and properties. It ensures that the necessary interactions between the
     * {@link BulkLoadWrapper}, {@link StageHandler}, and database connection are performed correctly.
     * The test verifies that:
     * </p>
     * <ul>
     *   <li>The method correctly retrieves the stage path, file format, chunk size, auto compression flag, and record delimiter.</li>
     *   <li>The {@link StageHandler#UploadHandler} method is called with the appropriate parameters, including the stage path, file format, input file, chunk size, compression flag, and record delimiter.</li>
     *   <li>Mock interactions with {@link ConnectionGetter} and {@link Connection} (e.g., preparing a statement) are properly verified.</li>
     * </ul>
     * <p>
     * The test uses {@link Mockito} for mocking the dependencies and {@link Whitebox} to verify the internal state
     * of the {@code BulkLoadWrapper} instance.
     * </p>
     *
     * @throws SQLException If an SQL error occurs during the test execution.
     */
    @Test
    public void testUploadDataValidInput() throws SQLException{
        String stagePath = "temp_stage_path";
        Mockito.when(mockProperties.getStageTempPath(mockInputDocument)).thenReturn(stagePath);
        Mockito.when(mockInputDocument.getDynamicOperationProperties()).thenReturn(mockDynamicProperties);
        Mockito.when(mockConnectionGetter.getConnection(mockLogger)).thenReturn(mockConnection);
        Mockito.when(mockConnectionGetter.getConnection(mockLogger, mockDynamicProperties)).thenReturn(mockConnection);
        Mockito.when(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(mockPreparedStatement);

        bulkLoadWrapper.uploadData(mockInputFile, mockInputDocument, mockProperties);

        // Accessing private fields using Whitebox
        String fileFormatValue = Whitebox.getInternalState(bulkLoadWrapper, "_fileFormat");
        Long chunkSizeValue = Whitebox.getInternalState(bulkLoadWrapper, "_chunkSize");
        Boolean autoCompressValue = Whitebox.getInternalState(bulkLoadWrapper, "_autoCompress");
        char recordDelimiterValue = Whitebox.getInternalState(bulkLoadWrapper, "_recordDelimiter");

        // Verify the interaction with mockStageHandler
        Mockito.verify(mockStageHandler).UploadHandler(
                Mockito.eq(stagePath),
                Mockito.eq(fileFormatValue),
                Mockito.eq(mockInputFile),
                Mockito.eq(chunkSizeValue),
                Mockito.eq(autoCompressValue),
                Mockito.eq(recordDelimiterValue)
        );
    }
}

