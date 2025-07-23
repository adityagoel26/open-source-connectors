// Copyright (c) 2022 Boomi, LP.
package boomi.connector.oracledatabase;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.storedprocedureoperation.StoredProcedureExecute;
import com.boomi.connector.oracledatabase.storedprocedureoperation.StoredProcedureOperation;
import com.boomi.connector.oracledatabase.util.SchemaBuilderUtil;
import com.boomi.connector.testutil.SimpleOperationContext;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.fasterxml.jackson.core.JsonGenerator;
import oracle.jdbc.OracleCallableStatement;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoredProcedureTest {

    public static final String INPUT6 = TestUtil.readJsonFromFile("src/test/java/StoredProcedure_Varray.txt");
    private static final String METHOD_NAME = "processStruct2";
    private static final String METHOD_NAME1 = "processRefCursors";
    private static final String PROCEDURE1 = "Test.TestProcedure";
    private static final String PROCNAME1 = "Test.package.TestProcedure";
    private static final Logger logger = mock(Logger.class);
    private static final MockedStatic<ArrayDescriptor> arrayDescriptor1 = Mockito.mockStatic(ArrayDescriptor.class);
    private static final MockedStatic<StructDescriptor> structDescriptor1 = Mockito.mockStatic(StructDescriptor.class);
    private static final MockedStatic<SchemaBuilderUtil> schemaBuilderUtil = Mockito.mockStatic(SchemaBuilderUtil.class);
    private final SimpleOperationResponse simpleOperationResponse1 = new SimpleOperationResponse();
    private final SimpleOperationContext simpleOperationContext1 = mock(SimpleOperationContext.class);
    private final Connection _connection1 = mock(Connection.class);
    private final UpdateRequest _updateRequest1 = mock(UpdateRequest.class);
    private final JsonGenerator generator1 = mock(JsonGenerator.class);
    private final OracleCallableStatement _oracleCallableStatement1 = mock(OracleCallableStatement.class);
    private final ResultSet _resultSet1 = mock(ResultSet.class);
    private final ResultSetMetaData _resultSetMetaData1 = mock(ResultSetMetaData.class);
    private final DatabaseMetaData _databaseMetaData1 = mock(DatabaseMetaData.class);
    private final Class[] parameterTypes1 = new Class[3];
    private final Object[] parameters1 = new Object[3];
    private final OracleDatabaseConnection con = mock(OracleDatabaseConnection.class);
    private final StoredProcedureOperation ops = mock(StoredProcedureOperation.class);
    private final UpdateRequest request = mock(UpdateRequest.class);
    private final OperationResponse response = mock(OperationResponse.class);
    private final JsonGenerator generator = mock(JsonGenerator.class);
    private final Connection conn = mock(Connection.class);
    private final ResultSetMetaData rsmData = mock(ResultSetMetaData.class);
    private final ArrayDescriptor descriptor = mock(ArrayDescriptor.class);
    private final StructDescriptor structDescriptor = mock(StructDescriptor.class);
    private final StoredProcedureExecute storedProcedureExecute = mock(StoredProcedureExecute.class);
    private final Class[] parameterTypes = new Class[5];
    private final Object[] parameters = new Object[5];
    private Method method;
    private Method method1;
    private StoredProcedureExecute storedProcedureExecute1;

    @Before
    @SuppressWarnings("java:S3011")
    //suppressing impact of setAccessible(true) through Reflection to avoid sonar scan failure
    public void init() throws SQLException, NoSuchMethodException {
        when(response.getLogger()).thenReturn(logger);

        when(structDescriptor.getMetaData()).thenReturn(rsmData);
        when(rsmData.getColumnTypeName(2)).thenReturn("BOOMIORACLEDB.STRUCT_DATA_TAB");
        when(rsmData.getColumnName(1)).thenReturn("Name");
        when(rsmData.getColumnName(2)).thenReturn("EId");

        schemaBuilderUtil.when(() -> SchemaBuilderUtil.getUnwrapConnection(Mockito.any())).thenReturn(conn);

        when(descriptor.getBaseName()).thenReturn("BASENAME");

        arrayDescriptor1.when(() -> ArrayDescriptor.createDescriptor(Mockito.anyString(), Mockito.any())).thenReturn(descriptor);
        structDescriptor1.when(() -> StructDescriptor.createDescriptor(Mockito.anyString(), Mockito.any())).thenReturn(structDescriptor);

        parameterTypes[0] = JsonGenerator.class;
        parameterTypes[1] = Integer.class;
        parameterTypes[2] = Object.class;
        parameterTypes[3] = String.class;
        parameterTypes[4] = StructDescriptor.class;
        method = storedProcedureExecute.getClass().getDeclaredMethod(METHOD_NAME, parameterTypes);
        method.setAccessible(true);

        when(_connection1.getMetaData()).thenReturn(_databaseMetaData1);
        when(_databaseMetaData1.getProcedureColumns(null, null, null, null)).thenReturn(_resultSet1);
        when(_resultSet1.next()).thenReturn(true, false, true, false);
        when(_resultSet1.getString("TYPE_NAME")).thenReturn("REF CURSOR").thenReturn("REF CURSOR");
        when(_resultSet1.getString("COLUMN_NAME")).thenReturn("CODE").thenReturn("CODE");
        when(_resultSet1.getShort(5)).thenReturn((short) 4);
        when(_resultSet1.getString(4)).thenReturn("CODE");

        when(_resultSet1.getMetaData()).thenReturn(_resultSetMetaData1);
        when(_resultSetMetaData1.getColumnCount()).thenReturn(1);
        when(_resultSetMetaData1.getColumnType(anyInt())).thenReturn(2);

        when(_resultSetMetaData1.getColumnName(anyInt())).thenReturn("Test").thenReturn("9876598744433443");

        when(_resultSet1.getBigDecimal("9876598744433443")).thenReturn(new BigDecimal("9876598744433443"));

        storedProcedureExecute1 = new StoredProcedureExecute(_connection1, PROCEDURE1, PROCNAME1, _updateRequest1, simpleOperationResponse1, simpleOperationContext1);

        when(((_oracleCallableStatement1)).getCursor(anyInt())).thenReturn(_resultSet1);

        parameterTypes1[0] = JsonGenerator.class;
        parameterTypes1[1] = CallableStatement.class;
        parameterTypes1[2] = int.class;

        method1 = storedProcedureExecute1.getClass().getDeclaredMethod(METHOD_NAME1, parameterTypes1);
        method1.setAccessible(true);
    }

    @Test
    public void testexecuteCreateOperation() throws IOException {
        con.loadProperties();
        InputStream resultStream = new ByteArrayInputStream(INPUT6.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(1, resultStream);
        Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
        when(request.iterator()).thenReturn(objDataItr);
        when(objDataItr.hasNext()).thenReturn(true, false);
        when(objDataItr.next()).thenReturn(trackedData);
        when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
        ops.executeSizeLimitedUpdate(request, response);
        assertTrue(true);
        resultStream.close();
    }

    @Test
    public void testProcessStruct2() throws IOException, SQLException, InvocationTargetException, IllegalAccessException {
        generator.writeStartObject();

        Object[] arr1 = {"Michael", "1"};
        STRUCT s1 = mock(STRUCT.class);
        Object[] arr2 = {"Dwight", "2"};
        STRUCT s2 = mock(STRUCT.class);

        Object[] structArray = {s1, s2};
        ARRAY arrz = mock(ARRAY.class);
        when(arrz.getArray()).thenReturn(structArray);
        when(s1.getAttributes()).thenReturn(arr1);
        when(s2.getAttributes()).thenReturn(arr2);

        parameters[0] = generator;
        parameters[1] = 2;
        parameters[2] = arrz;
        parameters[3] = "STRUCT_DATA";
        parameters[4] = structDescriptor;
        method.invoke(storedProcedureExecute, parameters);
        generator.writeEndObject();
        generator.close();
        verify(generator, times(1)).writeStringField("Name", "Michael");
        verify(generator, times(1)).writeStringField("EId", "1");
        verify(generator, times(1)).writeStringField("Name", "Dwight");
        verify(generator, times(1)).writeStringField("EId", "2");
    }

    /**
     * This test is used to check whether more than Int limit fetched without numeric overflow error for NUMBER column
     */
    @Test
    public void columnTypeTwoMoreThanIntLimitTest() throws IOException, InvocationTargetException, IllegalAccessException {

        generator1.writeStartObject();

        parameters1[0] = generator1;
        parameters1[1] = _oracleCallableStatement1;
        parameters1[2] = 0;

        method1.invoke(storedProcedureExecute1, parameters1);
        generator1.writeEndObject();
        generator1.close();

        verify(generator1, times(1)).writeNumberField("Test", new BigDecimal("9876598744433443"));
    }

    /**
     * This test is used to check whether more than Int limit fetched without numeric overflow error for NUMBER column
     */
    @Test
    public void columnTypeTwoMoreThanIntLimitDecimalTest() throws IOException, InvocationTargetException, IllegalAccessException, SQLException {

        generator1.writeStartObject();

        parameters1[0] = generator1;
        parameters1[1] = _oracleCallableStatement1;
        parameters1[2] = 0;

        when(_resultSetMetaData1.getColumnName(1)).thenReturn("TestBigDecimal");
        when(_resultSet1.getBigDecimal("TestBigDecimal")).thenReturn(new BigDecimal("12345678901234567890.12345"));

        method1.invoke(storedProcedureExecute1, parameters1);
        generator1.writeEndObject();
        generator1.close();

        verify(generator1, times(1)).writeNumberField("TestBigDecimal", new BigDecimal("12345678901234567890.12345"));
    }

    /**
     * This test is used to check whether a column with type two times the Int limit is handled correctly
     */
    @Test
    public void columnTypeTwoMaxIntLimitTest() throws IOException, InvocationTargetException, IllegalAccessException, SQLException {

        when(_resultSetMetaData1.getColumnName(1)).thenReturn("ColumnTwoMaxIntLimit");
        when(_resultSet1.getBigDecimal("ColumnTwoMaxIntLimit")).thenReturn(new BigDecimal(2).multiply(new BigDecimal(Integer.MAX_VALUE)));

        generator1.writeStartObject();

        parameters1[0] = generator1;
        parameters1[1] = _oracleCallableStatement1;
        parameters1[2] = 0;

        method1.invoke(storedProcedureExecute1, parameters1);
        generator1.writeEndObject();
        generator1.close();

        verify(generator1, times(1)).writeNumberField("ColumnTwoMaxIntLimit", new BigDecimal(2).multiply(new BigDecimal(Integer.MAX_VALUE)));
    }

    /**
     * This test is used to check whether a column with type two less than Int limit is handled correctly
     */
    @Test
    public void columnTypeTwoLessThanIntLimitTest() throws IOException, InvocationTargetException, IllegalAccessException, SQLException {

        when(_resultSetMetaData1.getColumnName(1)).thenReturn("ColumnTwoLessThanIntLimit");
        when(_resultSet1.getBigDecimal("ColumnTwoLessThanIntLimit")).thenReturn(new BigDecimal("-4732746732648273472"));

        generator1.writeStartObject();

        parameters1[0] = generator1;
        parameters1[1] = _oracleCallableStatement1;
        parameters1[2] = 0;

        method1.invoke(storedProcedureExecute1, parameters1);
        generator1.writeEndObject();
        generator1.close();

        verify(generator1, times(1)).writeNumberField("ColumnTwoLessThanIntLimit", new BigDecimal("-4732746732648273472"));
    }

    /**
     * This test is used to check whether a column with type within Int limit is handled correctly
     */
    @Test
    public void columnTypeLessWithinIntLimitTest() throws IOException, InvocationTargetException, IllegalAccessException, SQLException {

        when(_resultSetMetaData1.getColumnName(1)).thenReturn("ColumnWithinIntLimit");
        when(_resultSet1.getBigDecimal("ColumnWithinIntLimit")).thenReturn(new BigDecimal(20000));

        generator1.writeStartObject();

        parameters1[0] = generator1;
        parameters1[1] = _oracleCallableStatement1;
        parameters1[2] = 0;

        method1.invoke(storedProcedureExecute1, parameters1);
        generator1.writeEndObject();
        generator1.close();

        verify(generator1, times(1)).writeNumberField("ColumnWithinIntLimit", new BigDecimal(20000));
    }

}
