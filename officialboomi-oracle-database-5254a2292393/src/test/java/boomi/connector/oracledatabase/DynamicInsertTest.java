// Copyright (c) 2022 Boomi, LP.
package boomi.connector.oracledatabase;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.DynamicInsertOperation;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.testutil.SimpleTrackedData;

public class DynamicInsertTest {

	private static final String TAB = "{\r\n";
	private OracleDatabaseConnection con = mock(OracleDatabaseConnection.class);
	private DynamicInsertOperation ops = mock(DynamicInsertOperation.class);
	private UpdateRequest request = mock(UpdateRequest.class);
	private OperationResponse response = mock(OperationResponse.class);
	private Logger logger = mock(Logger.class);
	static Random rand =  mock(Random.class);
	static int id = rand.nextInt(1000);

	public static final String INPUT = TAB + "\"COUNTRIES\": {\r\n" + "\"COUNTRY_ID\": \"IN\",\r\n"
			+ "\"LOCATIONS\": {\r\n" + "\"LOCATION\": \"y\",\r\n"
			+ "\"LOCATION_ID\": 12,\r\n" + "\"POSTAL_CODE\": \"567\",\r\n"
			+ "\"STARTEND_DATE\": {\r\n" + "\"element 1\": \"2020-11-11 00:00:00\",\r\n"
			+ "\"element 2\": \"2020-11-11 00:00:00\"\r\n" + "}\r\n" + "}\r\n" + "},\r\n"
			+ "\"REGION_ID\": 11,\r\n" + "\"REGION_NAME\": \"DAKSH\"\r\n" + "}";

	public static final String INPUT2 = TAB + "\"PERSON_ID\": \"+in+\",\r\n" + "\"NAME\": \"Swastik\",\r\n"
			+ "\"PERSON_DOB\": \"12-MAR-2020\",\r\n" + "\"CITY\": \"BANGALORE\",\r\n" + "\"STATUS\": \"E\"\r\n"
			+ "}";
	
	public static final String INPUT3 = TAB + " \"ROW_NO\": 13,\r\n" + " \"DATE_FORMAT\": \"2022-04-28 18:45:45.345\"\r\n"+ "}";
	
	public static final String INPUT4 = TAB + " \"ROW_NO\": 9,\r\n" + " \"DATE_FORMAT\": \"2022-04-29 10:45:45\"\r\n"+ "}";
	
	public static final String INPUT5 = TAB + " \"ROW_NO\": 10,\r\n" + " \"DATE_FORMAT\": \"30/APR/2022 10:45:45 AM\"\r\n"+ "}";

	@Before
	public void init() {
		when(response.getLogger()).thenReturn(logger);
	}

	@Test
	public void testexecuteInsertOperation() throws IOException {
		con.loadProperties();
		InputStream result = new ByteArrayInputStream(INPUT3.getBytes(StandardCharsets.UTF_8));
		SimpleTrackedData trackedData = new SimpleTrackedData(1, result);
		Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(objDataItr);
		when(objDataItr.hasNext()).thenReturn(true, false);
		when(objDataItr.next()).thenReturn(trackedData);
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		ops.executeSizeLimitedUpdate(request, response);
		assertTrue(true);
		result.close();
	}

}