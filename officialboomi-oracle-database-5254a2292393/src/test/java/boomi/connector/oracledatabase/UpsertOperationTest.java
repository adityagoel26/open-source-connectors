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
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.oracledatabase.upsert.DynamicUpsert;
import com.boomi.connector.testutil.SimpleTrackedData;

public class UpsertOperationTest {
	
	private OracleDatabaseConnection con = mock(OracleDatabaseConnection.class);
	private OracleDatabaseConnection con1 = mock(OracleDatabaseConnection.class);
	private DynamicUpsert ops = mock(DynamicUpsert.class);
	private DynamicUpsert ops1 = mock(DynamicUpsert.class);
	private UpdateRequest request = mock(UpdateRequest.class);
	private OperationResponse response = mock(OperationResponse.class);
	private Logger logger = mock(Logger.class);

	public static final String INPUT = "{\r\n" + 
			"\"PERSON_ID\": 1234,\r\n" + 
			"\"NAME\": \"Swastik\",\r\n" + 
			"\"PERSON_DOB\": \"12-MAR-2020\",\r\n" + 
			"\"CITY\": \"BANGALORE\",\r\n" + 
			"\"STATUS\": \"E\"\r\n" + 
			"}";
	public static final String INPUT_PERFORMANCE = "{\r\n" + 
			"  \"IDENTIFIER\" : \"100000006\",\r\n" + 
			"  \"TITLE\" : \"\",\r\n" + 
			"  \"GIVEN_NAME\" : \"\",\r\n" + 
			"  \"MIDDLE_NAME\" : \"\",\r\n" + 
			"  \"FAMILY_NAME\" : \"\",\r\n" + 
			"  \"PREFERRED_NAME\" : \"\",\r\n" + 
			"  \"BIRTH_DATE\" : \"\",\r\n" + 
			"  \"LIBRARY_BARCODE\" : \"\",\r\n" + 
			"  \"EMAIL_ADDRESS\" : \"\",\r\n" + 
			"  \"MOBILE_NUMBER\" : \"\",\r\n" + 
			"  \"CONTACT_POSTCODE\" : \"\",\r\n" + 
			"  \"CONSOLIDATED_FLAG\" : \"\",\r\n" + 
			"  \"POSTGRAD_FLAG\" : \"\",\r\n" + 
			"  \"RESEARCH_FLAG\" : \"\",\r\n" + 
			"  \"DECEASED_FLAG\" : \"\",\r\n" + 
			"  \"LAST_MODIFIED\" : \"\"\r\n" + 
			"}";

	@Before
	public void init() {
		when(response.getLogger()).thenReturn(logger);
	}

	@Test
	public void testexecuteCreateOperation() throws IOException {
		con.loadProperties();
		InputStream result = new ByteArrayInputStream(INPUT_PERFORMANCE.getBytes(StandardCharsets.UTF_8));
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
	@Test
	public void testexecuteCreateOperation1() throws IOException {
		con1.loadProperties();
		InputStream result = new ByteArrayInputStream(INPUT_PERFORMANCE.getBytes(StandardCharsets.UTF_8));
		SimpleTrackedData trackedData = new SimpleTrackedData(1, result);
		Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(objDataItr);
		when(objDataItr.hasNext()).thenReturn(true, false);
		when(objDataItr.next()).thenReturn(trackedData);
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		ops1.executeSizeLimitedUpdate(request, response);
		assertTrue(true);
		result.close();
	}
	

	
	

}
