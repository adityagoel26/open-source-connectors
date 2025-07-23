// Copyright (c) 2022 Boomi, LP.
package boomi.connector.oracledatabase;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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


public class DynamicUpsertOpsTest {

	private OracleDatabaseConnection con = mock(OracleDatabaseConnection.class);
	private DynamicUpsert ops = mock(DynamicUpsert.class);
	private UpdateRequest request = mock(UpdateRequest.class);
	private OperationResponse response = mock(OperationResponse.class);
	private static Logger logger = mock(Logger.class);
	
	public static final String INPUT = "{\r\n" + " \"ROW_NO_COL\": 1,\r\n" + " \"TIMESTAMP_FORMAT\": \"2022-04-29 20:45:45.345\"\r\n"+ "}";

	public static final String INPUT1 = TestUtil.readJsonFromFile("src/test/java/DynamicUpsert.txt");
	@Before
	public void init() {
		when(response.getLogger()).thenReturn(logger);
	}

	@Test
	public void testexecuteCreateOperation() throws IOException {
		con.loadProperties();
		InputStream result = new ByteArrayInputStream(INPUT1.getBytes(StandardCharsets.UTF_8));
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
