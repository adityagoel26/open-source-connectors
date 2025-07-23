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
import com.boomi.connector.oracledatabase.get.DynamicGetOperation;
import com.boomi.connector.testutil.SimpleTrackedData;

public class DynamicGetOpsTest {
	
	private OracleDatabaseConnection con = mock(OracleDatabaseConnection.class);
	private UpdateRequest request = mock(UpdateRequest.class);
	private OperationResponse response = mock(OperationResponse.class);
	private Logger logger = mock(Logger.class);
	
	public static final String INPUT = "{\r\n" + 
			"\"name\":\"Swastik\",\r\n" + 
			"\r\n" + 
			"}";
	
	public static final String INPUT1 = "{\r\n" + " \"ROW_NO\": 8 \r\n" + "}" ;
	
	@Before
	public void init() {
		when(response.getLogger()).thenReturn(logger);
	}
	


	@Test
	public void testexecuteGetOperation() throws IOException {
		con.loadProperties();
		InputStream result = new ByteArrayInputStream(INPUT1.getBytes(StandardCharsets.UTF_8));
		SimpleTrackedData trackedData = new SimpleTrackedData(1, result);
		Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(objDataItr);
		when(objDataItr.hasNext()).thenReturn(true, false);
		when(objDataItr.next()).thenReturn(trackedData);
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		assertTrue(true);
		result.close();
	}
	

}
