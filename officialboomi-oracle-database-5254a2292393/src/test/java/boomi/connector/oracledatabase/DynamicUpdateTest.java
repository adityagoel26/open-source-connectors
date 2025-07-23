// Copyright (c) 2022 Boomi, LP.
package boomi.connector.oracledatabase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.oracledatabase.DynamicUpdateOperation;
import com.boomi.connector.oracledatabase.OracleDatabaseConnection;
import com.boomi.connector.testutil.SimpleTrackedData;

public class DynamicUpdateTest {
	
	private OracleDatabaseConnection con =mock(OracleDatabaseConnection.class);
	private DynamicUpdateOperation ops = mock(DynamicUpdateOperation.class);
	private UpdateRequest request = mock(UpdateRequest.class);
	private OperationResponse response = mock(OperationResponse.class);
	private static Logger logger = mock(Logger.class);
	

	public static final String INPUT = "{\r\n" + "\"PERSON_ID\": \"123\",\r\n" + "\"NAME\": \"Swastik\",\r\n"
			+ "\"PERSON_DOB\": \"12-MAR-2020\",\r\n" + "\"CITY\": \"BANGALORE\",\r\n" + "\"STATUS\": \"E\"\r\n"
			+ "}";

	public static final String INPUT1 = TestUtil.readJsonFromFile("src/test/java/DynamicUpdate.txt");
	
	@Before
	public void init() {
		when(response.getLogger()).thenReturn(logger);
	}
	
	
	@Test(expected = SQLException.class)
	public void testexecuteUpdateOperation() throws IOException, SQLException {
		con.loadProperties();
		InputStream result = new ByteArrayInputStream(INPUT1.getBytes(StandardCharsets.UTF_8));
		SimpleTrackedData trackedData = new SimpleTrackedData(1, result);
		Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(objDataItr);
		when(objDataItr.hasNext()).thenReturn(true, false);
		when(objDataItr.next()).thenReturn(trackedData);
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		try(Connection connn = DriverManager.getConnection(con.getUrl(), con.getUsername(), con.getPassword())) {
			connn.setAutoCommit(false);
			ops.executeUpdateOperation(request, response, connn);
		} 
		result.close();
	}
}
