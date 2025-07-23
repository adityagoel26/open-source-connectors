// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.operation;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.logging.Logger;

import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.sapjco.SAPJcoConnection;
import com.boomi.connector.sapjco.SAPJcoConnectorBrowserTestContext;
import com.boomi.connector.testutil.SimpleTrackedData;

/**
 * @author kishore.pulluru
 *
 */
public class SAPJcoExecuteOperationTest {

	SAPJcoConnectorBrowserTestContext context = new SAPJcoConnectorBrowserTestContext();
	SAPJcoConnection connection = new SAPJcoConnection(context);

	private static final String INPUT_EMP = "<?xml version='1.0' encoding='UTF-8'?>\r\n" + "<BAPI_EMPLOYEE_GETDATA>\r\n"
			+ "  <LASTNAME_M>A*</LASTNAME_M>\r\n" + "</BAPI_EMPLOYEE_GETDATA>";

	@Test
	public void testExecuteUpdateUpdateRequestOperationResponse() {
		SAPJcoExecuteOperation executeOperation = new SAPJcoExecuteOperation(connection);
		OperationResponse response = Mockito.mock(OperationResponse.class);
		UpdateRequest request = Mockito.mock(UpdateRequest.class);
		InputStream result = new ByteArrayInputStream(INPUT_EMP.getBytes(StandardCharsets.UTF_8));
		SimpleTrackedData trackedData = new SimpleTrackedData(1, result);
		Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(objDataItr);
		when(objDataItr.hasNext()).thenReturn(true, false);
		when(objDataItr.next()).thenReturn(trackedData);
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		executeOperation.executeUpdate(request, response);
		assertTrue(true);

	}

}
