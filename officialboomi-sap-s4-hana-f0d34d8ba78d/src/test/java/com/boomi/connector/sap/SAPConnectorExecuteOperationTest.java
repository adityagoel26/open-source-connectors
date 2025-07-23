// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.testutil.SimpleTrackedData;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorExecuteOperationTest {
	
	private static final String INPUT ="{\r\n" + 
			"\"A_PATH_PARAM_Language\" : \"EN\",\r\n" + 
			"\"A_PATH_PARAM_RecipeUUID\" : \"guid'00163e19-8846-1ed7-8ee2-164ce8cd5d15'\"\r\n" + 
			"}";

	

	@Test
	public void testExecuteUpdateUpdateRequestOperationResponse() {
		SAPConnectorExecuteTestContext context = new SAPConnectorExecuteTestContext();
		SAPConnectorConnection conn = new SAPConnectorConnection(context);
		SAPConnectorExecuteOperation execute = new SAPConnectorExecuteOperation(conn);
		OperationResponse response = Mockito.mock(OperationResponse.class);
		UpdateRequest request = Mockito.mock(UpdateRequest.class);
		SimpleTrackedData trackedData = new SimpleTrackedData(1, IOUtils.toInputStream(INPUT));
		Iterator<ObjectData> iterator=Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(iterator);
		when(iterator.next()).thenReturn(trackedData);
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		execute.executeUpdate(request, response);

	}

	

}
