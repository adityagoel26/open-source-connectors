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
public class SAPConnectorDeleteOperationTest {
	
	private static final String INPUT = "{\r\n" + 
			"  \"A_PATH_PARAM_AddressID\" : \"22825\",\r\n" + 
			"  \"A_PATH_PARAM_Person\": \"22838\",\r\n" + 
			"  \"A_PATH_PARAM_OrdinalNumber\": \"1\"\r\n" + 
			"}";
	
	@Test
	public void testDeleteOperation() {
		SAPConnectorDeleteTestContext context = new SAPConnectorDeleteTestContext();
		SAPConnectorConnection con = new SAPConnectorConnection(context);
		SAPConnectorDeleteOperation deleteOperation = new SAPConnectorDeleteOperation(con);
		
		OperationResponse response = Mockito.mock(OperationResponse.class);
		UpdateRequest request = Mockito.mock(UpdateRequest.class);
		SimpleTrackedData trackedData = new SimpleTrackedData(1, IOUtils.toInputStream(INPUT));
		Iterator<ObjectData> iterator=Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true,false);
		when(iterator.next()).thenReturn(trackedData);
		
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		deleteOperation.executeUpdate(request, response);
		
	}

}
