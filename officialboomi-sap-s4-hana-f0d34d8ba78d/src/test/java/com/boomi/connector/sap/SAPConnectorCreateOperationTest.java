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
public class SAPConnectorCreateOperationTest {
	
	private static final String INPUT = "{\r\n" + 
			"  \"A_PATH_PARAM_AddressID\" : \"22825\",\r\n" + 
			"  \"A_PATH_PARAM_BusinessPartner\" : \"17100002\",\r\n" + 
			"  \"AddressID\": \"22825\",\r\n" + 
			"  \"Person\": \"\",\r\n" + 
			"  \"OrdinalNumber\": \"1\",\r\n" + 
			"  \"IsDefaultEmailAddress\": true,\r\n" + 
			"  \"EmailAddress\": \"path@test.com\"\r\n" + 
			"}";
	
	@Test
	public void testUpdateOperationResponse() {
		SAPConnectorCreateTestContext context = new SAPConnectorCreateTestContext();
		SAPConnectorConnection conn = new SAPConnectorConnection(context);
		SAPConnectorCreateOperation execute = new SAPConnectorCreateOperation(conn);
		//SAPConnectorCreateOperation
		OperationResponse response = Mockito.mock(OperationResponse.class);
		UpdateRequest request = Mockito.mock(UpdateRequest.class);
		SimpleTrackedData trackedData = new SimpleTrackedData(1, IOUtils.toInputStream(INPUT));
		Iterator<ObjectData> iterator=Mockito.mock(Iterator.class);
		when(request.iterator()).thenReturn(iterator);
		when(iterator.hasNext()).thenReturn(true,false);
		when(iterator.next()).thenReturn(trackedData);
		
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		execute.executeSizeLimitedUpdate(request, response);

	}
	

}
