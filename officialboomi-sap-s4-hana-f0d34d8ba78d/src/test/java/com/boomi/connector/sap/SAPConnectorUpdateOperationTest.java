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
public class SAPConnectorUpdateOperationTest {
	
	private static final String INPUT ="{       \r\n" + 
			" \"A_PATH_PARAM_Customer\":\"17100009\",\r\n" + 
			"\"A_PATH_PARAM_DistributionChannel\":\"10\",\r\n" + 
			"\"A_PATH_PARAM_Material\":\"TG11\",\r\n" + 
			"\"A_PATH_PARAM_SalesOrganization\":\"1710\",\r\n" + 
			"  \"d\": {         \r\n" + 
			"\"MaterialDescriptionByCustomer\": \"Test SAP\"\r\n" + 
			"        }\r\n" + 
			"}";
			
			/*"{\r\n" + 
			"        \"A_PATH_PARAM_AddressID\":\"22825\",\r\n" + 
			"\"A_PATH_PARAM_OrdinalNumber\":\"1\",\r\n" + 
			"\"A_PATH_PARAM_Person\":\"22838\",\r\n" + 
			"  \"d\": {\r\n" + 
			"      \r\n" + 
			"        \"EmailAddress\": \"sap.update@17100002.com\",\r\n" + 
			"        \"SearchEmailAddress\": \"SAP.UPDATE@17100002.\"\r\n" + 
			"        }\r\n" + 
			"}";*/

	
	
	@Test
	public void testUpdateOperationResponse() {
		SAPConnectorUpdateTestContext context = new SAPConnectorUpdateTestContext();
		SAPConnectorConnection conn = new SAPConnectorConnection(context);
		SAPConnectorUpdateOperation execute = new SAPConnectorUpdateOperation(conn);
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
