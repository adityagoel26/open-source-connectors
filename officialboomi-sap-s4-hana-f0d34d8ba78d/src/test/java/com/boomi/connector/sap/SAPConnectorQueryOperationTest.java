// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap;

import static org.mockito.Mockito.when;

import java.util.logging.Logger;

import org.junit.Test;
import org.mockito.Mockito;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.QueryRequest;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorQueryOperationTest {

	@Test
	public void testExecuteQueryQueryRequestOperationResponse() {
		SAPConnectorQueryTestContext context = new SAPConnectorQueryTestContext();
		SAPConnectorConnection con = new SAPConnectorConnection(context);
		SAPConnectorQueryOperation operation = new SAPConnectorQueryOperation(con);
		QueryRequest request = Mockito.mock(QueryRequest.class);
		OperationResponse response = Mockito.mock(OperationResponse.class);
		when(request.getFilter()).thenReturn(Mockito.mock(FilterData.class));
		when(response.getLogger()).thenReturn(Mockito.mock(Logger.class));
		operation.executeQuery(request, response);
	}

}
