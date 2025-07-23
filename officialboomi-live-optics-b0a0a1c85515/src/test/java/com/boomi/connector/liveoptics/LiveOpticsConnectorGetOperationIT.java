//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnector;
import com.boomi.connector.liveoptics.utils.LiveOpticsConstants;
import com.boomi.connector.testutil.ConnectorTester;

/**
 * @author aditi.ardhapure
 *
 * 
 */
public class LiveOpticsConnectorGetOperationIT { 
    
	@Test
	public void testLiveOpticsConnectorGetOperation() { 
		LiveOpticsConnectorConnector connector = new LiveOpticsConnectorConnector();
		ConnectorTester tester = new ConnectorTester(connector);
        Map<ObjectDefinitionRole, String> cookie = new HashMap<>();
        cookie.put(ObjectDefinitionRole.OUTPUT, "true");
        Map<String, Object> opProperty = new HashMap<>();
        Map<String, Object> connProperty = new HashMap<>(); 
        connProperty.put(LiveOpticsConstants.LOGIN_SECRET,"0BSSbaAxya5Cwbmjm/s4y7Kv5ua2pvuDM228s5dc+efp");
        connProperty.put(LiveOpticsConstants.SHARED_SECRET,"+idBbAUqzNVV02z300zD5PLOfuABOuTWypBv4s2xEqwf");
        connProperty.put(LiveOpticsConstants.URL_PROPERTY,"https://papi.liveoptics-uat.com");
        connProperty.put(LiveOpticsConstants.LOGIN_ID_1,"Z9DrRp6NCdX45HJY0X+5hY3VdJkYO2793/wUPd8S4T6o");
        opProperty.put(LiveOpticsConstants.INCLUDE_ENTITIES,Boolean.TRUE);
        tester.setOperationContext(OperationType.GET, connProperty, opProperty, "File", cookie);
        tester.setBrowseContext(OperationType.GET,connProperty,opProperty);
        tester.executeGetOperation("469209");
	}
}
