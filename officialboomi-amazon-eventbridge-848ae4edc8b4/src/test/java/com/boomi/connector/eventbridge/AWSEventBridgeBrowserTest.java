/*
*  Copyright 2019 Accenture. All Rights Reserved.
*  The trademarks used in these materials are the properties of their respective owners.
*  This work is protected by copyright law and contains valuable trade secrets and
*  confidential information.
*/

package com.boomi.connector.eventbridge;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.util.AWSEventBridgeActionEnum;
import com.boomi.connector.util.AWSEventBridgeUtil;

/**
 * This class helps to do the browser operation including selecting the object
 * type and creating the request and response profile.
 * 
 * @author swastik.vn
 */
public class AWSEventBridgeBrowserTest {

	private final AWSEventBridgeTestContext testContext = new AWSEventBridgeTestContext();
	private final AWSEventBridgeConnection connection = new AWSEventBridgeConnection(testContext);
	private final AWSEventBridgeBrowser browser = new AWSEventBridgeBrowser(connection);

	@Test
	public void testGetObjectTypes() {
		ObjectTypes actual = browser.getObjectTypes();
		assertNotNull(actual);

		ObjectTypes expected = new ObjectTypes();
		AWSEventBridgeConnection conn = browser.getConnection();
		assertNotNull(conn);
		BrowseContext browsecontext = conn.getContext();
		assertNotNull(browsecontext);
		List<ObjectType> objTypeList = new ArrayList<>();
		String operationType = browsecontext.getOperationType().toString();
		assertNotNull(operationType);
		List<String> events = new ArrayList<>();
		for (AWSEventBridgeActionEnum m : AWSEventBridgeActionEnum.values()) {
			if (m.showValue().equalsIgnoreCase(operationType)) {
				
				events.add(AWSEventBridgeUtil.convertString(m.toString()));
			}
		}
		for (String event : events) {
			ObjectType objtype = new ObjectType();
			objtype.setId(event);
			objTypeList.add(objtype);
		}
		expected.getTypes().addAll(objTypeList);
		assertEquals(expected.getTypes().get(0).getId(), actual.getTypes().get(0).getId());

	}

	@Test
	public void testGetObjectDefinitions() {
		String objectTypeId = "PUTEVENTS";
		ObjectDefinitions objectDefinitions = browser.getObjectDefinitions(objectTypeId, Arrays.asList(INPUT, OUTPUT));
		assertNotNull(objectDefinitions);
		List<ObjectDefinition> definitions = objectDefinitions.getDefinitions();
		assertNotNull(definitions.get(0).getJsonSchema());
		assertEquals("", definitions.get(0).getElementName());
		assertNull(definitions.get(0).getExtraSchemas());
		assertNull(definitions.get(0).getCookie());
		assertEquals(ContentType.JSON, definitions.get(0).getInputType());
		assertEquals(ContentType.JSON, definitions.get(0).getOutputType());
	}

	@Test
	public void testGetObjectDefinitionsWithException() {
		String objectTypeId = null;
		ObjectDefinitions objDef = browser.getObjectDefinitions(objectTypeId, Arrays.asList(INPUT, OUTPUT));
		assertNotNull(objDef);
	}

}