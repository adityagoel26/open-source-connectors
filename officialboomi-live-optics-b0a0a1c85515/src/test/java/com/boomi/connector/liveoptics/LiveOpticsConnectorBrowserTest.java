//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static com.boomi.connector.api.ObjectDefinitionRole.INPUT;
import static com.boomi.connector.api.ObjectDefinitionRole.OUTPUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.liveoptics.LiveOpticsConnectorBrowser;
import com.boomi.connector.liveoptics.LiveOpticsConnectorConnection;
/**
 * @author aditi ardhapure
 *
 * ${tags}
 */
public class LiveOpticsConnectorBrowserTest {
	private final LiveOpticsConnectorConnection connection = mock(LiveOpticsConnectorConnection.class);
	private final LiveOpticsConnectorBrowser browser = new LiveOpticsConnectorBrowser(connection);

	@Test
	public void testGetObjectTypes(){
		ObjectTypes expected = new ObjectTypes();
		ObjectType type = new ObjectType();
		expected.getTypes().add(type);
		type.setId("Project");
		ObjectTypes actual = browser.getObjectTypes();
		assertNotNull(actual);
		assertEquals(expected.getTypes().get(0).getId(), actual.getTypes().get(0).getId());
	}

	@Test
	public void testGetObjectDefinitions() throws IOException {
		String objectTypeId = "theObject";
		ContentType expectedInputType = ContentType.JSON;
		ContentType expectedOutputType = ContentType.JSON;
		ObjectDefinitions objectDefinitions = browser.getObjectDefinitions(objectTypeId, Arrays.asList(INPUT, OUTPUT));
		assertNotNull(objectDefinitions);
		List<ObjectDefinition> definitions = objectDefinitions.getDefinitions();
		assertNull(definitions.get(0).getSchema());
		assertNull(definitions.get(0).getExtraSchemas());
		assertNull(definitions.get(0).getCookie());
		assertEquals(null,definitions.get(0).getJsonSchema());
		assertEquals("", definitions.get(0).getElementName());
		assertEquals(expectedInputType, definitions.get(0).getInputType());
		assertEquals(expectedOutputType, definitions.get(0).getOutputType());
	}

	@Test
	public void testGetConnection() {
		assertEquals(connection, browser.getConnection());
	}

}
