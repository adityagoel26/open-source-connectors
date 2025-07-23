//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import org.junit.Test;

import com.boomi.connector.liveoptics.utils.JsonSchemaBuilder;

/**
 * @author aditi ardhapure
 *
 * ${tags}
 */
public class JsonSchemaBuilderTest {

	@Test
	public void testBuildJsonSchema() throws IOException {
		assertNotNull(LiveOpticsTestConstants.JSONSCHEMAURL);
		JsonSchemaBuilder.buildJsonSchema(LiveOpticsTestConstants.JSONSCHEMAURL);
	}
}