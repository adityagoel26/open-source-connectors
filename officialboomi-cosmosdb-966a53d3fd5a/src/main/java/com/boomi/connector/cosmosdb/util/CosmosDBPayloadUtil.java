//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.util;

import java.io.IOException;
import java.io.OutputStream;

import com.boomi.connector.api.BasePayload;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author abhijit.d.mishra
 **/
public class CosmosDBPayloadUtil extends BasePayload{
	
	/**
	 * Instantiates a new mongo DB connector payload util.
	 */
	private CosmosDBPayloadUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	/** The Constant MAPPER. */
	public static final ObjectMapper MAPPER = (new ObjectMapper())
			.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/**
	 * Returns with given object.
	 *
	 * @param object
	 * @return the base payload
	 */
	public static BasePayload toPayload(final Object object) {
		if (object == null || MAPPER == null || !MAPPER.canSerialize(object.getClass())) {
			return null;
		}

		return new BasePayload() {
			@Override
			public void writeTo(OutputStream out) throws IOException {
				MAPPER.writeValue(out, object);
			}
		};

	}
	
	/**
	 * Returns with results of Query Operation.
	 *
	 * @param parser
	 * @return the base payload
	 */
	public static BasePayload toPayloadQuery(final JsonParser parser)
	{
		if (parser == null || MAPPER == null ) {
		      return null;
		    }
		    
		return new BasePayload() {
        	@Override
        	public void writeTo(OutputStream out) throws IOException {
        		JsonFactory factory = new JsonFactory();
        		JsonGenerator generator = null;
        		try {
        			generator = factory.createGenerator(out);
        			generator.copyCurrentStructure(parser);
        			generator.flush();
        			generator.close();
        		} finally {
        			IOUtil.closeQuietly(generator);
        		}
        	}
        };
		
	}

}

