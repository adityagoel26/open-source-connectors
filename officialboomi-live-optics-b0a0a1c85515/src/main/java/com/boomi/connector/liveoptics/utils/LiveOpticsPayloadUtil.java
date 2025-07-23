//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics.utils;

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

public class LiveOpticsPayloadUtil extends BasePayload {
private static final ObjectMapper MAPPER = (new ObjectMapper()).disable(new MapperFeature[] { MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS }).disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	
	public static BasePayload toPayload(final JsonParser parser)
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