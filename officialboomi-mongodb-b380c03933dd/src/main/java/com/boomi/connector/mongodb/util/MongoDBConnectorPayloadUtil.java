// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.util;

import java.io.IOException;
import java.io.OutputStream;

import com.boomi.connector.api.BasePayload;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;


/**
 * The Class MongoDBConnectorPayloadUtil.
 *
 */
public class MongoDBConnectorPayloadUtil {
	
	/**
	 * Instantiates a new mongo DB connector payload util.
	 */
	private MongoDBConnectorPayloadUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	/** The Constant MAPPER. */
	public static final ObjectMapper MAPPER = (new ObjectMapper())
			.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	/**
	 * Returns with given object.
	 *
	 * @param object the object
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
}
