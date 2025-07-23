/*
*  Copyright (c) 2020 Boomi, Inc.
*/
package com.boomi.connector.util;

import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.model.PutEventSchemaType;
import com.boomi.connector.model.PutEventResponse;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;

/**
 * Helper class to create the request and response profile Json Schema.
 * 
 * @author swastik.vn
 *
 */
public class AWSEventBridgeSchemaBuilder {
	private static final   ObjectMapper mapper =  new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
	
	private AWSEventBridgeSchemaBuilder() {

	}

	private static Logger logger = Logger.getLogger(AWSEventBridgeSchemaBuilder.class.getName());

	/**
	 * Creates the JSON schema for request profile
	 * 
	 * @param objectTypeId
	 * @param operationType 
	 * @return JSON String
	 */
	public static String getInputJsonSchema(String objectTypeId, String operationType) {
		String json = null;
		try {
			SchemaFactoryWrapper wrapper = new SchemaFactoryWrapper();
			if (objectTypeId.equalsIgnoreCase(AWSEventBridgeConstant.PUTEVENTS)&& operationType.equals(AWSEventBridgeConstant.CREATE)) {
				mapper.acceptJsonFormatVisitor(PutEventSchemaType.class, wrapper);
			}
			JsonSchema schema = wrapper.finalSchema();
			json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
			logger.info("Json Schema for input");
			logger.info(json);
		} catch (Exception e) {
			throw new ConnectorException(AWSEventBridgeConstant.FAILEDRQUESTSCHEMA, e);
		}

		return json;

	}

	/**
	 * Create the JSON Schema for response profile
	 * 
	 * @param objectTypeId
	 * @param operationType 
	 * @return JSON String
	 */
	public static String getJsonOutPutSchema(String objectTypeId, String operationType) {

		String json = null;

		try {
			SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
			if (objectTypeId.equalsIgnoreCase(AWSEventBridgeConstant.PUTEVENTS)&& operationType.equals(AWSEventBridgeConstant.CREATE)) {
				mapper.acceptJsonFormatVisitor(PutEventResponse.class, visitor);
			}
			JsonSchema schema = visitor.finalSchema();

			json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
			logger.info("Json Schema for output {}");
			logger.info(json);
		} catch (Exception e) {
			throw new ConnectorException(AWSEventBridgeConstant.FAILEDRESPONESCHEMAERROR, e);
		}

		return json;
	}
}