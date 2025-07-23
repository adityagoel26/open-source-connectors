// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome.util;

/**
 * Contains schema definitions for various object types in the DELL OME system.
 */
public interface DellOMEObjectSchemas {

	//The current version of the connector supports only the type "ALERTS". 
	public static final String SCHEMA_ALERTS = "{\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"type\":\"object\",\"properties\":{\"Id\":{\"type\":\"integer\"},\"SeverityType\":{\"type\":\"integer\"},\"SeverityName\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertDeviceId\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"},\"AlertDeviceName\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertDeviceType\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"},\"AlertDeviceIpAddress\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertDeviceMacAddress\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertDeviceIdentifier\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertDeviceAssetTag\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"DefinitionId\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"},\"CatalogName\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"CategoryId\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"},\"CategoryName\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"SubCategoryId\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"},\"SubCategoryName\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"StatusType\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"},\"StatusName\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"TimeStamp\":{\"type\":\"string\"},\"Message\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"EemiMessage\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"RecommendedAction\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertMessageId\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertVarBindDetails\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertMessageType\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"MessageArgs\":{\"filterable\":false,\"selectable\":false,\"type\":\"string\"},\"AlertDeviceGroup\":{\"filterable\":false,\"selectable\":false,\"type\":\"integer\"}}}";

}
