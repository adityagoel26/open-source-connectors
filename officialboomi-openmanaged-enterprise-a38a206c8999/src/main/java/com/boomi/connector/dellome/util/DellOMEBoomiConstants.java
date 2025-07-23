// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome.util;

import java.util.Arrays;
import java.util.List;

public interface DellOMEBoomiConstants {

	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String IPADDRESS = "ipaddress";
	public static final String ENABLESSL = "enablessl";
	
	public static final String METHOD_GET = "GET";
	public static final String METHOD_POST = "POST";
	public static final String METHOD_PUT = "PUT";
	
	public static final String ALERTS = "ALERTS";
	public static final String ODATA_NODE_CONTEXT = "@odata.context";
	public static final String ODATA_NODE_ID = "@odata.id";
	public static final String ODATA_NODE_TYPE = "@odata.type";
	public static final String ODATA_NODE_COUNT = "@odata.count";
	public static final String ODATA_NODE_ALERTS = "value";
	public static final String ODATA_NODE_MSGEXTINFO = "@Message.ExtendedInfo";
	public static final String ODATA_NODE_MESSAGE = "Message";

	public static final String RESPONSE_ERROR = "error";
	public static final String RESPONSE_MESSAGE = "message";

	public static final String API_ENDPOINT_ALERTS = "/api/AlertService/Alerts";
	public static final String API_ENDPOINT_ALERTS_METADATA = "/api/schemas/Alerts.xml";

	/**
	 * Enum representing the supported query operations (these names match the ids
	 * specified in the connector descriptor). Each operation has a prefix field
	 * which is used when sending the query filter to the service. 
	 * used eq, ge, le.
	 */
	public static enum QueryOp {
		EQUALS("eq"), NOT_EQUALS("ne"), GREATER_THAN("gt"), LESS_THAN("lt"), GREATER_THAN_OR_EQUALS("ge"),
		LESS_THAN_OR_EQUALS("le"), IN_LIST("in");
		
		private final String _prefix;

		private QueryOp(String prefix) {
			_prefix = prefix;
		}

		public String getPrefix() {
			return _prefix;
		}
	}
	
	public static enum FieldType {
		INT("Id", "SeverityType"),
		STRING("TimeStamp");
		
		private final List<String> values;

		FieldType(String ...values) {
	        this.values = Arrays.asList(values);
	    }

	    public List<String> getValues() {
	        return values;
	    }
	    
	    public static FieldType find(String name) {
	        for (FieldType type : FieldType.values()) {
	            if (type.getValues().contains(name)) {
	                return type;
	            }
	        }
	        return null;
	    }
	}


}
