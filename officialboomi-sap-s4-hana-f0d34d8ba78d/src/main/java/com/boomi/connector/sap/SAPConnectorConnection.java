// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sap;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sap.util.SAPConstants;
import com.boomi.connector.util.BaseConnection;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorConnection extends BaseConnection {
	
	

	private String host;
	private String apikey;
	private String businessHubUserName;
	private String businessHubPassword;
	private String sapUserId;
	private String sapPassword;
	
	public SAPConnectorConnection(BrowseContext context) {
		super(context);
		PropertyMap propertyMap = context.getConnectionProperties();
		host = propertyMap.getProperty(SAPConstants.HOST);
		apikey = propertyMap.getProperty(SAPConstants.APIKEY);
		businessHubUserName = propertyMap.getProperty(SAPConstants.BUSINESS_HUB_USERNAME);
		businessHubPassword = propertyMap.getProperty(SAPConstants.BUSINESS_HUB_PASS);
		sapUserId = propertyMap.getProperty(SAPConstants.SAP_USER);
		sapPassword = propertyMap.getProperty(SAPConstants.SAP_PASS);

	}

	public String getBusinessHubUserName() {
		return businessHubUserName;
	}

	public String getBusinessHubPassword() {
		return businessHubPassword;
	}

	public String getApikey() {
		return apikey;
	}

	public String getHost() {
		return host;
	}
	
	public String getSapUserId() {
		return sapUserId;
	}

	public String getSapPassword() {
		return sapPassword;
	}



}