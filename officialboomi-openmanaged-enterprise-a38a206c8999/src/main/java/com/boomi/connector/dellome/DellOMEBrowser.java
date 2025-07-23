// Copyright (c) 2016 Boomi, Inc.

package com.boomi.connector.dellome;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.logging.Logger;

import javax.net.ssl.HttpsURLConnection;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.util.BaseBrowser;

import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.*;
import static com.boomi.connector.dellome.util.DellOMEObjectSchemas.*;

public class DellOMEBrowser extends BaseBrowser implements ConnectionTester{
	
	private Logger logger = Logger.getLogger(DellOMEBrowser.class.getName());
	
    public DellOMEBrowser(DellOMEConnection conn) {
        super(conn);
    }

	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId,
			Collection<ObjectDefinitionRole> roles) {

		String jsonString = null;

		ObjectDefinitions defs = new ObjectDefinitions();

		//The current version of the connector supports only the type "ALERTS".
		switch (objectTypeId) {
	        case ALERTS:
	    		jsonString = SCHEMA_ALERTS;
	        	break;
	        default: //Defaulting to the ALERTS type schema
	    		jsonString = SCHEMA_ALERTS;
        }

		ObjectDefinition def = new ObjectDefinition();
        def.setElementName("");//added for schema JsonPointerException: illegal pointer: expected a slash to separate tokens
        def.setInputType(ContentType.JSON);
        def.setOutputType(ContentType.JSON);
        def.setJsonSchema(jsonString);
        defs.getDefinitions().add(def);

        return defs;
	}

	@Override
	public ObjectTypes getObjectTypes() {

		ObjectTypes types = new ObjectTypes();

		// Currently the connector supports one Object type "ALERTS"
		ObjectType type = new ObjectType();
		type.setId(ALERTS);
		types.getTypes().add(type);

		return types;
	}

	@Override
    public DellOMEConnection getConnection() {
        return (DellOMEConnection) super.getConnection();
    }

	@Override
	public void testConnection() throws ConnectorException {
		HttpsURLConnection con = null;
		try {
			logger.info("DellOMEBrowser::testConnection() --> ipaddress : " + getConnection().getIpaddress());

			con = getConnection().getConnection("GET", getConnection().getBaseAlertsApiurl());

			if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
				logger.info("OMEBrowser::testConnection() Sucessfull : " + con.getResponseCode());
			} else {
				logger.info("OMEBrowser::testConnection() Failed --> responseCode : " + con.getResponseCode() 
														+ ", --> Error Message : " + con.getResponseMessage());
				throw new ConnectorException("Connection failed with responseCode " + con.getResponseCode() + " Error Message - " + con.getResponseMessage());
			}

		} catch (IOException e) {
			throw new ConnectorException("Could not establish a connection", e);
		} finally {
			if (null != con) {
				con.disconnect();
				con = null;
			}
		}

	}
}