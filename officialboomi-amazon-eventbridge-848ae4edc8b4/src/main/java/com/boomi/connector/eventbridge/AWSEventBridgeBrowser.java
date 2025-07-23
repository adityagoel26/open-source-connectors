/*
*  Copyright (c) 2020 Boomi, Inc.
*/

package com.boomi.connector.eventbridge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.util.AWSEventBridgeActionEnum;
import com.boomi.connector.util.AWSEventBridgeSchemaBuilder;
import com.boomi.connector.util.AWSEventBridgeUtil;
import com.boomi.connector.util.BaseBrowser;

/**
 * This class helps to do the browser operation including selecting the object
 * type and creating the request and response profile.
 * 
 * @author swastik.vn
 */
public class AWSEventBridgeBrowser extends BaseBrowser {
	private static final Logger LOGGER = Logger.getLogger(AWSEventBridgeBrowser.class.getName());

	protected AWSEventBridgeBrowser(AWSEventBridgeConnection conn) {
		super(conn);
	}

	/**
	 * Returns the object definitions for Input and Output Response Profile.
	 * 
	 * @param objectTypeId
	 * @param roles
	 * @return ObjectDefinitions
	 */
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
		AWSEventBridgeConnection con = this.getConnection();
		String operationType=con.getContext().getOperationType().toString();
		ObjectDefinitions objdefs = new ObjectDefinitions();
		String jsonSchema = null;
		try {
			for (ObjectDefinitionRole role : roles) {
				ObjectDefinition objdef = new ObjectDefinition();
				switch (role) {

				case INPUT:
					jsonSchema = AWSEventBridgeSchemaBuilder.getInputJsonSchema(objectTypeId,operationType);
					objdef.setElementName("");
					objdef.setJsonSchema(jsonSchema);
					objdef.setOutputType(ContentType.JSON);
					objdef.setInputType(ContentType.JSON);
					objdefs.getDefinitions().add(objdef);
					break;
				case OUTPUT:
					jsonSchema = AWSEventBridgeSchemaBuilder.getJsonOutPutSchema(objectTypeId,operationType);
					objdef.setElementName("");
					objdef.setJsonSchema(jsonSchema);
					objdef.setOutputType(ContentType.JSON);
					objdef.setInputType(ContentType.JSON);
					objdefs.getDefinitions().add(objdef);
					break;
				default:
					break;
				}
			}
		} catch (ConnectorException ex) {
			LOGGER.info("Exception occured while creating Request and Response profile :" + ex.getMessage());
		}

		return objdefs;
	}

	/**
	 * This method will return all the event names as Object types.
	 * 
	 * @return ObjectTypes
	 */

	@Override
	public ObjectTypes getObjectTypes() {
		AWSEventBridgeConnection con = this.getConnection();
		BrowseContext browseContext = con.getContext();
		ObjectTypes objtypes = new ObjectTypes();
		List<ObjectType> objTypeList = new ArrayList<>();
		String optype = browseContext.getOperationType().toString();
		List<String> bus = this.getEvents(optype);
		for (String event : bus) {
			ObjectType objtype = new ObjectType();
			objtype.setId(event);
			objTypeList.add(objtype);
		}
		objtypes.getTypes().addAll(objTypeList);
		return objtypes;
	}

	/**
	 * This method will take the operation name and return the list of events
	 * associated to it.
	 * 
	 * @param optype
	 * @return List<String>
	 */
	
	private List<String> getEvents(String optype) {
		List<String> events = new ArrayList<>();
		for (AWSEventBridgeActionEnum m : AWSEventBridgeActionEnum.values()) {
			if (m.showValue().equalsIgnoreCase(optype)) {
				events.add(AWSEventBridgeUtil.convertString(m.toString()));
			}
		}
		return events;
	}

	
	/**
	 * This method will return the AWSEventBridgeConnection
	 * 
	 * 
	 * @return AWSEventBridgeConnection
	 */
	@Override
	public AWSEventBridgeConnection getConnection() {
		return (AWSEventBridgeConnection) super.getConnection();
	}

}