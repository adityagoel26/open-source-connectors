//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.liveoptics;

import java.util.Collection;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.liveoptics.utils.LiveOpticsConstants;
import com.boomi.connector.util.BaseBrowser;

/**
 * @author Naveen Ganachari
 *
 * ${tags}
 */
public class LiveOpticsConnectorBrowser extends BaseBrowser {
	

    protected LiveOpticsConnectorBrowser(LiveOpticsConnectorConnection conn) {
        super(conn);
    }

	@Override
	/**
	 * This method creates the object Definitions on the click of button import.
	 * It is used when we are setting up the Operation context
	 */
	public ObjectDefinitions getObjectDefinitions(String objectTypeId,
			Collection<ObjectDefinitionRole> roles) {
        try {
            ObjectDefinitions defs = new ObjectDefinitions();
            ObjectDefinition def = new ObjectDefinition();
            def.setElementName("");
            def.setInputType(ContentType.JSON);
            def.setOutputType(ContentType.JSON);
            def.setJsonSchema(this.getConnection().getMetadata());
            defs.getDefinitions().add(def);
            return defs;
        }
        catch (Exception e) {
            throw new ConnectorException((Throwable)e);
        }
    }

	@Override
	/**
	 * This method creates the object Types on the click of button import.
	 * It is used when we are setting up the Operation context
	 */
	public ObjectTypes getObjectTypes() {
        try {
            ObjectTypes types = new ObjectTypes();
            ObjectType type = new ObjectType();
            types.getTypes().add(type);
            type.setId(LiveOpticsConstants.ID);
            return types;
        }
        catch (Exception e) {
            throw new ConnectorException((Throwable)e);
        }
    }

	@Override
    public LiveOpticsConnectorConnection getConnection() {
        return (LiveOpticsConnectorConnection) super.getConnection();
    }
}