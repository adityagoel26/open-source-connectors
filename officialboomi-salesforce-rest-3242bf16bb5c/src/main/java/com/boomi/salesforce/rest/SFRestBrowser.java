// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.salesforce.rest.builder.schema.XMLCreateTreeSchemaBuilder;
import com.boomi.salesforce.rest.builder.schema.XMLQuerySchemaBuilder;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.controller.metadata.BrowseController;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;

import java.util.Collection;
import java.util.List;

public class SFRestBrowser extends BaseBrowser implements ConnectionTester {
    @SuppressWarnings("unchecked")
    protected SFRestBrowser(SFRestConnection conn) {
        super(conn);
    }

    /**
     * Import fields and relations for a given SObject
     */
    @Override
    public ObjectDefinitions getObjectDefinitions(String sobjectName, Collection<ObjectDefinitionRole> roles) {
        SFRestConnection connection = getConnection();
        try {
            connection.initialize();
            ObjectDefinitions defs = new ObjectDefinitions();

            long parentsDepth = getContext().getOperationProperties()
                                            .getLongProperty(Constants.PARENT_DEPTH_DESCRIPTOR, 0L);
            long childrenDepth = getContext().getOperationProperties()
                                             .getLongProperty(Constants.CHILDREN_DEPTH_DESCRIPTOR, 0L);

            SObjectController controller = new SObjectController(connection);

            ObjectDefinition def;
            if (Constants.CREATE_TREE_CUSTOM_DESCRIPTOR.equals(getContext().getCustomOperationType())) {
                def = new XMLCreateTreeSchemaBuilder(controller, childrenDepth).generateSchema(sobjectName);
            } else {
                def = new XMLQuerySchemaBuilder(controller, parentsDepth, childrenDepth).generateSchema(sobjectName);
            }
            return defs.withDefinitions(def);
        } finally {
            connection.close();
        }
    }

    /**
     * Import SObjects list
     */
    @Override
    public ObjectTypes getObjectTypes() {
        List<String> sobjects;
        SFRestConnection connection = getConnection();
        try {
            connection.initialize();
            BrowseController controller = new BrowseController(connection);
            switch (getContext().getOperationType()) {
                case QUERY:
                    sobjects = controller.listSObjects(Constants.QUERYABLE);
                    break;
                case DELETE:
                    sobjects = controller.listSObjects(Constants.DELETABLE);
                    break;
                case CREATE:
                    // CREATE and CREATE_TREE Operation
                    sobjects = controller.listSObjects(Constants.CREATEABLE);
                    break;
                case UPDATE:
                case UPSERT:
                    sobjects = controller.listSObjects(Constants.UPDATEABLE);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        } finally {
            connection.close();
        }

        ObjectTypes returnedTypes = new ObjectTypes();
        for (String sobject : sobjects) {
            ObjectType current = new ObjectType();
            current.setId(sobject);
            current.withLabel(sobject);
            current.withHelpText(sobject);
            returnedTypes.getTypes().add(current);
        }
        return returnedTypes;
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }

    @Override
    public void testConnection() {
        try (SFRestConnection connection = getConnection()) {
            connection.initialize();
        }
    }
}