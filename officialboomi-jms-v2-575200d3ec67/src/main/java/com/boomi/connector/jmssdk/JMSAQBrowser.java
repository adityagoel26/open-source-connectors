// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.OracleAQAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.pool.AdapterPool;
import com.boomi.connector.jmssdk.pool.AdapterPoolManager;
import com.boomi.connector.jmssdk.util.JMSConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Browser implementation for Oracle AQ
 */
public class JMSAQBrowser extends JMSBrowser {

    private static final String DYNAMIC_DESTINATION_AQ_ID = "::" + JMSConstants.DYNAMIC_DESTINATION_ID;
    private static final ObjectType DYNAMIC_DESTINATION = new ObjectType().withLabel("Dynamic Destination").withId(
            DYNAMIC_DESTINATION_AQ_ID);

    protected JMSAQBrowser(JMSConnection<BrowseContext> connection) {
        super(connection);
    }

    @Override
    public ObjectTypes getObjectTypes() {
        return new ObjectTypes().withTypes(DYNAMIC_DESTINATION).withTypes(getQueues());
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectType, Collection<ObjectDefinitionRole> roles) {
        Properties fieldsProperties = loadImportableFieldProperties();
        ObjectDefinitions definitions = new ObjectDefinitions();
        OperationType operationType = getContext().getOperationType();
        AdapterPool adapterPool = AdapterPoolManager.getPool(getConnection());
        GenericJndiBaseAdapter adapter = null;
        try {
            adapter = adapterPool.createAdapter();
            TargetDestination targetDestination = adapter.createTargetDestination(objectType);
            ObjectDefinition objectDefinition = adapter.getObjectDefinition(targetDestination);
            switch (operationType) {
                case CREATE:
                    addSendDefinitions(definitions, objectDefinition, targetDestination.isProfileRequired(), roles);
                    if (JMSConstants.DYNAMIC_DESTINATION_ID.equals(targetDestination.getName())) {
                        definitions.withOperationFields(buildDestinationField(fieldsProperties, operationType),
                                buildDestinationTypeField(fieldsProperties, null));
                    }
                    break;
                case QUERY:
                case LISTEN:
                    addGetDefinitions(definitions, objectDefinition, targetDestination.isProfileRequired(), roles);
                    if (JMSConstants.DYNAMIC_DESTINATION_ID.equals(targetDestination.getName())) {
                        definitions.withOperationFields(buildDestinationField(fieldsProperties, operationType));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("invalid operation type: " + operationType);
            }

            if (!JMSConstants.DYNAMIC_DESTINATION_ID.equals(targetDestination.getName())) {
                //when it's not dynamic destination, we add the data type as destination type in order to show it in
                // the UI
                fieldsProperties.setProperty("destination_type.options",
                        String.format("%s:%s", targetDestination.getDataType(), targetDestination.getDataType()));
                definitions.withOperationFields(buildDestinationTypeField(fieldsProperties, null));
            }

            return definitions;
        } finally {
            adapterPool.releaseAdapter(adapter);
        }
    }

    /**
     * get back the enabled queues and create an object type for each of them.
     *
     * @return List<ObjectType>
     */
    private List<ObjectType> getQueues() {
        AdapterPool adapterPool = AdapterPoolManager.getPool(getConnection());
        GenericJndiBaseAdapter adapter = null;
        try {
            adapter = adapterPool.createAdapter();
            List<ObjectType> objectTypes = new ArrayList<>();
            Collection<TargetDestination> targetDestinationList = ((OracleAQAdapter) adapter).getAllDestinations(
                    getConnection().getBrowseFilter());
            for (TargetDestination targetDestination : targetDestinationList) {
                objectTypes.add(new ObjectType().withId(targetDestination.getId())
                        .withLabel(String.format("%s:%s", targetDestination.getType(), targetDestination.getName())));
            }
            return objectTypes;
        } finally {
            adapterPool.releaseAdapter(adapter);
        }
    }
}
