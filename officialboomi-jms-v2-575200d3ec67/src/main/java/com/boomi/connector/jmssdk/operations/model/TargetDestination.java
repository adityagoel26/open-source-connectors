// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model;

import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.jmssdk.client.DestinationType;

import org.w3c.dom.Element;

public interface TargetDestination {

    /**
     * return the type queue or topic
     *
     * @return
     */
    String getType();

    /**
     * return the data type
     *
     * @return
     */
    String getDataType();

    /**
     * return the destination name according to the service
     *
     * @return
     */
    String getDestinationName();

    /**
     * return true when the destination has custom data type and its required a profile.
     *
     * @return
     */
    Boolean isProfileRequired();

    String getId();

    /**
     * return destination target name
     *
     * @return
     */
    String getName();

    /**
     * return destination type
     *
     * @return text_message, text_message_xml, map_message:,byte_message,adt_message
     */
    DestinationType getDestinationType();

    void setDestinationType(DestinationType destinationType);

    /**
     * return ObjectDefinition related to destination
     *
     * @return @{@link ObjectDefinition}
     */
    default ObjectDefinition getObjectDefinition() {
        return new ObjectDefinition();
    }

    /**
     * create and return object definition with schema and json schema
     *
     * @param schema
     * @param jsonSchema
     * @return @{@link ObjectDefinition}
     */
    default ObjectDefinition getObjectDefinition(Element schema, String jsonSchema) {
        return new ObjectDefinition().withSchema(schema).withCookie(jsonSchema).withElementName(getName())
                .withOutputType(ContentType.XML);
    }
}
