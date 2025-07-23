// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.builder.schema;

import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.model.SObjectRelation;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Responsible for dynamically building XML Schema for importing CREATE TREE profile
 */
public class XMLCreateTreeSchemaBuilder extends XMLSchemaBuilder {
    /**
     * Initializes the schema
     *
     * @param controller    controller to retrieve SObjects metadata
     * @param childrenDepth Integer, will import children up to level childrenDepth, 0 means no children, 1 means only
     *                      direct children, 2 means direct children and 1 level of CHILDREN, 3 means 3 level of
     *                      CHILDREN and so on up to 5 levels.
     */
    public XMLCreateTreeSchemaBuilder(SObjectController controller, long childrenDepth) {
        super(controller, 0L, childrenDepth);
    }

    /**
     * The intro function, generates the schema for the main SObject in the _schema Element
     *
     * @param sobjectName main SObject name
     * @return main element with name="SObjectTreeRequest" to be added to schema
     */
    @Override
    protected Element generateMainSObject(String sobjectName) {
        defineSObjectChildren(sobjectName, _childDepth + 1);

        // generates SObject fields with the parent and child relationships
        Element records = _doc.createElement(XSD_TAG_ELEMENT);
        records.setAttribute(ATTR_NAME, Constants.SALESFORCE_RECORDS);
        records.setAttribute(ATTR_TYPE, sobjectName + (_childDepth + 1));
        records.setAttribute(ATTR_MINOCCURS, "0");
        records.setAttribute(ATTR_MAXOCCURS, "unbounded");

        // append the generated SObject to the schema
        Element rootMain = _doc.createElement(XSD_TAG_ELEMENT);
        rootMain.setAttribute(ATTR_NAME, "SObjectTreeRequest");
        rootMain.appendChild(generateComplexType("SObjectTreeRequest", null, Collections.singletonList(records)));
        return rootMain;
    }

    private void defineSObjectChildren(String sobjectName, int remainingDepth) {
        if (remainingDepth == -1 || !_definedSet.add(sobjectName + remainingDepth)) {
            return;
        }
        SObjectModel curSObject = _sobjectController.buildSObject(sobjectName);
        Element childFieldsTypes;
        if (remainingDepth == 0) {
            childFieldsTypes = generateComplexTypeWithReferenceID(sobjectName + remainingDepth, curSObject.getFields());
        } else {
            defineSObjectChildren(sobjectName, 0);
            ArrayList<Element> childrenElements = generateChildElements(curSObject.getChildren(), remainingDepth);
            childFieldsTypes = generateComplexTypeExtendsComplexType(sobjectName + remainingDepth, sobjectName + "0",
                                                                     childrenElements);
        }
        // define in the schema to be referenced
        defineForReference(childFieldsTypes);
    }


    /**
     * Generates schema child elements and their parents
     *
     * @param children list of SObjectRelation describes the children relationship
     * @return List of Element ready to be appended in the schema
     */
    protected ArrayList<Element> generateChildElements(List<SObjectRelation> children, int remainingDepth) {
        ArrayList<Element> childElements = new ArrayList<>();
        for (SObjectRelation childRelation : children) {
            defineSObjectChildren(childRelation.getRelationSObjectName(), remainingDepth - 1);

            Element childRecords = _doc.createElement(XSD_TAG_ELEMENT);
            childRecords.setAttribute(ATTR_NAME, Constants.SALESFORCE_RECORDS);
            // reference to child fields and the PARENT_OF_CHILD_DEPTH level parent
            childRecords.setAttribute(ATTR_TYPE, childRelation.getRelationSObjectName() + (remainingDepth - 1));
            childRecords.setAttribute(ATTR_MINOCCURS, "0");
            childRecords.setAttribute(ATTR_MAXOCCURS, "unbounded");

            Element relationshipElementWrapper = generateComplexType(null, null, new ArrayList<>(
                    Collections.singletonList(childRecords)));

            // child outer relationship element
            Element childrenWrapper = _doc.createElement(XSD_TAG_ELEMENT);
            childrenWrapper.setAttribute(ATTR_NAME, childRelation.getRelationshipName());
            childrenWrapper.setAttribute(ATTR_MINOCCURS, "0");
            childrenWrapper.appendChild(relationshipElementWrapper);

            childElements.add(childrenWrapper);
        }
        return childElements;
    }

    private Element generateComplexTypeWithReferenceID(String rootName, List<SObjectField> fields) {
        Element complexType = generateComplexType(rootName, fields, null);
        if (fields != null) {
            // has fields means a 'records' element set @referenceId attribute
            Element attributeRef = _doc.createElement(TAG_ATTRIBUTE);
            attributeRef.setAttribute(ATTR_NAME, "referenceId");
            complexType.appendChild(attributeRef);
        }
        return complexType;
    }
}
