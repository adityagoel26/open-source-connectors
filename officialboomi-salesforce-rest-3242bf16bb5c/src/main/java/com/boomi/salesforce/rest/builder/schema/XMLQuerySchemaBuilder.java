// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.builder.schema;

import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.model.SObjectRelation;
import org.w3c.dom.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Responsible for dynamically building XML Schema for importing QUERY profile, handles the parent-child * relationship
 * hierarchy too
 */
public class XMLQuerySchemaBuilder extends XMLSchemaBuilder {
    /**
     * Initializes the schema
     *
     * @param controller    controller to retrieve SObjects metadata
     * @param parentsDepth  Integer, will import parents up to level parentsDepth
     * @param childrenDepth Integer, will import children up to level childrenDepth, 0 means no children, 1 means only
     *                      direct children, 2 means direct children and their 1 level of PARENTS
     */
    public XMLQuerySchemaBuilder(SObjectController controller, long parentsDepth, long childrenDepth) {
        super(controller, parentsDepth, childrenDepth);
    }

    /**
     * The main function, generates the main XSD element representing the imported SObject, to be directly appended to
     * the _schema Element
     *
     * @param sobjectName main SObject name
     * @return main element with name="records" to be added to schema
     */
    @Override
    protected Element generateMainSObject(String sobjectName) {
        SObjectModel sObjectModel = _sobjectController.buildSObject(sobjectName);

        // generates parent/child relations IF ANY
        ArrayList<Element> relationElements = new ArrayList<>();
        if (_childDepth >= 0) {
            relationElements = generateChildElements(sObjectModel.getChildren());
        }

        if (_parentDepth > -1) {
            ArrayList<Element> parentElements = new ArrayList<>();
            for (SObjectRelation parentRelation : sObjectModel.getParents()) {
                // define the child fields and PARENT_DEPTH level parent
                defineSObjectParents(parentRelation.getRelationSObjectName(), _parentDepth);

                Element parentRecords = _doc.createElement(XSD_TAG_ELEMENT);
                parentRecords.setAttribute(ATTR_NAME, parentRelation.getRelationshipName());
                // reference to child fields and the PARENT_DEPTH level parent
                parentRecords.setAttribute(ATTR_TYPE, parentRelation.getRelationSObjectName() + _parentDepth);
                parentRecords.setAttribute(ATTR_MINOCCURS, "0");
                parentElements.add(parentRecords);
            }
            relationElements.addAll(parentElements);
        }

        // generates SObject fields with the parent and child relationships
        Element sobject = _doc.createElement(XSD_TAG_ELEMENT);
        sobject.setAttribute(ATTR_NAME, Constants.SALESFORCE_RECORDS);

        defineSObjectParents(sobjectName, 0);
        Element complexType = generateComplexTypeExtendsComplexType(sobjectName, sobjectName + "0", relationElements);
        sobject.appendChild(complexType);
        return sobject;
    }


    /**
     * Generates schema child elements and their parents
     *
     * @param children list of SObjectRelation describes the children relationship
     * @return List of Element ready to be appended in the schema
     */
    protected ArrayList<Element> generateChildElements(List<SObjectRelation> children) {
        ArrayList<Element> childElements = new ArrayList<>();
        for (SObjectRelation childRelation : children) {
            // define the child fields and PARENT_OF_CHILD_DEPTH level parent
            defineSObjectParents(childRelation.getRelationSObjectName(), _childDepth);

            Element childRecords = _doc.createElement(XSD_TAG_ELEMENT);
            childRecords.setAttribute(ATTR_NAME, Constants.SALESFORCE_RECORDS);
            // reference to child fields and the PARENT_OF_CHILD_DEPTH level parent
            childRecords.setAttribute(ATTR_TYPE, childRelation.getRelationSObjectName() + _childDepth);
            childRecords.setAttribute(ATTR_MINOCCURS, "0");
            childRecords.setAttribute(ATTR_MAXOCCURS, "unbounded");

            // Append the 'done' and 'totalSize' tags
            Element salesforceDone = _doc.createElement(XSD_TAG_ELEMENT);
            salesforceDone.setAttribute(ATTR_NAME, TAG_SALESFORCE_DONE);
            salesforceDone.setAttribute(ATTR_TYPE, TAG_SALESFORCE_DONE);

            Element salesforceSize = _doc.createElement(XSD_TAG_ELEMENT);
            salesforceSize.setAttribute(ATTR_NAME, TAG_SALESFORCE_SIZE);
            salesforceSize.setAttribute(ATTR_TYPE, TAG_SALESFORCE_SIZE);

            Element relationshipElementWrapper = generateComplexType(childRelation.getRelationshipName(), null,
                                                                     new ArrayList<>(
                                                                             Arrays.asList(salesforceDone, childRecords,
                                                                                           salesforceSize)));

            // child outer relationship element
            Element childrenWrapper = _doc.createElement(XSD_TAG_ELEMENT);
            childrenWrapper.setAttribute(ATTR_NAME, childRelation.getRelationshipName());
            childrenWrapper.setAttribute(ATTR_MINOCCURS, "0");
            // ignore the children fields for sorts
            childrenWrapper.setAttribute(ATTR_TYPE, KEY_IGNORE_SORT);
            childrenWrapper.appendChild(relationshipElementWrapper);

            childElements.add(childrenWrapper);
        }
        return childElements;
    }

    /**
     * Recursive function does Depth First Search (DFS), to build all the definitions for SObjects.
     * <br>
     * When remainingDepth equals 0 will generate the fields for sobjectName.
     * <br>
     * Otherwise will extends the the definition for remainingDepth-1 and append the parents to it.
     * <br>
     * Definitions includes:
     * <br>
     * - The fields of the SObject <br> - The relation parents of the SObject <br> - The Fields of the parents <br> -
     * Parents of <br> - the parents up to 'remainingDepth' levels of parents<br>
     * <br>
     * Worst case Complexity for this function in the whole import operation is the total number of parents*5 + total
     * number of SObjects and fields
     */
    protected void defineSObjectParents(String sobjectName, int remainingDepth) {
        if (remainingDepth == -1 || !_definedSet.add(sobjectName + remainingDepth)) {
            return;
        }
        SObjectModel curSObject = _sobjectController.buildSObject(sobjectName);
        Element parentFieldsTypes;
        if (remainingDepth == 0) {
            parentFieldsTypes = generateComplexType(sobjectName + remainingDepth, curSObject.getFields(), null);
        } else {
            defineSObjectParents(sobjectName, 0);
            ArrayList<Element> parentElements = new ArrayList<>();

            for (SObjectRelation parentRelation : curSObject.getParents()) {
                // recursively define the next parent level to be referenced by the current level
                defineSObjectParents(parentRelation.getRelationSObjectName(), remainingDepth - 1);

                // reference the previously defined parent
                Element parentFields = _doc.createElement(XSD_TAG_ELEMENT);
                parentFields.setAttribute(ATTR_NAME, parentRelation.getRelationshipName());
                parentFields.setAttribute(ATTR_TYPE, parentRelation.getRelationSObjectName() + (remainingDepth - 1));
                parentFields.setAttribute(ATTR_MINOCCURS, "0");

                parentElements.add(parentFields);
            }

            parentFieldsTypes = generateComplexTypeExtendsComplexType(sobjectName + remainingDepth, sobjectName + "0",
                                                                      parentElements);
        }

        defineForReference(parentFieldsTypes);
    }

}
