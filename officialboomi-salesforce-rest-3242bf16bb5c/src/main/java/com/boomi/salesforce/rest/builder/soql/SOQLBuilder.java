// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.builder.soql;

import com.boomi.connector.api.QueryFilter;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Helper class responsible for building a SOQL query string based on the Platform input parameters
 */
public class SOQLBuilder {

    private static final String CHILD_RECORDS_TAG = "records/";
    private static final int FIELD_PARTS_COUNT = 2;

    /**
     * Responsible to build the WHERE and ORDER BY and LIMIT parts of the SOQL
     */
    private final SOQLFilterBuilder _filterBuilder;
    private final String _sobjectName;
    /**
     * Contains lists of children sub-queries RelationshipName to the Fields
     */
    private final Map<String, List<String>> _children;
    /**
     * Contains lists of fields that are part of the main query
     */
    private final List<String> _fields;

    /**
     * The selectedFields from the platform are given in these formats:
     * <br>
     * Main SObject field:
     * <br>
     * [field]
     * <br>
     * Child field:
     * <br>
     * [relationshipName]/records/[field]
     * <br>
     * Parent of Child field:
     * <br>
     * [relationshipName]/records/[RelationshipName]/[field]
     * <br>
     * Parent of SObject field:
     * <br>
     * [relationshipName]/[field]
     * <br>
     * Parent of Parent SObject field up to 5 levels:
     * <br>
     * [relationshipName]/[relationshipName]/[field]
     * <br>
     * [relationshipName]/[relationshipName]/[relationshipName]/[field]
     * <br>
     * [relationshipName]/[relationshipName]/[relationshipName]/[relationshipName]/[field]
     * <br>
     * [relationshipName]/[relationshipName]/[relationshipName]/[relationshipName]/[relationshipName]/[field]
     * <br>
     *
     * @param controller     controller to retrieve SObjects metadata
     * @param sobjectName    the imported SObject name
     * @param selectedFields the list of Platform selected fields
     * @param filter         QueryFilter object contains Platform Filters and Sorts
     * @param limit          integer value of the Limit Platform field
     */
    public SOQLBuilder(SObjectController controller, String sobjectName, List<String> selectedFields,
                       QueryFilter filter, Long limit) {
        _sobjectName = sobjectName;
        _filterBuilder = new SOQLFilterBuilder(controller, sobjectName, filter, limit);
        _children = new HashMap<>();
        _fields = new ArrayList<>();
        fillChildrenAndFields(selectedFields, _children, _fields);
    }

    /**
     * Initializes the selected fields, split them into two types, main SObject fields and Children fields.
     * <p>
     * Reformat the fields name from the Platform format to Salesforce format, by replacing slashes '/' with dots '.'
     *
     * @param selectedFields list of Platform selected fields
     */
    private static void fillChildrenAndFields(List<String> selectedFields, Map<String, List<String>> children,
            List<String> fields) {
        for (String field : selectedFields) {
            if (isChild(field)) {
                String[] split = field.split("/", FIELD_PARTS_COUNT);
                String relationshipName = split[0];
                String fieldName = split[1].replace(CHILD_RECORDS_TAG, "").replace('/', '.');

                List<String> list = children.computeIfAbsent(relationshipName, k -> new ArrayList<>());
                list.add(fieldName);
            } else {
                fields.add(field.replace('/', '.'));
            }
        }
    }

    /**
     * Returns true if the field name, before canonize it, will be part of a sub-query child relationships.
     * <br>
     * A field is a childField if it contains "records/" tag (i.e. "Contacts/records/Id" is a child field)
     *
     * @param fieldName field name as it came from the platform
     * @return true if the fieldName is in child sub-query
     */
    private static boolean isChild(String fieldName) {
        return fieldName.contains(CHILD_RECORDS_TAG);
    }

    /**
     * Generates the SELECT Clause contains comma separated of the selected fields.
     * <br>
     * Build first both parent and child fields, then calls generateChildSelectClauses() method to build the child
     * fields
     *
     * @return String contains the SELECT clause with the child relationships
     */
    private String generateSelectClause() {
        StringBuilder select = new StringBuilder("SELECT ");

        boolean isFirstField = true;
        for (String field : _fields) {
            if (isFirstField) {
                isFirstField = false;
            } else {
                // if not a first field add comma before field
                select.append(",");
            }
            select.append(field);
        }

        String childSubQuery = generateChildSelectClauses();
        if (StringUtil.isNotBlank(childSubQuery)) {
            if (!isFirstField) {
                // if not a first field add comma before sub-query
                select.append(",");
            }
            select.append(childSubQuery);
        }
        select.append(" FROM ").append(_sobjectName);
        return select.toString();
    }

    /**
     * Generates the Sub-SELECT Clauses for the children relationship with its filters
     *
     * @return String contains the sub-SELECT of the child relationships
     */
    private String generateChildSelectClauses() {
        StringBuilder returnedStatement = new StringBuilder();

        boolean isFirstFieldInReturned = true;
        for (Entry<String, List<String>> childEntry : _children.entrySet()) {
            StringBuilder currentSubSelect = new StringBuilder();

            boolean isFirstFieldInCurrent = true;
            for (String field : childEntry.getValue()) {
                if (isFirstFieldInCurrent) {
                    isFirstFieldInCurrent = false;
                    currentSubSelect.append("(SELECT ");
                } else {
                    // if not a first field add comma before field
                    currentSubSelect.append(",");
                }
                currentSubSelect.append(field);
            }

            // if subQuery not empty append it to returned statements
            if (!isFirstFieldInCurrent) {
                currentSubSelect.append(" FROM ").append(childEntry.getKey())
                                .append(_filterBuilder.generateWhereClause(childEntry.getKey())).append(")");
                // if not a first sub-query add comma before sub-query
                if (isFirstFieldInReturned) {
                    isFirstFieldInReturned = false;
                } else {
                    returnedStatement.append(",");
                }
                returnedStatement.append(currentSubSelect);
            }
        }
        return returnedStatement.toString();
    }

    /**
     * Generates the SOQL query ready for execution, includes SELECT, Relationships, WHERE, ORDER BY and LIMIT
     *
     * @return the SOQL query
     */
    public String generateSOQLQuery() {
        return generateSelectClause() + _filterBuilder.generateWhereClause(null) +
               _filterBuilder.generateOrderByClause() + _filterBuilder.generateLimitClause();
    }
}
