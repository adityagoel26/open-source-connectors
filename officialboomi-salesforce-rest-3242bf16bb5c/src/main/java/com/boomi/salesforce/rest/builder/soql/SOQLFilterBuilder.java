// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.builder.soql;

import com.boomi.connector.api.Expression;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.model.SObjectRelation;
import com.boomi.util.StringUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Responsible to build the WHERE, ORDER BY and LIMIT parts of the SOQL
 */
public class SOQLFilterBuilder {

    private static final String CHILD_RECORDS_TAG = "records/";
    private static final int FIELD_PARTS_COUNT = 2;
    private static final int FIELD_NAME_PARTS_COUNT = 3;
    private static final int FIELD_NAME_POSITION = 2;

    private static final Set<String> NO_QUOTES_FIELDS = getNoQuotesFields();
    private static final Map<String, String> OPERATORS_MAP = getOperatorsMap();
    private static final Set<String> COMMA_OPERATORS = getCommaOperators();

    /**
     * SObjectController responsible to retrieve metadata, will be used to get the type of the fields that are filtered,
     * to apply 'escape value' and 'warp with quotes'
     */
    private final SObjectController _controller;
    private final Expression _filter;
    private final List<Sort> _sort;
    private final String _sobjectName;
    private final Long _limit;


    /**
     * The FILTERS from the platform are given in these formats:
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
     * @param controller  controller to retrieve SObjects metadata
     * @param sobjectName the main SObject name, will be used to track the relationship in the filter
     * @param filter      platform QueryFilter includes the FILTERS and SORTS parts
     * @param limit       the value of the LIMIT part of the SOQL
     */
    public SOQLFilterBuilder(SObjectController controller, String sobjectName, QueryFilter filter, Long limit) {
        _filter = filter.getExpression();
        _sort = filter.getSort();
        _limit = limit;
        _sobjectName = sobjectName;
        _controller = controller;
    }

    /**
     * Return set of salesforce fields types that doesn't support wrapping quotes in SOQL filter
     */
    private static Set<String> getNoQuotesFields() {
        Set<String> fields = new HashSet<>();
        fields.add("datetime");
        fields.add("date");
        fields.add("time");
        fields.add("int");
        fields.add("double");
        fields.add("percent");
        fields.add("currency");
        fields.add("boolean");
        return Collections.unmodifiableSet(fields);
    }

    /**
     * Return of operators that support comma separated list for a single filter
     */
    private static Set<String> getCommaOperators() {
        Set<String> operators = new HashSet<>();
        operators.add("in");
        operators.add("not in");
        operators.add("includes");
        operators.add("excludes");
        return Collections.unmodifiableSet(operators);
    }

    /**
     * Maps the Platform operators from descriptor IDs to salesforce filter keys
     */
    private static Map<String, String> getOperatorsMap() {
        Map<String, String > operators = new HashMap<>();
        operators.put("LIKE", "like");
        operators.put("EQUALS", "=");
        operators.put("NOT_EQUALS", "!=");
        operators.put("GREATER_THAN", ">");
        operators.put("GREATER_THAN_OR_EQUALS", ">=");
        operators.put("LESS_THAN", "<");
        operators.put("LESS_THAN_OR_EQUALS", "<=");
        operators.put("IN_LIST", "in");
        operators.put("NOT_IN_LIST", "not in");
        operators.put("INCLUDES_LIST", "includes");
        operators.put("EXCLUDES_LIST", "excludes");

        operators.put("asc_nulls_first", "ASC NULLS FIRST");
        operators.put("asc_nulls_last", "ASC NULLS LAST");
        operators.put("desc_nulls_first", "DESC NULLS FIRST");
        operators.put("desc_nulls_last", "DESC NULLS LAST");

        return Collections.unmodifiableMap(operators);
    }

    /**
     * Converts the Platform Filter Expressions to Salesforce WHERE clause.
     * <br>
     * Recursive function handles nested simple and group expression of AND, OR operators.
     * <br>
     * If relationshipName is not null will be used to generate only the FILTER of the child sub-query with the
     * relationshipName, otherwise will generate only the main SObject filters
     *
     * @param expression       Platform expression contains the platform filters
     * @param relationshipName if NULL, will be used on main SObject filters. If not null will use the relationshipName
     *                         to generate FILTER on child relationship sub-query
     * @return String ready to be used in the WHERE clause of the main SObject or in the child sub-query
     */
    private String getWhereFilter(Expression expression, String relationshipName) {
        if (expression == null) {
            return null;
        }
        // base case for the simple expressions contains a single filter operator
        if (expression instanceof SimpleExpression) {
            // if the value of the filter is not specified by the user ignore the filter
            if (((SimpleExpression) expression).getArguments().isEmpty()) {
                return null;
            }
            String field = ((SimpleExpression) expression).getProperty();
            if (!isFieldInQuery(field, relationshipName)) {
                return null;
            }
            String operator = OPERATORS_MAP.get(((SimpleExpression) expression).getOperator());
            String value = ((SimpleExpression) expression).getArguments().get(0);
            return canonize(field) + " " + operator + " " + wrapFilterValue(value, field, operator);
        }

        /*
         * recursively handles sub-groups of the GroupingExpression(s) and separate them with the
         * correct Operator (i.e. AND, OR)
         */
        List<Expression> nestedExprs = ((GroupingExpression) expression).getNestedExpressions();
        StringBuilder retTerms = new StringBuilder();
        for (Expression nestedExpr : nestedExprs) {
            String term = getWhereFilter(nestedExpr, relationshipName);
            if (StringUtil.isNotBlank(term)) {
                if (retTerms.length() != 0) {
                    // separate the terms with OPERATOR (i.e. AND, OR)
                    retTerms.append(" ").append(((GroupingExpression) expression).getOperator()).append(" ");
                } else {
                    // surround the group terms with brackets
                    retTerms.append("(");
                }
                retTerms.append(term);
            }
        }
        if (retTerms.length() == 0) {
            return null;
        }
        return retTerms.append(")").toString();
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
     * Reformat fields from Platform format to Salesforce format.
     * <br>
     * Removes the child "records/" tag.
     * <br>
     * Replaces slashes '/' with dots '.'
     */
    private static String canonize(String field) {
        if (isChild(field)) {
            field = field.split("/", FIELD_PARTS_COUNT)[1].replace(CHILD_RECORDS_TAG, "");
        }
        return field.replace('/', '.');
    }

    /**
     * Returns true if the Platform selected field, before canonize, is part of this query filter by comparing
     * relationshipName.
     * <br>
     * The relationshipName can be null means this query is on main SObject.
     *
     * @param fieldName        field name as it came from the platform
     * @param relationshipName the relationship name of the child sub-query, or null if it is on the main SObject.
     * @return true if this field is part of this relationship name
     */
    private static boolean isFieldInQuery(String fieldName, String relationshipName) {
        if (isChild(fieldName)) {
            // field is child field or one of the parents
            return StringUtil.isNotBlank(relationshipName) && relationshipName.equals(fieldName.split("/",
                    FIELD_PARTS_COUNT)[0]);
        }
        // field is main SObject field or one of the parents
        return StringUtil.isBlank(relationshipName);
    }

    /**
     * Wraps the argument value of an operator if it needs wrapping with either quotes or splitting.
     * <br>
     * Decision depends on the type of the given field
     *
     * @param value     operator argument value
     * @param fieldName field of the operator
     * @param operator  operator
     * @return the argument value after formatting
     */
    private String wrapFilterValue(String value, String fieldName, String operator) {
        String fieldType = getFieldType(fieldName);
        if (NO_QUOTES_FIELDS.contains(fieldType)) {
            return value;
        }
        if (COMMA_OPERATORS.contains(operator)) {
            return "(" + commaSeparateValue(value) + ")";
        }
        return "'" + escapeStringValue(value) + "'";
    }

    /**
     * Returns the type of the given field, before canonize.
     * <br>
     * Tracks the relationship flow of the field up to the exact field and returns its type.
     * <br>
     * Handles fields of child sub-query, parent of child, parent of SObject up to 5 levels.
     *
     * @param fieldName field name as it came from the platform
     * @return Salesforce Type of given field
     */
    private String getFieldType(String fieldName) {
        // current SObject will track the relationship flow
        String currentSObject = _sobjectName;
        // if field is in child relationship will move the currentSObject to be a child SObject
        if (isChild(fieldName)) {
            String[] slitted = fieldName.split("/", FIELD_NAME_PARTS_COUNT);
            String relationshipName = slitted[0];
            fieldName = slitted[FIELD_NAME_POSITION];
            SObjectModel model = _controller.buildSObject(_sobjectName, false);
            for (SObjectRelation relation : model.getChildren()) {
                if (relationshipName.equals(relation.getRelationshipName())) {
                    currentSObject = relation.getRelationSObjectName();
                    break;
                }
            }
        }

        /*
         * if field is in parent relationship will move the currentSObject to be a parent SObject.
         * Supports all levels of parents
         */
        String[] splitted = fieldName.split("/");
        for (int i = 0; i < splitted.length - 1; ++i) {
            String relationshipName = splitted[i];
            SObjectModel model = _controller.buildSObject(currentSObject, false);
            for (SObjectRelation relation : model.getParents()) {
                if (relationshipName.equals(relation.getRelationshipName())) {
                    currentSObject = relation.getRelationSObjectName();
                    break;
                }
            }
        }

        /*
         * Searches for the field in the currentSObject fields, and return its type
         */
        SObjectModel model = _controller.buildSObject(currentSObject, false);
        for (SObjectField fieldModel : model.getFields()) {
            if (splitted[splitted.length - 1].equals(fieldModel.getName())) {
                return fieldModel.getType();
            }
        }

        // if not found for any reason return empty string so quotes will not be escaped
        return "";
    }

    /**
     * Appends single quotes around each term of the given comma separated filter value
     *
     * @param value the value of the filter
     * @return String the same value after wrapping quotes around each term
     */
    private static String commaSeparateValue(String value) {
        String[] values = value.split(",");
        StringBuilder ret = new StringBuilder();
        for (String s : values) {
            if (ret.length() != 0) {
                ret.append(",");
            }
            ret.append("'").append(escapeStringValue(s.trim())).append("'");
        }
        return ret.toString();
    }

    /**
     * Escapes argument values, escapes both single quote ' and backslash \
     *
     * @param filterValue a filter argument
     * @return the same argument after escaping characters
     */
    private static String escapeStringValue(String filterValue) {
        return filterValue.replace("\\", "\\\\").replace("'", "\\'");
    }

    /**
     * Generates the WHERE Clause or empty string if no filters applied.
     * <br>
     * relationshipName can be Null, if relationshipName is null, will generate WHERE Clause of the main SObject.
     * <br>
     * Otherwise will generate the WHERE Clause of the child sub-select statement that matches the given
     * relationshipName
     *
     * @param relationshipName target relationshipName of the child sub-Select statement. Can be Null
     * @return Where Clause applied on the given relationshipName
     */
    public String generateWhereClause(String relationshipName) {
        String where = getWhereFilter(_filter, relationshipName);
        if (StringUtil.isNotBlank(where)) {
            return " WHERE " + where;
        }
        return "";
    }

    /**
     * Generates the ORDER BY Clause or empty string if none applied
     *
     * @return Order By Clause of the main Select statement
     */
    public String generateOrderByClause() {
        if (_sort == null || _sort.isEmpty()) {
            return "";
        }
        StringBuilder ret = new StringBuilder(" ORDER BY ");
        for (int i = 0; i < _sort.size(); ++i) {
            String field = _sort.get(i).getProperty();
            String orderClause = OPERATORS_MAP.get(_sort.get(i).getSortOrder());
            if (i != 0) {
                ret.append(", ");
            }
            ret.append(field).append(" ").append(orderClause);
        }
        return ret.toString();
    }

    /**
     * Generates the LIMIT Clause or empty string if none applied
     *
     * @return Limit Clause of the main Select statement
     */
    public String generateLimitClause() {
        if (_limit > 0) {
            return " LIMIT " + _limit;
        }
        return "";
    }
}
