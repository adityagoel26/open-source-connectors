// Copyright (c) 2025 Boomi, LP

package com.boomi.connector.veeva.operation.query;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Expression;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.connector.veeva.browser.VeevaBrowser;
import com.boomi.util.CollectionUtil;
import com.boomi.util.NumberUtil;
import com.boomi.util.StringUtil;

import java.util.List;
import java.util.Set;

/**
 * Encompasses the logic behind a Vault Query Language query creation, retrieving the filters and selections the
 * user chose and concatenating them according to specification.
 * <a href="https://developer.veevavault.com/api/23.3/#vault-query-language-vql">Vault Query Language (VQL)
 * Documentation</a>
 */
public class VQLQueryBuilder {

    private static final String STRING = "string";
    private static final String NUMBER = "number";
    private static final String DATE = "date";
    private static final String BOOLEAN = "boolean";
    private static final String EQUAL = "eq_";
    private static final String NON_EQUAL = "ne_";
    private static final String LESS_THAN = "lt_";
    private static final String GREATER_THAN = "gt_";
    private static final String LESS_OR_EQUAL_THAN = "le_";
    private static final String GREAT_OR_EQUAL_THAN = "ge_";
    private static final String SINGLE_QUOTE = "'";
    private static final String PAGE_SIZE_OPERATION_FIELD = "PAGESIZE";
    private static final String MAX_DOCUMENTS_OPERATION_FIELD = "MAXDOCUMENTS";
    private static final String FIND_OPERATION_FIELD = "FIND";
    private static final String DOT = ".";
    private static final String SLASH = "/";

    private static final char PERCENTAGE = '%';
    private static final char AMPERSAND = '&';
    /**
     * % = %25
     * & = %26
     */
    private static final Set<CharSequence> ENCODED_CHARACTERS = CollectionUtil.asNavigableSet("%25", "%26");

    private final String _objectType;
    private final List<String> _selectedFields;
    private final FilterVersionOptions _filterVersionOption;
    private final Long _pageSize;
    private final Long _maxDocuments;
    private final String _find;

    public VQLQueryBuilder(OperationContext context) {
        _objectType = context.getObjectTypeId();
        _selectedFields = context.getSelectedFields();

        PropertyMap operationProperties = context.getOperationProperties();
        _filterVersionOption = FilterVersionOptions.get(operationProperties);
        _pageSize = operationProperties.getLongProperty(PAGE_SIZE_OPERATION_FIELD);
        _maxDocuments = operationProperties.getLongProperty(MAX_DOCUMENTS_OPERATION_FIELD);
        _find = StringUtil.trimToEmpty(operationProperties.getProperty(FIND_OPERATION_FIELD));
    }

    /**
     * Append a regular field to the statement. This method takes care of closing any open sub query and adding comma
     * (,) delimiters if necessary before appending the field.
     *
     * @param statement             the statement being built
     * @param currentSubQueryObject the current sub query entity name
     * @param field                 the field to be appended
     */
    private static void appendRegularField(StringBuilder statement, String currentSubQueryObject, String field) {
        closeSubQueryIfNeeded(statement, currentSubQueryObject);
        addFieldSeparatorIfNeeded(statement);
        statement.append(field);
    }

    /**
     * Append a sub query field to the statement. This method takes care of opening a sub query clause and adding comma
     * (,) delimiters if necessary before appending the field.
     *
     * @param statement             the statement being built
     * @param currentSubQueryObject the current sub query entity name
     * @param field                 the field to be appended
     * @return the current sub query entity name
     */
    private static String appendSubQueryField(StringBuilder statement, String currentSubQueryObject, String field) {
        String[] elements = field.split(VeevaBrowser.SUBQUERY_SUFFIX + SLASH);
        String subQueryObject = elements[0];

        String fieldName;
        if (StringUtil.equalsIgnoreCase(subQueryObject, currentSubQueryObject)) {
            fieldName = elements[1];
        } else {
            closeSubQueryIfNeeded(statement, currentSubQueryObject);
            fieldName = "(SELECT " + elements[1];
        }

        addFieldSeparatorIfNeeded(statement);
        statement.append(fieldName);
        return subQueryObject;
    }

    /**
     * Append a nested field to the statement. Replace slashes (/) with dots (.) as required by VQL spec. This method
     * takes care of closing any open sub query and adding comma (,) delimiters if necessary before appending the field.
     *
     * @param statement             the statement being built
     * @param currentSubQueryObject the current sub query entity name
     * @param field                 the field to be appended
     */
    private static void appendNestedField(StringBuilder statement, String currentSubQueryObject, String field) {
        field = field.replace(SLASH, DOT);
        closeSubQueryIfNeeded(statement, currentSubQueryObject);
        addFieldSeparatorIfNeeded(statement);
        statement.append(field);
    }

    /**
     * Close the given subquery by inserting the FROM Statement and the closing parenthesis.
     *
     * @param vqlStatement - the statement to append the FROM clause to
     * @param objectName   - this can be the object name in the many query or a joined subquery
     */
    private static void closeSubQueryIfNeeded(StringBuilder vqlStatement, String objectName) {
        if (StringUtil.isBlank(objectName)) {
            return;
        }
        // close out select term
        vqlStatement.append(" FROM ").append(objectName).append(")");
    }

    private static void addFieldSeparatorIfNeeded(StringBuilder statement) {
        if (statement.length() != 0) {
            statement.append(",");
        }
    }

    /**
     * Build the WHERE clause for the VQL query from the QueryFilters configured by the user. This method handles
     * simple and group expressions
     *
     * @return the VQL WHERE clause
     */
    private static String buildWhereStatement(QueryFilter queryFilter) {
        if (queryFilter == null) {
            return StringUtil.EMPTY_STRING;
        }

        String terms = buildWhereFilters(queryFilter.getExpression(), 0);
        if (StringUtil.isBlank(terms)) {
            return StringUtil.EMPTY_STRING;
        }

        // terms already includes a white space at the beginning
        return " WHERE" + terms;
    }

    /**
     * Parse the given expression to build the filters for the VQL WHERE clause. Group expressions are processed
     * recursively by this method.
     *
     * @param baseExpr the filter expression specified by the user in the Query Operation Filters
     * @param depth    the current level when processing nested expressions
     * @return the filters for the VQL WHERE clause
     */
    private static String buildWhereFilters(Expression baseExpr, int depth) {
        if (baseExpr == null) {
            return StringUtil.EMPTY_STRING;
        }

        StringBuilder statementBuilder = new StringBuilder();

        if (baseExpr instanceof SimpleExpression) {
            // base expression is a single simple expression
            statementBuilder.append((buildSimpleExpression((SimpleExpression) baseExpr)));
        } else {
            // handle single level of grouped expressions
            GroupingExpression groupExpr = (GroupingExpression) baseExpr;

            // parse all the simple expressions in the group
            boolean first = true;
            for (Expression nestedExpr : groupExpr.getNestedExpressions()) {
                if (!first) {
                    statementBuilder.append(" ").append(groupExpr.getOperator().name().toUpperCase());
                }
                first = false;
                if (nestedExpr instanceof GroupingExpression) {
                    String groupingTerms = buildWhereFilters(nestedExpr, depth + 1);
                    statementBuilder.append(" (").append(groupingTerms).append(")");
                } else {
                    String term = buildSimpleExpression((SimpleExpression) nestedExpr);
                    if (StringUtil.isNotBlank(term)) {
                        statementBuilder.append(term);
                    }
                }
            }
        }

        return statementBuilder.toString();
    }

    private static String buildSimpleExpression(SimpleExpression expr) {
        // Veeva uses dots (.) for nested fields
        String propName = expr.getProperty();
        if (StringUtil.isBlank(propName)) {
            throw new ConnectorException("Filter field parameter required");
        }
        propName = propName.replace(SLASH, DOT);

        String operatorName = expr.getOperator();

        // we only support 1 argument operations
        if (CollectionUtil.size(expr.getArguments()) != 1) {
            throw new IllegalStateException(
                    "Unexpected " + NUMBER + " of arguments for operation " + operatorName + "; found "
                            + CollectionUtil.size(expr.getArguments()) + ", expected 1");
        }

        // this is the single operation argument
        String parameter = CollectionUtil.getFirst(expr.getArguments());
        if (StringUtil.isBlank(parameter)) {
            throw new ConnectorException("Filter parameter is required for field: " + propName);
        }

        parameter = encodeVQL(parameter);

        String operator = getOperator(operatorName);
        String term = " " + propName + " " + operator;

        if (operatorName.endsWith("_" + STRING)) {
            // wrap it with quotes
            return term + " '" + parameter + "' ";
        } else if ("LIKE".equals(operatorName)) {
            return term + " '" + parameter + "' ";
        } else if (operatorName.endsWith("_" + DATE)) {
            return term + " '" + parameter + "' ";
        } else {
            switch (operatorName) {
                case "CONTAINS":
                    return term + " ('" + parameter.replace(",", "','") + "')";
                case "IN":
                    return term + " (" + parameter + ") ";
                case "BETWEEN":
                default:
                    return term + " " + parameter + " ";
            }
        }
    }

    /**
     * Encodes a string for safe use in VQL queries. This implementation handles % and & characters, other characters
     * are not encoded.
     * When a % or & is already encoded in the input, it is not encoded again.
     *
     * @param input The string to encode
     * @return The encoded string
     */
    private static String encodeVQL(CharSequence input) {

        // Pre-allocate buffer with reasonable size estimate
        final int len = input.length();
        StringBuilder sb = new StringBuilder(len * 2);

        // Iterate through the input string one character at a time
        int index = 0;
        while (index < len) {
            char ch = input.charAt(index);

            switch (ch) {
                case PERCENTAGE:
                    boolean alreadyEncoded = (index + 2 < len) && ENCODED_CHARACTERS.contains(
                            input.subSequence(index, index + 3));
                    if (alreadyEncoded) {
                        // Valid encoding — copy it as-is
                        sb.append(input.subSequence(index, index + 3));
                        index += 3;
                    } else {
                        // Invalid or standalone %, encode it as %25
                        sb.append("%25");
                        index++;
                    }
                    break;

                case AMPERSAND:
                    sb.append("%26");
                    index++;
                    break;

                default:
                    // Append all other characters unchanged
                    sb.append(ch);
                    index++;
                    break;
            }
        }

        return sb.toString();
    }

    private static String getOperator(String operatorName) {
        switch (operatorName) {
            case EQUAL + STRING:
            case EQUAL + NUMBER:
            case EQUAL + DATE:
            case EQUAL + BOOLEAN:
                return "=";
            case NON_EQUAL + STRING:
            case NON_EQUAL + NUMBER:
            case NON_EQUAL + DATE:
            case NON_EQUAL + BOOLEAN:
                return "!=";
            case LESS_THAN + STRING:
            case LESS_THAN + NUMBER:
            case LESS_THAN + DATE:
                return "<";
            case GREATER_THAN + STRING:
            case GREATER_THAN + NUMBER:
            case GREATER_THAN + DATE:
                return ">";
            case LESS_OR_EQUAL_THAN + STRING:
            case LESS_OR_EQUAL_THAN + NUMBER:
            case LESS_OR_EQUAL_THAN + DATE:
                return "<=";
            case GREAT_OR_EQUAL_THAN + STRING:
            case GREAT_OR_EQUAL_THAN + NUMBER:
            case GREAT_OR_EQUAL_THAN + DATE:
                return ">=";
            default:
                return operatorName;
        }
    }

    /**
     * Build the ORDER BY clause for the VQL query from the sorting configured by the user.
     *
     * @return the VQL ORDER BY clause
     */
    private static String buildOrderByStatement(QueryFilter queryFilter) {
        if (queryFilter == null) {
            return StringUtil.EMPTY_STRING;
        }

        List<Sort> sortTerms = queryFilter.getSort();
        if (CollectionUtil.isEmpty(sortTerms)) {
            return StringUtil.EMPTY_STRING;
        }

        StringBuilder orderByStatement = new StringBuilder();
        for (Sort sort : sortTerms) {
            String sortTerm = sort.getProperty();
            if (StringUtil.isBlank(sortTerm)) {
                continue;
            }

            // Veeva uses dots (.) for nested fields
            sortTerm = sortTerm.replace(SLASH, DOT);

            if (orderByStatement.length() == 0) {
                orderByStatement.append(" ORDER BY ");
            } else {
                orderByStatement.append(", ");
            }

            orderByStatement.append(sortTerm).append(" ").append(sort.getSortOrder());
        }

        return orderByStatement.toString();
    }

    /**
     * Build a Vault Query Language (VQL) statement (a SQL-like statement) that specifies the object to query
     * (in the FROM clause), the fields to retrieve (in the SELECT clause), and any optional filters to apply
     * (in the WHERE and FIND clauses) to narrow the results. Queries should be formatted as q={query}.
     * For example, {@code q=SELECT id FROM documents}.
     *
     * @param queryFilter the operation's filters as chosen by the user to build the query.
     */
    String buildVQLQuery(QueryFilter queryFilter) {
        return "q=" + buildSelectKeyword() + buildFieldsForSelectStatement() + buildFromStatement()
                + buildFindStatement() + buildWhereStatement(queryFilter) + buildOrderByStatement(queryFilter)
                + buildMaxDocumentsStatement() + buildPageSizeStatement();
    }

    /**
     * Build the SELECT keyword of the VQL statement. It includes the LATESTVERSION modifier when needed.
     *
     * @return The VQL Select keyword
     */
    private String buildSelectKeyword() {
        if (_filterVersionOption == FilterVersionOptions.LATEST_MATCHING_VERSION) {
            return "SELECT LATESTVERSION ";
        }

        return "SELECT ";
    }

    /**
     * Build the list of fields to include in the SELECT clause. This method handles adding sub queries and nested
     * fields when needed
     *
     * @return the fields for the SELECT clause
     */
    private String buildFieldsForSelectStatement() {
        if (CollectionUtil.isEmpty(_selectedFields)) {
            throw new ConnectorException("At least one field must be selected");
        }

        StringBuilder fieldsStatement = new StringBuilder();
        String currentSubQueryObject = "";
        for (String field : _selectedFields) {
            if (!field.contains(SLASH)) {
                // When selected a parent and child, ONLY the child element is added to the statement
                if (_selectedFields.stream().noneMatch(f -> f.startsWith(field + DOT))) {
                    appendRegularField(fieldsStatement, currentSubQueryObject, field);
                }
                currentSubQueryObject = StringUtil.EMPTY_STRING;
            } else if (field.contains(VeevaBrowser.SUBQUERY_SUFFIX)) {
                currentSubQueryObject = appendSubQueryField(fieldsStatement, currentSubQueryObject, field);
            } else {
                appendNestedField(fieldsStatement, currentSubQueryObject, field);
                currentSubQueryObject = StringUtil.EMPTY_STRING;
            }
        }
        closeSubQueryIfNeeded(fieldsStatement, currentSubQueryObject);
        return fieldsStatement.toString();
    }

    /**
     * Build the FROM clause for the VQL query. It includes the version qualifier for the queried object if needed.
     *
     * @return the VQL FROM clause
     */
    private String buildFromStatement() {
        String versions;

        switch (_filterVersionOption) {
            case ALL_MATCHING_VERSIONS:
            case LATEST_MATCHING_VERSION:
                versions = "ALLVERSIONS ";
                break;
            case LATEST_VERSION:
                versions = StringUtil.EMPTY_STRING;
                break;
            default:
                throw new IllegalArgumentException("Invalid filter version option: " + _filterVersionOption);
        }

        return " FROM " + versions + _objectType;
    }

    /**
     * Build the FIND clause for the VQL query. The term is provided by the user in an operation property, it's
     * included in the query as it is except this method wraps it with quotes if needed.
     *
     * @return the VQL FIND clause
     */
    private String buildFindStatement() {
        if (StringUtil.isEmpty(_find)) {
            return StringUtil.EMPTY_STRING;
        }

        // add quotes at the beginning and end of the string if needed
        String wrapped = _find.contains(SINGLE_QUOTE) ? _find : (SINGLE_QUOTE + _find + SINGLE_QUOTE);
        return " FIND (" + wrapped + ") ";
    }

    /**
     * Build the MAXROWS clause for the VQL query. It indicates how many entities shall be returned at most.
     *
     * @return the VQL MAXROWS clause
     */
    private String buildMaxDocumentsStatement() {
        if (_maxDocuments == null || _maxDocuments <= 0L) {
            return StringUtil.EMPTY_STRING;
        }
        return " MAXROWS " + _maxDocuments;
    }

    /**
     * Build the PAGESIZE clause for the VQL query. It indicates how many entities shall be returned at most per page.
     *
     * @return the VQL PAGESIZE clause
     */
    private String buildPageSizeStatement() {
        if (_pageSize == null || _pageSize <= 0L) {
            return StringUtil.EMPTY_STRING;
        }
        return " PAGESIZE " + _pageSize;
    }

    private enum FilterVersionOptions {
        LATEST_VERSION, ALL_MATCHING_VERSIONS, LATEST_MATCHING_VERSION;

        private static final String OPERATION_FIELD_ID = "VERSION_FILTER_OPTIONS";

        static FilterVersionOptions get(PropertyMap operationProperties) {
            return NumberUtil.toEnum(FilterVersionOptions.class, operationProperties.getProperty(OPERATION_FIELD_ID),
                    LATEST_VERSION);
        }
    }
}