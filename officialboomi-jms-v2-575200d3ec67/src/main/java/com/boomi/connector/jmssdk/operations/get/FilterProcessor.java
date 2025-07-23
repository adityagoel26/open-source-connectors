// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Expression;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.GroupingOperator;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import java.util.List;

/**
 * Utility class to extract the filter value configured for Get (custom query) Operation.
 */
final class FilterProcessor {

    private static final String ERROR_NOT_AND_GROUPING =
            "invalid QUERY filter; the first level of the query must have an AND grouping expression";
    private static final String ERROR_MULTIPLE_EXPRESSIONS = "invalid QUERY filter; there are more than 1 expressions";
    private static final String ERROR_MULTIPLE_GROUPS =
            "invalid QUERY filter; there are more than 1 group of nested expressions";
    private static final String EQUALS_OPERATOR = "EQUAL_TO";

    private FilterProcessor() {
    }

    /**
     * Process the given {@link QueryFilter} and return the configured Message Selector value.
     * <p>
     * This method verifies that the query has an AND grouping expression in the first level and then a single simple
     * expression in the second level.
     *
     * @param input a query operation filter.
     * @return the Message Selector value or an empty String if not configured.
     */
    static String getMessageSelector(FilterData input) {
        QueryFilter filter = input.getFilter();
        if (filter == null) {
            return StringUtil.EMPTY_STRING;
        }

        Expression expression = filter.getExpression();

        if (expression == null) {
            return StringUtil.EMPTY_STRING;
        }

        if (expression instanceof GroupingExpression) {
            return process((GroupingExpression) expression);
        }

        throw new ConnectorException(ERROR_NOT_AND_GROUPING);
    }

    private static String process(GroupingExpression expression) {
        if (expression.getOperator() != GroupingOperator.AND) {
            throw new ConnectorException(ERROR_NOT_AND_GROUPING);
        }

        List<Expression> nestedExpressions = expression.getNestedExpressions();
        if (nestedExpressions.isEmpty()) {
            return StringUtil.EMPTY_STRING;
        }

        if (nestedExpressions.size() > 1) {
            throw new ConnectorException(ERROR_MULTIPLE_EXPRESSIONS);
        }

        Expression nestedExpression = CollectionUtil.getFirst(nestedExpressions);
        if (nestedExpression instanceof SimpleExpression) {
            SimpleExpression simpleExpression = (SimpleExpression) nestedExpression;
            if (EQUALS_OPERATOR.equals(simpleExpression.getOperator())) {
                return CollectionUtil.getFirst(simpleExpression.getArguments());
            }
            throw new ConnectorException("invalid QUERY Operator: " + simpleExpression.getOperator());
        }
        throw new ConnectorException(ERROR_MULTIPLE_GROUPS);
    }

}
