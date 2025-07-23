// Copyright (c) 2022 Boomi, Inc.
package com.boomi.snowflake.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Expression;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;
import com.boomi.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryHandler {

	private static final String LIKE = "LIKE";
	private static final String EQUALS = "EQUALS";
	private static final String NOT_EQUALS = "NOT_EQUALS";
	private static final String GREATER_THAN = "GREATER_THAN";
	private static final String LESS_THAN = "LESS_THAN";
	private static final String GREATER_THAN_OR_EQUALS = "GREATER_THAN_OR_EQUALS";
	private static final String LESS_THAN_OR_EQUALS = "LESS_THAN_OR_EQUALS";
	private static final String LIKE_OP = " LIKE ";
	private static final String EQUALS_OP = " = ";
	private static final String NOT_EQUALS_OP = " != ";
	private static final String GREATER_THAN_OP = " > ";
	private static final String LESS_THAN_OP = " < ";
	private static final String GREATER_THAN_OR_EQUALS_OP = " >= ";
	private static final String LESS_THAN_OR_EQUALS_OP = " <= ";
	private static final String PROPERTY = "property";
	private static final String ARGUMENTS = "arguments";
	private static final String OPERATOR = "operator";
	private static final int BATCH_INDEX = 6;

	private QueryHandler() {
		// Prevent initialization
	}

	/**
	 * gets list of expression values extracted in a map
	 * @param expression Boomi expression
	 * @param isBatching checks for batching
	 * @return List of extracted values in a map
	 */
	public static List<Map<String, String>> getExpression(Expression expression, boolean isBatching) {
		List<Map<String, String>> expressionList = new ArrayList<>();
		if (expression == null) {
			return expressionList;
		}
		if (expression instanceof SimpleExpression) {
			expressionList.add(extractSimpleExpression((SimpleExpression) expression, isBatching));
		} else {
			GroupingExpression groupExpr = (GroupingExpression) expression;
			List<Expression> nestedExprs = groupExpr.getNestedExpressions();
			for (Expression nestedExpr : nestedExprs) {
				expressionList.add(extractSimpleExpression((SimpleExpression) nestedExpr, isBatching));
			}
		}
		return expressionList;
	}

	/**
	 * Extracts values from simple expression
	 * @param expression Boomi expression
	 * @param isBatching checks for batching
	 * @return extracted values in a map
	 */
	private static Map<String, String> extractSimpleExpression(SimpleExpression expression, boolean isBatching) {
		Map<String,String> map = new HashMap<>();
		if (isBatching) {
			map.put(PROPERTY,expression.getProperty().substring(BATCH_INDEX));
		} else {
			map.put(PROPERTY,expression.getProperty());
		}
		map.put(OPERATOR,getOperatorValue(expression.getOperator()));
		map.put(ARGUMENTS,expression.getArguments().get(0));
		return map;
	}

	private static String getOperatorValue(String operator) {
		switch (operator) {
			case LIKE:
					return LIKE_OP;
			case EQUALS:
					return EQUALS_OP;
			case NOT_EQUALS:
					return NOT_EQUALS_OP;
			case GREATER_THAN:
					return GREATER_THAN_OP;
			case LESS_THAN:
					return LESS_THAN_OP;
			case GREATER_THAN_OR_EQUALS:
					return GREATER_THAN_OR_EQUALS_OP;
			case LESS_THAN_OR_EQUALS:
					return LESS_THAN_OR_EQUALS_OP;
			default:
					throw new ConnectorException(operator+ " operator not supported");
		}
	}

	/**
	 * gets list of order items for snowflake
	 * 
	 * @param sortFields fields entered by the user in the platform
	 *
	 */
	public static List<String> getOrderItems(List<Sort> sortFields, boolean isBatching) {
		List<String> orderItems = new ArrayList<>();
		sortFields.forEach(sortField -> {
			String colName = isBatching ? sortField.getProperty().substring(BATCH_INDEX) : sortField.getProperty();
			String sortOrder = StringUtil.fastReplace(sortField.getSortOrder(),"_"," ");
			orderItems.add("\"" + colName + "\"" + " " + sortOrder);
		});
		return orderItems;
	}
}
