// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap.util;

import java.util.List;

import com.boomi.connector.api.Expression;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.api.Sort;

/**
 * @author kishore.pulluru
 *
 */
public class QueryBuilder {

	private enum QueryOperation {
		EQUALS("eq"), NOT_EQUALS("ne"), GREATER_THAN("gt"), LESS_THAN("lt"), GREATER_THAN_OR_EQUALS(
				"ge"), LESS_THAN_OR_EQUALS("le"), IN("in"), HAS("has");

		private String operation;

		private QueryOperation(String operation) {
			this.operation = operation;
		}

		private String getOperation() {
			return this.operation;
		}
	}

	/**
	 * This method will check the filter validity
	 * 
	 * @param filter
	 * @return true if the filter is valid
	 */
	public boolean isValidFilter(QueryFilter filter) {
		boolean isValid = false;
		if ((filter == null) || (filter.getExpression() == null)) {
			isValid = false;
		} else {
			isValid = true;
		}
		return isValid;
	}

	/**
	 * This method will check for valid sort order.
	 * 
	 * @param filter
	 * @return true if the sort order exists
	 */
	public boolean isValidSortOrder(QueryFilter filter) {

		return (null != filter && null != filter.getSort() && !filter.getSort().isEmpty());

	}

	/**
	 * This method will construct the QueryFilters from filter.
	 * 
	 * @param filter
	 * @param parameters 
	 * @return queryFilter
	 */
	public String consturctQueryFilter(QueryFilter filter) {
		StringBuilder sbExp = new StringBuilder();
		if (isValidFilter(filter)) {
			Expression expression = filter.getExpression();
			sbExp = buildExpression(expression);
		}
		return sbExp.toString();
	}

	/**
	 * This method will build the filter expression by parsing the filter data.
	 * 
	 * @param expression
	 * @return expression
	 */
	public StringBuilder buildExpression(Expression expression) {
		StringBuilder strExp = new StringBuilder();
		if (expression instanceof SimpleExpression) {
			strExp = buildSimpleExpression((SimpleExpression) expression);
		} else if (expression instanceof GroupingExpression) {
			GroupingExpression grpExp = (GroupingExpression) expression;
			strExp = buildGroupExpression(grpExp);
		}
		//strExp.insert(0, "$filter="); returning $filter= value
		return strExp;
	}

	/**
	 * This method will build the simple filter expression from filter data.
	 * 
	 * @param smplExp
	 * @return expression
	 */
	public StringBuilder buildSimpleExpression(SimpleExpression smplExp) {
		StringBuilder strExp = new StringBuilder();
		strExp.append(buildExprProperty(smplExp.getProperty()) + " "
				+ QueryOperation.valueOf(smplExp.getOperator()).getOperation() + " " + buildArgument(smplExp) + " ");
		return strExp;

	}

	/**
	 * This method will format the property.
	 * 
	 * @param property
	 * @return property
	 */
	public String buildExprProperty(String property) {
		if (property.contains("/")) {
			return property.substring(property.lastIndexOf('/') + 1, property.length());
		} else {
			return property;
		}

	}

	/**
	 * This method will build the arguments from the expressions.
	 * 
	 * @param smplExp
	 * @return arguments
	 */
	public String buildArgument(SimpleExpression smplExp) {
		StringBuilder sbArgs = new StringBuilder();
		if (smplExp.getOperator().equals(QueryOperation.IN.toString()) && smplExp.getArguments().get(0).contains(",")) {
			sbArgs.append("(");
			for (String arg : smplExp.getArguments().get(0).split(",")) {
				// removing single quotes for guid case
				if (arg.contains(SAPConstants.GUID) || arg.contains(SAPConstants.DATETIME)) {
					sbArgs.append(arg).append(",");
				} else {
					sbArgs.append("'").append(arg).append("'").append(",");
				}

			}
			sbArgs.deleteCharAt(sbArgs.length() - 1);
			sbArgs.append(")");
		} else {
			// removing single quotes for guid case
			if (smplExp.getArguments().get(0).contains(SAPConstants.GUID)
					|| smplExp.getArguments().get(0).contains(SAPConstants.DATETIME)) {
				sbArgs.append(smplExp.getArguments().get(0));
			} else {
				sbArgs.append("'").append(smplExp.getArguments().get(0)).append("'");
			}

		}
		return sbArgs.toString();
	}

	/**
	 * This method will build the QueryFilter from Group Expressions.
	 * 
	 * @param grpExp
	 * @return filter expression
	 */
	public StringBuilder buildGroupExpression(GroupingExpression grpExp) {
		StringBuilder strExp = new StringBuilder();
		strExp.append("(");
		String operator = grpExp.getOperator().value();
		for (Expression exp : grpExp.getNestedExpressions()) {
			if (exp instanceof GroupingExpression) {
				GroupingExpression grpexp = (GroupingExpression) exp;
				strExp.append(buildGroupExpression(grpexp));
			} else {
				SimpleExpression smplExp = (SimpleExpression) exp;
				strExp.append(buildSimpleExpression(smplExp));
			}
			strExp.append(operator);
			strExp.append(" ");
		}
		if (strExp.toString().trim().endsWith(operator)) {
			strExp.delete(strExp.lastIndexOf(operator), strExp.length());
		}
		strExp.append(")");
		return strExp;

	}

	/**
	 * This method is used to construct sort filters from the filter data.
	 * 
	 * @param filter
	 * @param parameters 
	 * @return sort expression
	 */
	public String consturctSortFilter(QueryFilter filter) {
		StringBuilder sortFilter = new StringBuilder();
		if (isValidSortOrder(filter)) {
			List<Sort> sortList = filter.getSort();
			if (!sortList.isEmpty()) {
				for (Sort sort : sortList) {
					sortFilter.append(buildExprProperty(sort.getProperty()) + " " + sort.getSortOrder());
					sortFilter.append(",");
				}
				sortFilter.deleteCharAt(sortFilter.length() - 1);
			}

		} 
		
		return sortFilter.toString();
	}

}
