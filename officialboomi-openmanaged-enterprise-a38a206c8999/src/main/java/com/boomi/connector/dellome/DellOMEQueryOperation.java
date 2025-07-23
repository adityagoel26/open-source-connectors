// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome;

import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import static com.boomi.connector.dellome.util.DellOMEBoomiConstants.*;

import com.boomi.connector.api.Expression;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.GroupingOperator;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.SimpleExpression;
import com.boomi.connector.dellome.util.DellOMEBoomiConstants.FieldType;
import com.boomi.connector.dellome.util.DellOMEBoomiConstants.QueryOp;
import com.boomi.connector.dellome.util.DellOMEPayloadUtil;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.CollectionUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class DellOMEQueryOperation extends BaseQueryOperation {
	private int totalCount = -1;
	private int currentCount = 0;

	public DellOMEQueryOperation(DellOMEConnection conn) {
		super(conn);
	}

	@Override
	protected void executeQuery(QueryRequest request, OperationResponse response) {

		FilterData requestData = request.getFilter();
		QueryFilter filter = null;
		DellOMEResponse oResponse = null;
		ObjectMapper mapper = new ObjectMapper();

		try {
			filter = requestData.getFilter();

			do {
				try {
					if ((filter == null) || (filter.getExpression() == null)) {
						oResponse = getConnection().doQuery(this.currentCount);

					} else {
						// convert the sdk filter into url query filter pairs
						List<Map.Entry<String, String>> baseQueryTerms = constructQueryTerms(filter);
						oResponse = getConnection().doQuery(baseQueryTerms, this.currentCount);
					}

					if (oResponse.getStatus().equals(OperationStatus.SUCCESS)) {
						try(JsonParser jp = mapper.getFactory().createParser(oResponse.getResponse());){
							while (jp.nextToken() != null) {
								if (jp.getCurrentToken() == JsonToken.FIELD_NAME
										&& jp.getCurrentName().equals("@odata.count")) {
									jp.nextToken();
									if (this.totalCount < 0)
										this.totalCount = jp.getIntValue();
								}

								if (jp.getCurrentToken() == JsonToken.START_ARRAY) {
									while (jp.getCurrentToken() != JsonToken.END_ARRAY) {
										jp.nextToken();

										if (jp.getCurrentToken() == JsonToken.START_OBJECT) {

											ResponseUtil.addPartialSuccess(response, requestData, oResponse.getResponseCodeAsString(),
													DellOMEPayloadUtil.toPayload(jp));
											this.currentCount++;
										}
									}
									break;
								}
							}
						}
					} else {
						try(InputStream errorStream = oResponse.getErrorResponse();){
							ResponseUtil.addApplicationError(response, requestData, oResponse.getResponseCodeAsString(),
									PayloadUtil.toPayload(errorStream));
							break;
						}
					}
				}
				finally {
					oResponse.close();
				}
			} while (this.totalCount > this.currentCount);

			response.getLogger().log(Level.INFO,
					"DellOMEQueryOperation::executeQuery - Total OME returned Alerts count - " +  Math.max(0, this.totalCount));
			if (oResponse.getStatus().equals(OperationStatus.SUCCESS))
				response.finishPartialResult(requestData);

		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(response, requestData, e);
		} finally {
			oResponse.close();
		}
	}

	@Override
	public DellOMEConnection getConnection() {
		return (DellOMEConnection) super.getConnection();
	}

	/**
	 * Constructs a list of query filter terms from the given filter, may be
	 * {@code null} or empty.
	 *
	 * @param filter
	 *            query filter from which to construct the terms
	 *
	 * @return collection of query filter terms for the service
	 */
	static List<Map.Entry<String, String>> constructQueryTerms(QueryFilter filter) throws IllegalStateException {

		if ((filter == null) || (filter.getExpression() == null)) {
			// no filter given, (this is equivalent to "select all")
			return null;
		}
		List<Map.Entry<String, String>> terms = new ArrayList<Map.Entry<String, String>>();

		// see if base expression is a single expression or a grouping expression
		Expression baseExpr = filter.getExpression();
		if (baseExpr instanceof SimpleExpression) {
			// base expression is a single simple expression
			terms.add(constructSimpleExpression(new ArrayList<Expression>(Arrays.asList((SimpleExpression) baseExpr))));

		} else {
			// handle single level of grouped expressions
			GroupingExpression groupExpr = (GroupingExpression) baseExpr;
			// we only support "AND" groupings
			if (groupExpr.getOperator() != GroupingOperator.AND) {
				throw new IllegalStateException("Invalid grouping operator " + groupExpr.getOperator());
			}

			terms.add(constructSimpleExpression(groupExpr.getNestedExpressions()));
		}

		return terms;
	}

	/**
	 * Returns a url query term (key, value pair) constructed from the given
	 * SimpleExpression.
	 *
	 * @param list
	 *            the simple expression from which to construct the term
	 *
	 * @return url query filter term for the service
	 */
	static Map.Entry<String, String> constructSimpleExpression(List<Expression> list) throws IllegalStateException {

		String queryValue = "";

		// parse all the simple expressions in the group
		for (Expression expression : list) {

			if (!(expression instanceof SimpleExpression)) {
				throw new IllegalStateException("Only one level of grouping supported");
			}

			SimpleExpression expr = (SimpleExpression) expression;

			// this is the name of the queried object's property
			String propName = expr.getProperty();
			String prop = propName.substring(propName.indexOf("/") + 1);
			FieldType ft = FieldType.find(prop);

			// translate the operation id into one of our supported operations
			QueryOp queryOp = QueryOp.valueOf(expr.getOperator());

			// we only support 1 argument operations
			if (CollectionUtil.size(expr.getArguments()) != 1) {
				throw new IllegalStateException("Unexpected number of arguments for operation " + queryOp + "; found "
						+ CollectionUtil.size(expr.getArguments()) + ", expected 1");
			}
			String param = ft.name().equalsIgnoreCase("STRING") ? "'" + expr.getArguments().get(0) + "'"
					: expr.getArguments().get(0);

			// combine the property name and operation into the query filter value
			queryValue += prop + " " + queryOp.getPrefix() + " " + param + " and ";

		}
		queryValue = queryValue.substring(0, queryValue.length() - 5);
		String queryKey = "$filter";

		return new AbstractMap.SimpleEntry<String, String>(queryKey, queryValue);
	}

}
