// Copyright (c) 2023 Boomi, Inc.
package com.boomi.snowflake.util;

import com.boomi.connector.api.GroupingExpression;
import com.boomi.connector.api.GroupingOperator;
import com.boomi.connector.api.SimpleExpression;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QueryHandlerTest {

    @Test
    public void testGetExpression() {
        SimpleExpression exp1 = createSimpleExpression("GREATER_THAN_OR_EQUALS","Timestamp","2023-10-01 11:02:56");
        SimpleExpression exp2 = createSimpleExpression("LIKE","Name","Abc");
        SimpleExpression exp3 = createSimpleExpression("LESS_THAN_OR_EQUALS","Timestamp","2023-11-30 01:10:05");
        GroupingExpression expression = new GroupingExpression().withNestedExpressions(exp1,exp2,exp3)
                .withOperator(GroupingOperator.AND);

        List<Map<String, String>> result = QueryHandler.getExpression(expression,false);
        assertEquals(3, result.size());
        assertEquals(2, result.stream().filter(map -> map.get("property").equals("Timestamp")).count());
    }

    private SimpleExpression createSimpleExpression(String operator, String property, String argument) {
        return new SimpleExpression().withOperator(operator)
                .withProperty(property).withArguments(argument);
    }
}