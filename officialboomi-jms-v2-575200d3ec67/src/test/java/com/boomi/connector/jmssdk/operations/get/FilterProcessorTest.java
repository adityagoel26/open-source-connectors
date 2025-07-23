// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterProcessorTest {

    @Test
    public void getMessageSelectorTest() {
        String expectedMessageSelector = "message selector value";
        QueryFilter queryFilter = buildFilter("EQUAL_TO", "message_selector", expectedMessageSelector);
        FilterData mockedDocument = mockFilterData(queryFilter);

        String actualMessageSelector = FilterProcessor.getMessageSelector(mockedDocument);

        assertThat(expectedMessageSelector, CoreMatchers.is(actualMessageSelector));
    }

    private static QueryFilter buildFilter(String operator, String property, String value) {
        QueryGroupingBuilder groupBuilder = QueryGroupingBuilder.and(new QuerySimpleBuilder(property, operator, value));
        return new QueryFilterBuilder(groupBuilder).toFilter();
    }

    private static FilterData mockFilterData(QueryFilter queryFilter) {
        FilterData mockedDocument = mock(FilterData.class);
        when(mockedDocument.getFilter()).thenReturn(queryFilter);
        return mockedDocument;
    }
}
