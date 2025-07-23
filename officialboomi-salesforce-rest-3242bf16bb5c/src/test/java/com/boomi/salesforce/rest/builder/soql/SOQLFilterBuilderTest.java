// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.builder.soql;

import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;
import com.boomi.salesforce.rest.controller.metadata.MetadataParser;
import com.boomi.salesforce.rest.controller.metadata.SObjectController;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.salesforce.rest.testutil.SFRestTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SOQLFilterBuilderTest {

    private SObjectController _controller;

    @BeforeEach
    public void initController() throws IOException {
        _controller = Mockito.mock(SObjectController.class, Mockito.RETURNS_DEEP_STUBS);
        try (InputStream stream = SFRestTestUtil.getContent("describeAccountResponse.json")) {
            SObjectModel sobject = MetadataParser.parseFieldsForOperation(stream, "QUERY");
            Mockito.when(_controller.buildSObject("Account", false)).thenReturn(sobject);
        }
    }

    @Test
    public void soqlMainAndParentFieldsStringAndNumberTest() {
        QueryFilter queryFilter = new QueryFilterBuilder(
                QueryGroupingBuilder.and(new QuerySimpleBuilder("Name", "LIKE", "%a%"),
                        new QuerySimpleBuilder("Type", "GREATER_THAN", "21"),
                        new QuerySimpleBuilder("AnnualRevenue", "GREATER_THAN", "22"),
                        new QuerySimpleBuilder("Accounts/AnnualRevenue", "GREATER_THAN", "23"))).toFilter();

        generateWhereClauseTest(
                " WHERE (Name like '%a%' AND Type > '21' AND AnnualRevenue > '22' AND Accounts.AnnualRevenue > '23')",
                null, queryFilter);
    }

    @Test
    public void soqlChildFieldsTest() {
        QueryFilter queryFilter = new QueryFilterBuilder(
                QueryGroupingBuilder.and(new QuerySimpleBuilder("Name", "LIKE", "%a%"),
                        new QuerySimpleBuilder("Type", "GREATER_THAN", "21"),
                        new QuerySimpleBuilder("AnnualRevenue", "GREATER_THAN", "22"),
                        new QuerySimpleBuilder("Accounts/AnnualRevenue", "GREATER_THAN", "23"),
                        new QuerySimpleBuilder("ChildAccounts/records" + "/AnnualRevenue", "GREATER_THAN",
                                "24"))).toFilter();

        generateWhereClauseTest(" WHERE (AnnualRevenue > '24')", "ChildAccounts", queryFilter);
    }

    private void generateWhereClauseTest(String expected, String relationshipName, QueryFilter queryFilter) {
        SOQLFilterBuilder soqlBuilder = new SOQLFilterBuilder(_controller, "Account", queryFilter, -1L);
        String actual = soqlBuilder.generateWhereClause(relationshipName);
        assertEquals(expected, actual);
    }
}
