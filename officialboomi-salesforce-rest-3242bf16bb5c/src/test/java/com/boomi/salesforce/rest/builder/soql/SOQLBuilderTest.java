// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.builder.soql;

import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.testutil.QueryFilterBuilder;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SOQLBuilderTest {

    private static final QueryFilter QUERY_FILTER = new QueryFilterBuilder().toFilter();

    @Test
    public void soqlMainFieldsTest() {
        generateSOQLQueryTest(Arrays.asList("Id", "IsDeleted", "Name", "Type"),
                "SELECT Id,IsDeleted,Name,Type FROM Account");
    }

    @Test
    public void soqlInvalidEmptyFieldsTest() {
        generateSOQLQueryTest(Collections.emptyList(), "SELECT  FROM Account");
    }

    @Test
    public void soqlChildren_1Level_1Sobject() {
        generateSOQLQueryTest(Arrays.asList("Contacts/records/Id", "Contacts/records/Name"),
                "SELECT (SELECT Id,Name FROM Contacts) FROM Account");
    }

    @Test
    public void soqlChildren_1Level_1Sobject_1MainTest() {
        generateSOQLQueryTest(Arrays.asList("Id", "Contacts/records/Id", "Contacts/records/Name"),
                "SELECT Id,(SELECT Id,Name FROM Contacts) FROM Account");
    }

    @Test
    public void soqlChildren_2Test() {
        generateSOQLQueryTest(Collections.singletonList("Contacts/records/Owner/Id"),
                "SELECT (SELECT Owner.Id FROM Contacts) FROM Account");
    }

    @Test
    public void soqlChildren_2Test_2() {
        generateSOQLQueryTest(Arrays.asList("Id", "Contacts/records/Id", "Contacts/records/Owner/Id"),
                "SELECT Id,(SELECT Id,Owner.Id FROM Contacts) FROM Account");
    }

    @Test
    public void soqlParents_1() {
        generateSOQLQueryTest(Collections.singletonList("Owner/Id"), "SELECT Owner.Id FROM Account");
    }

    @Test
    public void soqlParents_2() {
        generateSOQLQueryTest(Collections.singletonList("ChildAccounts/ChildAccounts/Owner/Id"),
                "SELECT ChildAccounts.ChildAccounts.Owner.Id FROM Account");
    }

    private static void generateSOQLQueryTest(List<String> selectedFields, String expected) {
        SOQLBuilder soqlBuilder = new SOQLBuilder(null, "Account", selectedFields, QUERY_FILTER, -1L);
        String actual = soqlBuilder.generateSOQLQuery();
        assertEquals(expected, actual);
    }
}
