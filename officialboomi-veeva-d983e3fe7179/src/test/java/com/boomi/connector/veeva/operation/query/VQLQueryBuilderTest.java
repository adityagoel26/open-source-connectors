// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.veeva.operation.query;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.Sort;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VQLQueryBuilderTest {

    public static final String MAXDOCUMENTS = "MAXDOCUMENTS";
    public static final String PAGESIZE = "PAGESIZE";
    private final OperationContext _context = mock(OperationContext.class, RETURNS_DEEP_STUBS);
    private final MutablePropertyMap _operationProperties = new MutablePropertyMap();

    VQLQueryBuilderTest() {
        when(_context.getOperationProperties()).thenReturn(_operationProperties);
        when(_context.getObjectTypeId()).thenReturn("test_object");
        when(_context.getSelectedFields()).thenReturn(Collections.singletonList("id"));
    }

    @Test
    void buildQueryWithContains() {
        _operationProperties.put(MAXDOCUMENTS, 50L);
        _operationProperties.put(PAGESIZE, 10L);

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object WHERE id = 0  AND name__v CONTAINS ('T') MAXROWS 50 PAGESIZE 10",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithEncodedCharactersAndLikeOperator() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%&"))).toFilter();
        qf.withSort(new Sort().withProperty("id").withSortOrder("ASC"));
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);
        assertEquals(
                "q=SELECT id FROM test_object WHERE id != 0  AND name__v LIKE 'Tr%25%26'  ORDER BY id ASC MAXROWS 28 "
                + "PAGESIZE 2", queryBuilder.buildVQLQuery(qf));
    }

    public static Collection<Arguments> filterEncodingTestCases() {
        Collection<Arguments> testCases = new ArrayList<>();
        {
            String description = "% should be encoded as %25";
            String filter = "example%example";
            String expected = "example%25example";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "& should be encoded as %26";
            String filter = "example&example";
            String expected = "example%26example";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "% should be encoded as %25, edge case";
            String filter = "example%cat";
            String expected = "example%25cat";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "& should be encoded as %26, % should be encoded as %25";
            String filter = "example%&example";
            String expected = "example%25%26example";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "% should be encoded as %25, & should be encoded as %26";
            String filter = "example&_%example";
            String expected = "example%26_%25example";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "%25 should not be encoded";
            String filter = "example%25example";
            String expected = "example%25example";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "%26 should not be encoded";
            String filter = "example%26example";
            String expected = "example%26example";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "% should be encoded as %25, %26 should not be encoded";
            String filter = "example%%26examp%le";
            String expected = "example%25%26examp%25le";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "% at the end of the string should be encoded as %25";
            String filter = "example%";
            String expected = "example%25";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "%26 at the end of the string should not be encoded";
            String filter = "example%26";
            String expected = "example%26";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "% should be encoded as %25, & should be encoded as %26 - short input";
            String filter = "%&";
            String expected = "%25%26";
            testCases.add(arguments(description, filter, expected));
        }
        {
            String description = "%27 should be encoded as %2527";
            String filter = "example%27example";
            String expected = "example%2527example";
            testCases.add(arguments(description, filter, expected));
        }

        return testCases;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filterEncodingTestCases")
    void filterEncodingTests(String description, String filter, String expected) {
        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "EQUALS", filter))).toFilter();
        qf.withSort(new Sort().withProperty("id").withSortOrder("ASC"));
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);
        assertEquals("q=SELECT id FROM test_object WHERE id != 0  AND name__v EQUALS " + expected + "  ORDER BY id ASC",
                queryBuilder.buildVQLQuery(qf));
    }

    /**
     * When selected a parent and child in the field selection panel, ONLY the child element is added to the SELECT
     * statement
     */
    @Test
    void buildQueryWithParentAndChildFieldsSelected() {
        _operationProperties.put(MAXDOCUMENTS, 50L);
        _operationProperties.put(PAGESIZE, 10L);

        _operationProperties.put("JOIN_DEPTH", "1");

        when(_context.getSelectedFields()).thenReturn(
                Arrays.asList("document_payment_request__cr", "document_payment_request__cr.amount__v"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT document_payment_request__cr.amount__v FROM test_object WHERE id = 0  AND name__v CONTAINS "
                + "('T') MAXROWS 50 PAGESIZE 10", queryBuilder.buildVQLQuery(qf),
                "Only the child should be added, not the parent");
    }

    @Test
    void buildQueryWithFilterChildSeparatorIsDot() {
        when(_context.getSelectedFields()).thenReturn(Arrays.asList("document_payment_request__cr"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(
                new QuerySimpleBuilder("document_payment_request__cr/amount__v", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT document_payment_request__cr FROM test_object WHERE document_payment_request__cr.amount__v "
                + "= 0  AND name__v CONTAINS ('T')", queryBuilder.buildVQLQuery(qf),
                "The statement should have the parent appended to the name of the child element selected, separated "
                + "by a dot, and not a slash.");
    }

    @ParameterizedTest
    @ValueSource(longs = { 0L, -1L })
    void buildQueryWithUnlimitedPageSize(long pageSize) {
        _operationProperties.put(PAGESIZE, pageSize);

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object WHERE id = 0  AND name__v CONTAINS ('T')",
                queryBuilder.buildVQLQuery(qf));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0L, -1L })
    void buildQueryWithUnlimitedMaxDocuments(long maxDocuments) {
        _operationProperties.put(MAXDOCUMENTS, maxDocuments);

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object WHERE id = 0  AND name__v CONTAINS ('T')",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithMaxDocuments() {
        _operationProperties.put(MAXDOCUMENTS, 42L);

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object WHERE id = 0  AND name__v CONTAINS ('T') MAXROWS 42",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithPageSize() {
        _operationProperties.put(PAGESIZE, 42L);

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "CONTAINS", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object WHERE id = 0  AND name__v CONTAINS ('T') PAGESIZE 42",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithFindStatement() {
        _operationProperties.put("FIND", "Example1");

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("name__v", "LIKE", "Tr%")).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object FIND ('Example1')  WHERE name__v LIKE 'Tr%25'  ORDER BY id ASC",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithQuotedFindStatement() {
        _operationProperties.put("FIND", "'Example1 OR Example2'");

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("name__v", "LIKE", "Tr%")).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT id FROM test_object FIND ('Example1 OR Example2')  WHERE name__v LIKE 'Tr%25'  ORDER BY id "
                + "ASC", queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithAllMatchingVersions() {
        _operationProperties.put("VERSION_FILTER_OPTIONS", "ALL_MATCHING_VERSIONS");

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "eq_number", "0"),
                new QuerySimpleBuilder("name__v", "IN", "T"))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM ALLVERSIONS test_object WHERE id = 0  AND name__v IN (T) ",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithLatestMatchingVersion() {
        _operationProperties.put("VERSION_FILTER_OPTIONS", "LATEST_MATCHING_VERSION");

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "gt_number", "0"),
                QueryGroupingBuilder.and(new QuerySimpleBuilder("description__v", "le_date", "20/12/2024"),
                        new QuerySimpleBuilder("name__v", "ge_string", "three")))).toFilter();
        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT LATESTVERSION id FROM ALLVERSIONS test_object WHERE id > 0  AND ( description__v <= "
                     + "'20/12/2024'  AND name__v >= 'three' )", queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithLatestVersion() {
        _operationProperties.put("VERSION_FILTER_OPTIONS", "LATEST_VERSION");

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("id", "lt_number", "0")).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        Sort sort2 = new Sort();
        sort2.setProperty("name__v");
        sort2.setSortOrder("DESC");

        qf.withSort(sort1, sort2);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id FROM test_object WHERE id < 0  ORDER BY id ASC, name__v DESC",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithoutFilterArgument() {

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("id", "eq_number", "")).toFilter();

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);
        assertThrows(ConnectorException.class, () -> queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithMultipleFilterArguments() {

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("id", "eq_number", "1", "2")).toFilter();

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);
        assertThrows(IllegalStateException.class, () -> queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithEmptyFilterPropertyName() {

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("", "eq_number", "1")).toFilter();

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);
        assertThrows(ConnectorException.class, () -> queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithoutFields() {

        when(_context.getSelectedFields()).thenReturn(Collections.emptyList());

        QueryFilter qf = new QueryFilterBuilder(new QuerySimpleBuilder("", "eq_number", "1")).toFilter();

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);
        assertThrows(ConnectorException.class, () -> queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithMultipleFields() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        when(_context.getSelectedFields()).thenReturn(Arrays.asList("id", "description__v"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT id,description__v FROM test_object WHERE id != 0  AND name__v LIKE 'Tr%25'  ORDER BY id ASC"
                + " MAXROWS 28 " + "PAGESIZE 2", queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithNestedFields() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        when(_context.getSelectedFields()).thenReturn(Arrays.asList("id", "cdx_rule_set__vr/initiator_role_label__v"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id,cdx_rule_set__vr.initiator_role_label__v FROM test_object WHERE id != 0  AND name__v "
                     + "LIKE 'Tr%25'  ORDER BY id ASC MAXROWS 28 " + "PAGESIZE 2", queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithSimpleSubquery() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        when(_context.getSelectedFields()).thenReturn(
                Arrays.asList("id", "agreement_transfers__vr__Subquery/created_date__v"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT id,(SELECT created_date__v FROM agreement_transfers__vr) FROM test_object WHERE id != 0  "
                + "AND name__v LIKE 'Tr%25'  ORDER BY id ASC MAXROWS 28 PAGESIZE 2", queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithMultipleFieldsInSingleSubquery() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        when(_context.getSelectedFields()).thenReturn(
                Arrays.asList("id", "agreement_transfers__vr__Subquery/created_date__v",
                        "agreement_transfers__vr__Subquery/finish_time__v"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT id,(SELECT created_date__v,finish_time__v FROM agreement_transfers__vr) FROM test_object "
                + "WHERE id != 0  AND name__v LIKE 'Tr%25'  ORDER BY id ASC MAXROWS 28 PAGESIZE 2",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithMultipleSubqueries() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        when(_context.getSelectedFields()).thenReturn(
                Arrays.asList("id", "agreement_transfers__vr__Subquery/created_date__v",
                        "agreement_transfers__vr__Subquery/finish_time__v",
                        "cdx_agreement_activity_items__vr__Subquery/component__v"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("name__v", "LIKE", "Tr%"))).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals("q=SELECT id,(SELECT created_date__v,finish_time__v FROM agreement_transfers__vr),(SELECT "
                     + "component__v FROM cdx_agreement_activity_items__vr) FROM test_object WHERE id != 0  AND "
                     + "name__v " + "LIKE 'Tr%25'  ORDER BY id ASC MAXROWS 28 PAGESIZE 2",
                queryBuilder.buildVQLQuery(qf));
    }

    @Test
    void buildQueryWithBooleanOperators() {
        _operationProperties.put(MAXDOCUMENTS, 28L);
        _operationProperties.put(PAGESIZE, 2L);

        when(_context.getSelectedFields()).thenReturn(Arrays.asList("id", "active", "deleted"));

        QueryFilter qf = new QueryFilterBuilder(QueryGroupingBuilder.and(new QuerySimpleBuilder("id", "ne_number", "0"),
                new QuerySimpleBuilder("active", "eq_boolean", "true"),
                new QuerySimpleBuilder("deleted", "ne_boolean", "true"))).toFilter();

        Sort sort1 = new Sort();
        sort1.setProperty("id");
        sort1.setSortOrder("ASC");
        qf.withSort(sort1);

        VQLQueryBuilder queryBuilder = new VQLQueryBuilder(_context);

        assertEquals(
                "q=SELECT id,active,deleted FROM test_object WHERE id != 0  AND active = true  AND deleted != true  "
                + "ORDER BY " + "id ASC MAXROWS 28 " + "PAGESIZE 2", queryBuilder.buildVQLQuery(qf));
    }
}