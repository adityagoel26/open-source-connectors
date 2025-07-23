//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.utils;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.workdayprism.PrismConnection;
import com.boomi.connector.workdayprism.responses.ListTableResponse;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;
import java.io.IOException;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Delegate in charge of the orchestration and pagination of the calls to get the list of Tables available in the
 * Workday Prism account to filter and list them a potential Object Types during the Browser phase.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class TableListHelper {
    private static final String ERROR_TOO_MANY_RESOURCES = "Too many resources. Please update the filter";
    private static final String ERROR_RETRIEVE_TABLES = "error retrieving tables";
    private static final int PAGE_SIZE = 100;

    private final PrismConnection connection;
    private final String labelPrefix;
    private final int maxResults;

    /**
     * Creates a new {@link TableListHelper} instance
     *
     * @param connection
     *         a {@link PrismConnection} instance
     */
    public TableListHelper(PrismConnection connection, String labelPrefix) {
        this.connection = connection;
        this.maxResults = connection.getContext().getConfig().getMaxNumberObjectTypes();
        this.labelPrefix = StringUtil.defaultIfBlank(labelPrefix, StringUtil.EMPTY_STRING);
    }

    /**
     * Returns the API-ready Tables in the account as a sorted collection of ObjectType
     *
     * @param entityFilter
     *         a string value of the custom filter to limit the returned resources by name
     * @return an alphabetically sorted {@link SortedSet} of {@link ObjectType}
     */
    
    public SortedSet<ObjectType> getObjectTypes(String entityFilter) {
        int offset = 0;
        boolean finalized;
        SortedSet<ObjectType> types = new TreeSet<>(getObjectTypeComparator());
        do {

        	ListTableResponse response = request(offset);
   
            if (!CollectionUtil.isEmpty(response.getData())) {
        
                Iterable<ObjectType> tables = getObjectTypes(response, entityFilter);

                CollectionUtil.addAll(tables, types);

                //Validates that the list of filtered object types it's not larger than the allowed by the atom
                if (types.size() > maxResults) {
                    throw new TooManyResourcesException(ERROR_TOO_MANY_RESOURCES);
                }
            }
            offset += PAGE_SIZE;
            finalized = offset >= response.getTotal();
        } while (!finalized);

        return types;
    }
   
   /**
    *  Returns the API-ready Tables in the account as a iterable collection of ObjectType
    * @param response an instance of ListTableResponse type
    * @param filter an instance of String type
    * @return an iterable collection of ObjectType
    */
    private Iterable<ObjectType> getObjectTypes(ListTableResponse response, String filter) {
        Iterable<ListTableResponse.Data> tables = response.getData();
        if (StringUtil.isNotBlank(filter)) {
            tables = CollectionUtil.filter(tables, getTableFilter(filter));
        }

        return CollectionUtil.apply(tables, getObjectTypeMappingFunction());
    }

   
    /**
     * Performs mapping of the object types
     * @return an instance of type CollectionUtil.Function<ListTableResponse.Data, ObjectType>
     */
    private CollectionUtil.Function<ListTableResponse.Data, ObjectType>
    getObjectTypeMappingFunction() {
        return new CollectionUtil.Function<ListTableResponse.Data, ObjectType>() {
            @Override
            public ObjectType apply(ListTableResponse.Data tables) {
                return new ObjectType().withId(tables.getId()).withLabel(labelPrefix + tables.getName());
            }
        };
    }

    private static Comparator<ObjectType> getObjectTypeComparator() {
        return new Comparator<ObjectType>() {
            @Override
            public int compare(ObjectType objectType1, ObjectType objectType2) {
                return objectType1.getLabel().compareTo(objectType2.getLabel());
            }
        };
    }

  
    /**
     * Method to get the filter required to select a particular table from the schema
     * @param filter a String instance
     * @return an instance of type CollectionUtil.Filter<ListTableResponse.Data>
     */
    private static CollectionUtil.Filter<ListTableResponse.Data> getTableFilter(
    	    final String filter) {
    	        return new CollectionUtil.Filter<ListTableResponse.Data>() {
    	            private final Pattern pattern = Pattern.compile(filter.replace("*", ".*").replace("?", "."));

    	            @Override
    	            public boolean accept(ListTableResponse.Data table) {
    	                return (!table.getName().isEmpty()) && pattern.matcher(table.getName()).matches();
    	            }
    	        };
    	    }

    /**
     * Returns list of tables as response to an incoming request with an offset
     * @param offset an integer
     * @return an instance of type ListTableResponse
     */
    private ListTableResponse request(int offset) {
        try {
            return connection.getTables(offset, PAGE_SIZE);
        }
        catch (IOException e) {
            throw new ConnectorException(ERROR_RETRIEVE_TABLES, e);
        }
    }
    
}
