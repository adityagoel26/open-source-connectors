// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.util;

/**
 * The Enum QueryOperationUtil
 *
 */
public enum QueryOperationUtil {
	
	/** The equals. */
	EQUALS("feq_"),
    
    /** The not equals. */
    NOT_EQUALS("fne_"),
    
    /** The greater than. */
    GREATER_THAN("fgt_"),
    
    /** The less than. */
    LESS_THAN("flt_"),
    
    /** The greater than or equals. */
    GREATER_THAN_OR_EQUALS("fge_"),
    
    /** The less than or equals. */
    LESS_THAN_OR_EQUALS("fle_"),
    
    /** The in list. */
    IN_LIST("fin_");
    
    /** The prefix. */
    private final String prefix;

    /**
     * Instantiates a new query operation util.
     *
     * @param prefix the prefix
     */
    private QueryOperationUtil(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Gets the prefix.
     *
     * @return the prefix
     */
    public String getPrefix() {
        return this.prefix;
    }
}
