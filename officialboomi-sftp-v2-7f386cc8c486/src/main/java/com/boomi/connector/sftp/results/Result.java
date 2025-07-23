//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.results;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.TrackedData;

/**
 * The Interface Result.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public interface Result {
    
    /**
     * Adds the to response.
     *
     * @param var1 the var 1
     * @param var2 the var 2
     */
    public void addToResponse(OperationResponse var1, TrackedData var2);
}

