//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.results;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;

/**
 * The Class EmptySuccess.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class EmptySuccess
implements Result {
    
    /**
     * Adds the to response.
     *
     * @param response the response
     * @param input the input
     */
    @Override
    public void addToResponse(OperationResponse response, TrackedData input) {
        ResponseUtil.addEmptySuccess(response, input,"0");
    }
}

