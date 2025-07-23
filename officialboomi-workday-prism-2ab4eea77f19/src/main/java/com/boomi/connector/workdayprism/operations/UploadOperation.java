//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.UploadResponse;
import com.boomi.connector.workdayprism.operations.upload.UploadHelper;
import com.boomi.util.IOUtil;
import java.util.logging.Level;

/**
 * @author juan.paccapelo <juan.paccapelo@boomi.com>
 */
public class UploadOperation extends BaseUpdateOperation {

    /**
     * Creates a new UploadOperation instance
     *
     * @param connection
     *         an PrismConnection instance
     */
    public UploadOperation(PrismOperationConnection connection) {
        super(connection);
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse opResponse) {
        UploadHelper uploadHelper = new UploadHelper(getConnection()); 
        for (ObjectData data : request) {
            UploadResponse response = null;
            try {   
            	response = uploadHelper.upload(data, null);
                response.addResult(data, opResponse);
            }
            catch (ConnectorException e) {
                data.getLogger().log(Level.WARNING, e.getMessage(), e);
                opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getStatusMessage(),
                        null);
            }
            catch (Exception e) {
                ResponseUtil.addExceptionFailure(opResponse, data, e);
            }
            finally {
                IOUtil.closeQuietly(response);
            }
        }
    }

    @Override
    public PrismOperationConnection getConnection() {
        return (PrismOperationConnection) super.getConnection();
    }

}