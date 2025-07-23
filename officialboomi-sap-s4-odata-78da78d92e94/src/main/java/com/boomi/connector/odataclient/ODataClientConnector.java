package com.boomi.connector.odataclient;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;

public class ODataClientConnector extends BaseConnector {

	static final PoolingHttpClientConnectionManager GLOBAL_CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();

	static {
		GLOBAL_CONNECTION_MANAGER.setMaxTotal(50);
		GLOBAL_CONNECTION_MANAGER.setDefaultMaxPerRoute(10);
	}

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new ODataClientBrowser(createConnection(context));
    }    

    @Override
    protected Operation createQueryOperation(OperationContext context) {
        return new ODataClientQueryOperation(createConnection(context));
    }

    @Override
    protected Operation createExecuteOperation(OperationContext context) {
        return new ODataClientExecuteOperation(createConnection(context));
    }
   
    private ODataClientConnection createConnection(BrowseContext context) {
        return new ODataClientConnection(context);
    }

	@Override
	protected void finalize() throws Throwable {
		//TODO THIS BREAKS JUNIT WE MUST FIX JUNIT
	   	super.finalize();
	}
}