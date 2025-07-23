package com.boomi.connector.odataclient;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;

import org.junit.jupiter.api.Test;


/**
 * @author Dave Hock
 */
public class ODataClientNorthwindsBrowseTestIT {
    static final String connectorName = "odataclient";
//    private static final boolean captureExpected = false;
//    private static final String baseUrl ="https://services.odata.org/V3/(S(dy1on0tblaxz1e1nxngitrpr))/OData/OData.svc/";
//    private static final String baseUrl ="https://services.odata.org/V3/OData/OData.svc/";
    private static final String baseUrl ="https://services.odata.org/V3/Northwind";
    private static final String servicePath = "/Northwind.svc/";
//    private static final String baseUrl ="https://services.odata.org/V3/OData/OData.svc/";
    

    @Test
    public void testBrowseTypesCREATE() throws JSONException, Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
//    	ODataBrowseUtil.buildJSONSchema();
         ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        Map<String, Object> opProps =  new HashMap<String,Object>();
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.CREATE, null, connProps, opProps);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
        System.out.println(actual);
    }
       
    @Test
    public void testBrowseTypesGET() throws JSONException, Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
//    	ODataBrowseUtil.buildJSONSchema();
         ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        Map<String, Object> opProps =  new HashMap<String,Object>();
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "GET", connProps, opProps);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
        System.out.println(actual);
    }
    
    @Test
    public void testBrowseTypesUPDATE() throws JSONException, Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
//    	ODataBrowseUtil.buildJSONSchema();
         ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        Map<String, Object> opProps =  new HashMap<String,Object>();
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.UPDATE, null, connProps, opProps);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
        System.out.println(actual);
    }
                    
    @Test
    public void testBrowseDefinitionsQueryOrders() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        Map<String, Object> opProps = new HashMap<String,Object>();
        
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 3L);        
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
        
        tester.setBrowseContext(OperationType.QUERY, connProps, opProps);
        String objectTypeId = "Orders";
//        String objectTypeId = "C_BusinessPartner";
        
        actual = tester.browseProfiles("Orders");
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
    @Test
    public void testBrowseDefinitionsQueryCustomers() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        Map<String, Object> opProps = new HashMap<String,Object>();
        
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 3L);        
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
       
        tester.setBrowseContext(OperationType.QUERY, connProps, opProps);
        String objectTypeId = "Customers";
//        String objectTypeId = "C_BusinessPartner";
        
        actual = tester.browseProfiles("Customers");
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
    @Test
    public void testBrowseDefinitionsEXECUTEGET() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        Map<String, Object> opProps = new HashMap<String,Object>();
        
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
         
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "GET", connProps, opProps);
        tester.setBrowseContext(sbc);
        String objectTypeId = "Customers";
//        String objectTypeId = "C_BusinessPartner";
        actual = tester.browseProfiles("Products");
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
    @Test
    public void testBrowseDefinitionsUpdate() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        Map<String, Object> opProps = new HashMap<String,Object>();
        
        connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), baseUrl);
        opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), servicePath);
         SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "PUT", connProps, opProps);
        tester.setBrowseContext(sbc);
        String objectTypeId = "Customers";
//        String objectTypeId = "C_BusinessPartner";
        
        actual = tester.browseProfiles("Customers");
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
//    @Test
//    public void testBrowseDefinitionsExecuteGet!!CLASSNAME!!() throws JSONException, Exception
//    {
//        !!CLASSNAME!!Connector connector = new !!CLASSNAME!!Connector();
//        ConnectorTester tester = new ConnectorTester(connector);
//        
//        String actual;
//        Map<String, Object> connProps = new HashMap<String,Object>();
//        
//        connProps.put(SwaggerConnection.ConnectionProperties.SWAGGERURL.name(), swaggerPath);
//        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "GET", connProps, null);
//        tester.setBrowseContext(sbc);
//        actual = tester.browseProfiles("/learn/api/public/v1/users/{userId}___get");
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsExecuteGet!!CLASSNAME!!", getClass(), connectorName, captureExpected);
//    }    
//    
//    @Test
//    public void testBrowseDefinitionsCreate!!CLASSNAME!!() throws JSONException, Exception
//    {
//        !!CLASSNAME!!Connector connector = new !!CLASSNAME!!Connector();
//        ConnectorTester tester = new ConnectorTester(connector);
//        
//        String actual;
//        Map<String, Object> connProps = new HashMap<String,Object>();
//        
//        connProps.put(SwaggerConnection.ConnectionProperties.SWAGGERURL.name(), swaggerPath);
//        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.CREATE, null, connProps, null);
//        tester.setBrowseContext(sbc);
//        actual = tester.browseProfiles("/learn/api/public/v1/users___post");
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsCreate!!CLASSNAME!!", getClass(), connectorName, captureExpected);
//    }    
    
}
