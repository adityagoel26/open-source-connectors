package com.boomi.connector.odataclient;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.dom4j.DocumentException;
import org.json.JSONException;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * @author Dave Hock
 */
public class ODataClientSAPS4BrowseTestIT {
    static final String connectorName = "odataclient";
//    private static final boolean captureExpected = false;
    private static final String _baseUrl="https://my304976-api.s4hana.ondemand.com:443";
    private static final String _serviceSupplierMaster ="/sap/opu/odata/sap/MD_SUPPLIER_MASTER_SRV/";
    private static final String _serviceBusinessPartner ="/sap/opu/odata/sap/API_BUSINESS_PARTNER/";
    private static final String _serviceBOM ="/sap/opu/odata/sap/API_BILL_OF_MATERIAL_SRV;v=0002/";
    private String _objectTypeIdEmail = "A_AddressEmailAddress";
	private static Map<String, Object> _connProps;
	private static Map<String, Object> _opProps;

	@BeforeEach
    void init() {
		_connProps = new HashMap<String,Object>();
		_opProps = new HashMap<String,Object>();
        _connProps.put(ODataClientConnection.ConnectionProperties.URL.name(),_baseUrl);
        _connProps.put(ODataClientConnection.ConnectionProperties.USERNAME.name(), System. getenv("JUNIT_USERNAME"));//_credentials.get("username"));
        _connProps.put(ODataClientConnection.ConnectionProperties.PASSWORD.name(), System. getenv("JUNIT_PASSWORD"));//_credentials.get("password"));
        _connProps.put(ODataClientConnection.ConnectionProperties.AUTHTYPE.name(), "BASIC");
        _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(),_serviceSupplierMaster);
    }
	
    @Test
    public void testBrowseTypesCREATE_MD_SUPPLIER_MASTER_SRV() throws JSONException, Exception
    {
    	;
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
             
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.CREATE, null, _connProps, _opProps);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
        System.out.println(actual);
    }
       
    @Test
    public void testBrowseTypesFunctionImports_MD_SUPPLIER_MASTER_SRV() throws JSONException, Exception
    {
    	;
    	ODataClientConnector connector = new ODataClientConnector();
//    	ODataBrowseUtil.buildJSONSchema();
         ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "EXECUTE", _connProps, _opProps);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
        System.out.println(actual);
    }
       
    @Test
    public void testBrowseTypesFunctionImports_BOM() throws JSONException, Exception
    {
    	;
    	ODataClientConnector connector = new ODataClientConnector();
//    	ODataBrowseUtil.buildJSONSchema();
         ConnectorTester tester = new ConnectorTester(connector);
          _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBOM);
        
        String actual;
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "EXECUTE", _connProps, _opProps);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
        System.out.println(actual);
    }
       
    @Test
    public void testBrowseTypesGET_MD_SUPPLIER_MASTER_SRV() throws JSONException, Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "GET", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseTypes();
        System.out.println(actual);
    }
        
    @Test
    public void testBrowseDefinitionsQuery_A_BusinessPartner() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBusinessPartner);
      
        _opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 2L);        
        tester.setBrowseContext(OperationType.QUERY, _connProps, _opProps);
        
        String actual = tester.browseProfiles("A_BusinessPartner");
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
    @Test
    public void testBrowseDefinitionsEXECUTE_DeleteBOMHeaderWithECN() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
       
        _opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 2L);        
        _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBOM);
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "EXECUTE", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles("DeleteBOMHeaderWithECN");
        
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
    @Test
    public void testBrowseDefinitionsEXECUTE_ExcplodeBOMHeaderWithECN() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
       
        _opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 2L);        
        _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBOM);
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "EXECUTE", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles("ExplodeBOM");
        
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
    @Test
    public void testBrowseDefinitionsGet_C_BusinessPartner() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "GET", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles("C_BusinessPartner");
        System.out.println(actual);
    }    
    
    @Test
    public void testBrowseDefinitionsCreate_A_BusinessPartner() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
         _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBusinessPartner);
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "POST", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles("A_BusinessPartner");
        System.out.println(actual);
    }    
    
    @Test
    public void testBrowseDefinitionsCreateDeep_A_BusinessPartner() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
         _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBusinessPartner);
        _opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 2L);        
        _opProps.put(ODataClientBrowser.OperationProperties.DEEPCREATEMODE.name(), ODataClientBrowser.DeepCreateMode.DEEPCREATE.name());        
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "POST", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles("A_BusinessPartner");
        System.out.println(actual);
    }    
    
    @Test
    public void testBrowseDefinitionsCreate_Email() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
         _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBusinessPartner);
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "POST", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles(_objectTypeIdEmail);
        System.out.println(actual);
    }    

    @Test
    public void testBrowseDefinitionsCreate_MaterialBOM() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
         _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBOM);
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, OperationType.EXECUTE, "POST", _connProps, _opProps);
        tester.setBrowseContext(sbc);
        String actual = tester.browseProfiles("MaterialBOM");
        System.out.println(actual);
    }    
    
    @Test
    public void testBrowseDefinitionsQuery_MaterialBOM() throws JSONException, Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
         _opProps.put(ODataClientBrowser.OperationProperties.ALT_SERVICEPATH.name(), _serviceBOM);
        
        _opProps.put(ODataClientBrowser.OperationProperties.MAXBROWSEDEPTH.name(), 2L);        
        tester.setBrowseContext(OperationType.QUERY, _connProps, _opProps);
        
        String actual = tester.browseProfiles("MaterialBOM");
        System.out.println(actual);
//        SwaggerTestUtil.compareXML(actual, "testBrowseDefinitionsQuery!!CLASSNAME!!", getClass(), connectorName, captureExpected);
    }
    
//    @Test
    public void generateReadme () throws DocumentException, IOException
    {
    	DocumentationUtil.generateDescriptorDoc("SAP S4 OData API");
    }
}
