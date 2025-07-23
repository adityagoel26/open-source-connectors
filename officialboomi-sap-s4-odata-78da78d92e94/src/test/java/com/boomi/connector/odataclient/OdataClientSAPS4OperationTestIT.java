// Copyright (c) 2018 Boomi, Inc.

package com.boomi.connector.odataclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.Sort;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.QueryFilterBuilder;
import com.boomi.connector.testutil.QueryGroupingBuilder;
import com.boomi.connector.testutil.QuerySimpleBuilder;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;

/**
 * @author Dave Hock
 */
//Create/Get/Delete in that order
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OdataClientSAPS4OperationTestIT
{
    private static final String servicePathBusinessPartner ="/sap/opu/odata/sap/API_BUSINESS_PARTNER/";
//    private static final String _urlSupplierMaster ="https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/MD_SUPPLIER_MASTER_SRV/";
    private static final String servicePathBOM ="/sap/opu/odata/sap/API_BILL_OF_MATERIAL_SRV;v=0002/";
    private static final String getPayload = "{\"BusinessPartner\":\"10100001\"}";
    private String _objectTypeIdBusinessPartner = "A_BusinessPartner";
    private String _objectTypeIdBOM = "MaterialBOM";
    private String _objectTypeIdEmail = "A_AddressEmailAddress";
//	private static JSONObject _credentials = new JSONObject(new JSONTokener(OdataClientSAPS4OperationTestIT.class.getClassLoader().getResourceAsStream("resources/testCredentials.json")));
	private static String _cookieBusinessPartner = (new JSONObject(new JSONTokener(OdataClientSAPS4OperationTestIT.class.getClassLoader().getResourceAsStream("resources/cookieBusinessPartner.json")))).toString();
	private static String _cookieBOM = (new JSONObject(new JSONTokener(OdataClientSAPS4OperationTestIT.class.getClassLoader().getResourceAsStream("resources/cookieBOM.json")))).toString();
	private static String _cookieEmail = (new JSONObject(new JSONTokener(OdataClientSAPS4OperationTestIT.class.getClassLoader().getResourceAsStream("resources/cookieEmail.json")))).toString();
	private static Map<String, Object> _connProps;
	String payload = "{\"BusinessPartner\":\"10100001\",\"Customer\":\"10100001\",\"Supplier\":\"\",\"AcademicTitle\":\"\",\"AuthorizationGroup\":\"\",\"BusinessPartnerCategory\":\"2\",\"BusinessPartnerFullName\":\"Inlandskunde DE 1\",\"BusinessPartnerGrouping\":\"BP03\",\"BusinessPartnerName\":\"Inlandskunde DE 1\",\"BusinessPartnerUUID\":\"fa163e3c-9712-1eea-96ae-7dd2fe02d0b2\",\"CorrespondenceLanguage\":\"\",\"CreatedByUser\":\"SAP_SYSTEM\",\"FirstName\":\"\",\"FormOfAddress\":\"0003\",\"Industry\":\"\",\"InternationalLocationNumber1\":\"0\",\"InternationalLocationNumber2\":\"0\",\"IsFemale\":false,\"IsMale\":false,\"IsNaturalPerson\":\"\",\"IsSexUnknown\":false,\"GenderCodeName\":\"\",\"Language\":\"\",\"LastChangedByUser\":\"\",\"LastName\":\"\",\"LegalForm\":\"01\",\"OrganizationBPName1\":\"Inlandskunde DE 1\",\"OrganizationBPName2\":\"\",\"OrganizationBPName3\":\"\",\"OrganizationBPName4\":\"\",\"OrganizationFoundationDate\":null,\"OrganizationLiquidationDate\":null,\"SearchTerm1\":\"KUNDE1\",\"SearchTerm2\":\"\",\"AdditionalLastName\":\"\",\"BirthDate\":null,\"BusinessPartnerBirthplaceName\":\"\",\"BusinessPartnerIsBlocked\":false,\"BusinessPartnerType\":\"\",\"ETag\":\"SAP_SYSTEM20200227151504\",\"GroupBusinessPartnerName1\":\"\",\"GroupBusinessPartnerName2\":\"\",\"IndependentAddressID\":\"\",\"InternationalLocationNumber3\":\"0\",\"MiddleName\":\"\",\"NameCountry\":\"\",\"NameFormat\":\"\",\"PersonFullName\":\"\",\"PersonNumber\":\"\",\"IsMarkedForArchiving\":false,\"BusinessPartnerIDByExtSystem\":\"\",\"TradingPartner\":\"\",\"to_BuPaIdentification\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BuPaIdentification\"}},\"to_BuPaIndustry\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BuPaIndustry\"}},\"to_BusinessPartnerAddress\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BusinessPartnerAddress\"}},\"to_BusinessPartnerBank\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BusinessPartnerBank\"}},\"to_BusinessPartnerContact\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BusinessPartnerContact\"}},\"to_BusinessPartnerRole\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BusinessPartnerRole\"}},\"to_BusinessPartnerTax\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_BusinessPartnerTax\"}},\"to_Customer\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_Customer\"}},\"to_Supplier\":{\"__deferred\":{\"uri\":\"https://my304976-api.s4hana.ondemand.com:443/sap/opu/odata/sap/API_BUSINESS_PARTNER/A_BusinessPartner('10100001')/to_Supplier\"}}}"; 

	@BeforeAll
    static void initAll() {
		_connProps = new HashMap<String,Object>();
        _connProps.put(ODataClientConnection.ConnectionProperties.URL.name(), "https://my304976-api.s4hana.ondemand.com:443");
        _connProps.put(ODataClientConnection.ConnectionProperties.USERNAME.name(), System. getenv("JUNIT_USERNAME"));//_credentials.get("username"));
        _connProps.put(ODataClientConnection.ConnectionProperties.PASSWORD.name(), System. getenv("JUNIT_PASSWORD"));//_credentials.get("password"));
        _connProps.put(ODataClientConnection.ConnectionProperties.AUTHTYPE.name(), "BASIC");
    }	
    
    @Test   
    @Order(1)
    public void testCreateBusinessPartnerOperation() throws Exception
    {
        ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put("FETCH_HEADERS", true);
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBOM);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieBusinessPartner);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
		
String payload = "{\r\n" + 
//		"      \"IndustrySector\" : \"023\",\r\n" + 
		"      \"IndustrySystemType\" : \"0002\",\r\n" + 
		"      \"BusinessPartner\" : \"15444\",\r\n" + 
		"      \"IsStandardIndustry\" : \"\",\r\n" + 
		"      \"to_BusinessPartnerAddress\" : {\"BusinessPartner\" : \"15444\",\r\n" + 
		"      \"AddressID\" : \"15444\"}\r\n" + 
		"}";
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdBusinessPartner, cookieMap, null);
        context.setCustomOperationType("POST");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(payload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        String responseString = new String(actual.get(0).getPayloads().get(0));
//        JSONObject responseObject = new JSONObject(responseString);
//        _petID = responseObject.getLong("id");
        System.out.println(responseString);
    }    
    
    @Test   
    @Order(2)
    public void testGetBusinessPartnerOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
        
        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBusinessPartner);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieBusinessPartner);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdBusinessPartner, cookieMap, null);
        context.setCustomOperationType("GET");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(getPayload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
    }
    
    

    @Test 
    //TODO can't test execute/delete until we figure out how to pass the customtype
    @Order(3)
    public void testDeleteBusinessPartnerOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBusinessPartner);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieBusinessPartner);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdBusinessPartner, cookieMap, null);
        context.setCustomOperationType("DELETE");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(getPayload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
    }
    
    @Test 
    public void testPUTBusinessPartnerOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put("FETCH_HEADERS", true);
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBusinessPartner);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieBusinessPartner);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdBusinessPartner, cookieMap, null);
        context.setCustomOperationType("PUT");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(payload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("No Content", actual.get(0).getMessage());
        assertEquals("204",actual.get(0).getStatusCode());
    }
    
    @Test 
    public void testPATCHBusinessPartnerOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put("FETCH_HEADERS", true);
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBusinessPartner);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieBusinessPartner);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdBusinessPartner, cookieMap, null);
        context.setCustomOperationType("PATCH");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(payload.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("No Content", actual.get(0).getMessage());
        assertEquals("204",actual.get(0).getStatusCode());
    }
    
    @Test   
    public void testQueryBusinessPartnerOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.PAGESIZE.name(), 100L);
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBusinessPartner);
        
        QueryFilter qf =  new QueryFilterBuilder(QueryGroupingBuilder.and(
              new QuerySimpleBuilder("FirstName", "eq", "Susan"),
              
              new QuerySimpleBuilder("LastName", "substring", "Mil")             
//              ,new QuerySimpleBuilder("CreationDate", "lt", "2010-09-01")
              )).toFilter();
        
        Sort sort = new Sort();
        sort.setProperty("FirstName");
        sort.setSortOrder("desc");
//        qf.getSort().add(sort);
        sort = new Sort();
        sort.setProperty("LastName");
//        sort.setSortOrder("asc");
//        qf.getSort().add(sort);
  

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
        tester.setOperationContext(OperationType.QUERY, _connProps, opProps, _objectTypeIdBusinessPartner, cookieMap);
        qf=null;
        List <SimpleOperationResult> actual = tester.executeQueryOperation(qf);
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(195, actual.get(0).getPayloads().size());
    } 
    
    @Test   
    public void testQueryBOMOperation() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.PAGESIZE.name(), 100L);
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBOM);

        QueryFilter qf = new QueryFilter();

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBOM);
		List<String> selectedFields = new ArrayList<String>();
//		/to_BillOfMaterialUsage/to_BillOfMaterialUsageText,to_BillOfMaterialItem/to_BOMItemCategory/to_BOMItemCategoryText&$top=20
		selectedFields.add("to_BillOfMaterialUsage/to_BillOfMaterialUsageText");
		selectedFields.add("to_BillOfMaterialItem/to_BOMItemCategory/to_BOMItemCategoryText");
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.QUERY, _connProps, opProps, _objectTypeIdBOM, cookieMap, selectedFields);	
		tester.setOperationContext(context);
        List <SimpleOperationResult> actual = tester.executeQueryOperation(qf);
        assertEquals("OK", actual.get(0).getMessage());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(158, actual.get(0).getPayloads().size());
    } 

    @Test   
    public void testEmailCRUD() throws Exception
    {
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);
		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieEmail);

        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.PAGESIZE.name(), 100L);
        opProps.put(ODataClientQueryOperation.OperationProperties.MAXDOCUMENTS.name(), 1L);
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBusinessPartner);
        
        //QUERY
        tester.setOperationContext(OperationType.QUERY, _connProps, opProps, _objectTypeIdEmail, cookieMap);
        List <SimpleOperationResult> actual = tester.executeQueryOperation(null);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        String responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        
        //GET
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieEmail);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdEmail, cookieMap, null);
        context.setCustomOperationType("GET");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(responseString.getBytes()));
        actual = tester.executeExecuteOperation(inputs);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        
        //XCSRF and ETAG
        SimplePayloadMetadata payloadMetadata = actual.get(0).getPayloadMetadatas().get(0);
        String xcsrf = payloadMetadata.getTrackedProps().get("X_CSRF_TOKEN");
        String etag = payloadMetadata.getTrackedProps().get("ETAG");
        String sessionCookie = payloadMetadata.getTrackedProps().get("SAP_SESSIONID_COOKIE");
        
        System.out.println(String.format("X_CSRF_TOKEN: %s, ETAG: %s, SAP_SESSIONID_COOKIE: %s", xcsrf, etag, sessionCookie ));
        
        //POST
        String createPayload = "{\r\n" + 
        		"  \"AddressID\": \"23186\",\r\n" + 
        		"  \"Person\": \"23198\",\r\n" + 
        		"  \"OrdinalNumber\": \"1\",\r\n" + 
        		"  \"IsDefaultEmailAddress\": true,\r\n" + 
        		"  \"EmailAddress\": \"test@test.com\",\r\n" + 
        		"  \"SearchEmailAddress\": \"test@test.com\",\r\n" + 
        		"  \"AddressCommunicationRemarkText\": \"\"\r\n" + 
        		"}";
        context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdEmail, cookieMap, null);
        context.setCustomOperationType("POST");
        tester.setOperationContext(context);
        List<SimpleTrackedData> trackedInputs = new ArrayList<SimpleTrackedData>();
		Map<String, String> dynamicProps = new HashMap<String, String>();
		dynamicProps.put("X_CSRF_TOKEN", xcsrf);
		dynamicProps.put("SAP_SESSIONID_COOKIE", sessionCookie);
//		dynamicProps.put("ETAG", etag);
		SimpleTrackedData trackedData = new SimpleTrackedData(0, new ByteArrayInputStream(createPayload.getBytes()), null, dynamicProps);
        trackedInputs.add(trackedData);
		actual = tester.executeExecuteOperationWithTrackedData(trackedInputs);
        responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        assertEquals("Created", actual.get(0).getMessage());
        assertEquals("201",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());   
//        payloadMetadata = actual.get(0).getPayloadMetadatas().get(0);
//        xcsrf = payloadMetadata.getTrackedProps().get("X_CSRF_TOKEN");
//        etag = payloadMetadata.getTrackedProps().get("ETAG");
//        System.out.println(String.format("X_CSRF_TOKEN : %s, ETAG %s", xcsrf,etag ));
        
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieEmail);
		context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdEmail, cookieMap, null);
        context.setCustomOperationType("GET");
        tester.setOperationContext(context);
        inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(responseString.getBytes()));
        actual = tester.executeExecuteOperation(inputs);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("200",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());
        responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
      payloadMetadata = actual.get(0).getPayloadMetadatas().get(0);
      xcsrf = payloadMetadata.getTrackedProps().get("X_CSRF_TOKEN");
      etag = payloadMetadata.getTrackedProps().get("ETAG");
      System.out.println(String.format("X_CSRF_TOKEN : %s, ETAG %s", xcsrf,etag ));
        
      opProps.put("FETCH_HEADERS", true);
        //PUT
        context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, _objectTypeIdEmail, cookieMap, null);
        context.setCustomOperationType("PUT");
        tester.setOperationContext(context);
        trackedInputs = new ArrayList<SimpleTrackedData>();
		dynamicProps = new HashMap<String, String>();
		dynamicProps.put("X_CSRF_TOKEN", xcsrf);
		dynamicProps.put("ETAG", etag);
		dynamicProps.put("SAP_SESSIONID_COOKIE", sessionCookie);
		trackedData = new SimpleTrackedData(0, new ByteArrayInputStream(createPayload.getBytes()), null, dynamicProps);
        trackedInputs.add(trackedData);
		actual = tester.executeExecuteOperationWithTrackedData(trackedInputs);
        responseString = new String(actual.get(0).getPayloads().get(0));
        System.out.println(responseString);
        assertEquals("OK", actual.get(0).getMessage());
        assertEquals("201",actual.get(0).getStatusCode());
        assertEquals(1, actual.get(0).getPayloads().size());          
    } 
    
    @Test
    void executeExplodeBOM()
    {
    	String request = "{\r\n" + 
    			"\"BillOfMaterial\":\"00058298\",\r\n" + 
    			"\"BillOfMaterialCategory\":\"M\",\r\n" + 
    			"\"BillOfMaterialVariant\":\"1\",\r\n" + 
    			"\"BillOfMaterialVersion\":\"\",\r\n" + 
    			"\"EngineeringChangeDocument\":\"\",\r\n" + 
    			"\"Material\":\"EX_H\",\r\n" + 
    			"\"Plant\":\"0001\",\r\n" + 
    			"\"BillOfMaterialItemCategory\":\"\",\r\n" + 
    			"\"BOMExplosionApplication\":\"PP01\",\r\n" + 
    			"\"BOMExplosionAssembly\":\"\",\r\n" + 
    			"\"BOMExplosionDate\":\"2019-12-07T00:00:00\",\r\n" + 
    			"\"BOMExplosionIsLimited\":false,\r\n" + 
    			"\"BOMExplosionIsMultilevel\":true,\r\n" + 
    			"\"BOMExplosionLevel\":2,\r\n" + 
    			"\"BOMItmQtyIsScrapRelevant\":\"\",\r\n" + 
    			"\"MaterialProvisionFltrType\":\" \",\r\n" + 
    			"\"RequiredQuantity\":60,\r\n" + 
    			"\"SparePartFltrType\":\" \"\r\n" + 
    			"}";
    	ODataClientConnector connector = new ODataClientConnector();
        ConnectorTester tester = new ConnectorTester(connector);

		HashMap<ObjectDefinitionRole, String> cookieMap = new HashMap<ObjectDefinitionRole, String>();
		cookieMap.put(ObjectDefinitionRole.INPUT, _cookieBusinessPartner);
		cookieMap.put(ObjectDefinitionRole.OUTPUT, _cookieBusinessPartner);
        Map<String, Object> opProps = new HashMap<String,Object>();
        opProps.put(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name(), servicePathBOM);
		MockOperationContext context = new MockOperationContext(null, connector, OperationType.EXECUTE, _connProps, opProps, "ExplodeBOM", cookieMap, null);
        context.setCustomOperationType("EXECUTE");
        tester.setOperationContext(context);
        List<InputStream> inputs = new ArrayList<InputStream>();
        inputs.add(new ByteArrayInputStream(request.getBytes()));
        List <SimpleOperationResult> actual = tester.executeExecuteOperation(inputs);
        assertEquals("No Content", actual.get(0).getMessage());
        assertEquals("204",actual.get(0).getStatusCode());
    }
}
