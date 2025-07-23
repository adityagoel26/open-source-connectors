package com.boomi.connector.odataclient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonParseException;

class ODataParseUtilTest {
	private static String odataPayload = "{\"Description\": \"Low fat milk\",\"Category\": {\"__deferred\": {\"uri\": \"https://services.odata.org/(S(wsql3bknhzpajxwwzymcpi01))/V2/OData/OData.svc/Products(1)/Category\"}},\"Price\": \"3.5\",\"Rating\": 3,\"Ingredients\": [{\"obj\": {\"key\": \"value\"},\"keys\": [],\"Amount\": 1,\"Name\": \"Lactose\"},{\"Amount\": 6,\"Name\": \"Fat\"},{\"Amount\": 2,\"Name\": \"Sugar\"}],\"DiscontinuedDate\": \"/Date(1582761600000)/\" ,\"CreatedTime\": \"PT19H54M36S\", \"Supplier\": {\"__deferred\": {\"uri\": \"https://services.odata.org/(S(wsql3bknhzpajxwwzymcpi01))/V2/OData/OData.svc/Products(1)/Supplier\"}},\"ID\": 1,\"__metadata\": {\"type\": \"ODataDemo.Product\",\"uri\": \"https://services.odata.org/(S(wsql3bknhzpajxwwzymcpi01))/V2/OData/OData.svc/Products(1)\"},\"ReleaseDate\": \"/Date(812505600000)/\",\"Name\": \"Milk\"}";
	private static String outputCookie = "{\"properties\":{\"/CreatedTime\":{\"type\":\"Time\"},\"/DiscontinuedDate\":{\"type\":\"DateTime\"},\"/ProductID\":{\"isKey\":true,\"type\":\"Int32\"}}}";
	private static String inputCookie = "{\"properties\":{\"/ProductID\":{\"isKey\":true,\"type\":\"Int32\"},\"/Amount\":{\"isKey\":true,\"type\":\"Decimal\",\"scale\":2}}}";
	private static String jsonPayload = "{\"ProductID\" : 3, \"Amount\" : 2}";
	
	private static String cookieAbusinesspartnerInputCreate = (new JSONObject(new JSONTokener(OdataClientSAPS4OperationTestIT.class.getClassLoader().getResourceAsStream("resources/cookieAbusinesspartnerInputCreate.json")))).toString();
	private static String requestAbusinesspartnerInputCreate = (new JSONObject(new JSONTokener(OdataClientSAPS4OperationTestIT.class.getClassLoader().getResourceAsStream("resources/requestAbusinesspartnerInputCreate.json")))).toString();

	
	@Test
	void testODataToBoomi() throws JsonParseException, IOException {
		ODataParseUtil oDataParseUtil = new ODataParseUtil();
		ByteArrayInputStream is = new ByteArrayInputStream(odataPayload.getBytes());
		//tempOutputStream = getContext().createTempOutputStream();
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		OperationCookie operationCookie = new OperationCookie(outputCookie);
		oDataParseUtil.parseODataToBoomi(is, os, operationCookie);
		//requestPayload = getContext().tempOutputStreamToInputStream(tempOutputStream);
		String outString = new String(os.toByteArray());
		JSONObject output = new JSONObject(outString);
		System.out.println(output.toString(2));
	}
	
	@Test
	void testBoomiToOData() throws JsonParseException, IOException, ParseException {
		ODataParseUtil oDataParseUtil = new ODataParseUtil();
		ByteArrayInputStream is = new ByteArrayInputStream(jsonPayload.getBytes());
		//tempOutputStream = getContext().createTempOutputStream();
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		OperationCookie operationCookie = new OperationCookie(inputCookie);
		String url = oDataParseUtil.parseBoomiToOData(is, os, operationCookie);
		//requestPayload = getContext().tempOutputStreamToInputStream(tempOutputStream);
		String outString = new String(os.toByteArray());
		JSONObject output = new JSONObject(outString);
		System.out.println(url);
		System.out.println(output.toString(2));
	}
	
	@Test
	void testBoomiToODataCreateBP() throws JsonParseException, IOException, ParseException {
		ODataParseUtil oDataParseUtil = new ODataParseUtil();
		ByteArrayInputStream is = new ByteArrayInputStream(requestAbusinesspartnerInputCreate.getBytes());
		//tempOutputStream = getContext().createTempOutputStream();
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		OperationCookie operationCookie = new OperationCookie(cookieAbusinesspartnerInputCreate);
		String url = oDataParseUtil.parseBoomiToOData(is, os, operationCookie);
		//requestPayload = getContext().tempOutputStreamToInputStream(tempOutputStream);
		String outString = new String(os.toByteArray());
		JSONObject output = new JSONObject(outString);
		System.out.println(url);
		System.out.println(output.toString(2));
	}
}
