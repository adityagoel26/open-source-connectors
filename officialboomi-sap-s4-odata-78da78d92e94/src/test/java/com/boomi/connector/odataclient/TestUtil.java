package com.boomi.connector.odataclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.Difference;
import org.xmlunit.diff.ElementSelectors;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;

public class TestUtil {
//	public static Map<ObjectDefinitionRole, String> getCookieList(ConnectorCookie cookie, ObjectDefinitionRole role) throws JsonProcessingException
//	{
//		HashMap<ObjectDefinitionRole, String> map = new HashMap<ObjectDefinitionRole, String>();
//		ObjectMapper mapper = new ObjectMapper();
//		map.put(role, mapper.writeValueAsString(cookie));
//		return map;
//	}
    public static void testBrowseTypes(Connector connector, OperationType operationType, String customOperationType, String swaggerPath, 
    		String connectorName, String nameSuffix, Class theClass, boolean writeExpected) throws JSONException, Exception
    {
        ConnectorTester tester = new ConnectorTester(connector);
        
        String actual;
        Map<String, Object> connProps = new HashMap<String,Object>();
        
        SimpleBrowseContext sbc = new SimpleBrowseContext(null, connector, operationType, customOperationType, connProps, null);
        tester.setBrowseContext(sbc);
//        tester.setBrowseContext(operationType, connProps, null);
        actual = tester.browseTypes();
//        System.out.println(operationType.toString());
//        System.out.println(actual);
//        SwaggerDocumentationUtil.prettyPrintTypes(connectorName, operationType.toString(), actual);
        String expectedFilePath = "testBrowseTypes_"+connectorName;
        if (nameSuffix!=null && nameSuffix.length()>0)
            expectedFilePath += "_"+nameSuffix;       	
        expectedFilePath += "_"+operationType.name();
        if (customOperationType!=null && customOperationType.length()>0)
            expectedFilePath += "_"+customOperationType;
        	
        compareXML(actual, expectedFilePath, theClass, connectorName, writeExpected);
    }
        
    public static void testBrowseTypes(Connector connector, OperationType operationType, String swaggerPath, String connectorName, Class theClass, boolean writeExpected) throws JSONException, Exception
    {
    	testBrowseTypes(connector, operationType, "", swaggerPath, connectorName, "", theClass, writeExpected);
    }
	public static void compareXML(String actual, String testName, Class theClass, String connectorName, boolean writeExpected) throws Exception
	{
		StringBuilder resolutionError = new StringBuilder();
		if (actual.contains("\"$ref\""))
			resolutionError.append("Definition includes unresolved $ref. ");
		if (actual.contains("\"allOf\""))
			resolutionError.append("Definition includes unresolved allOf. ");
		if (actual.contains("\"oneOf\""))
			resolutionError.append("Definition includes unresolved oneOf. ");
		if (actual.contains("\"anyOf\""))
			resolutionError.append("Definition includes unresolved anyOf. ");
		if (resolutionError.length()>0)
			throw new Exception(resolutionError.toString());
		
        actual = new String(actual.getBytes("UTF-8"));
		if (writeExpected)
		{
			String expectedDir = "src/test/java/resources/"+connectorName+"/expected/";
	        File dir = new File(expectedDir);
	        if (!dir.exists())
	        	dir.mkdirs();
			FileWriter writer = new FileWriter(expectedDir + testName+".xml");
			writer.write(actual);
			writer.flush();
			writer.close();
			return;
		}
        Document actualDoc = DocumentHelper.parseText(actual);        
//        System.out.println("ACTUAL\r\n"+toPrettyXML(actualDoc));

        String expected = readResource("resources/"+connectorName+"/expected/"+testName+".xml", theClass);
        expected = new String(expected.getBytes("UTF-8"));
        
        //ObjectDefinitions/definition[outputType='json']/jsonSchema
        Document expectedDoc = DocumentHelper.parseText(expected);        

        String def = getSingleNode(actualDoc,"/ObjectDefinitions/definition[@inputType='json']/jsonSchema");
        if (def!=null)
        {
//        	System.out.println("******INPUTPROFILE*******");
//            System.out.println((new JSONObject(def)).toString(2));        	
        }
//        System.out.println(expected);

	
       Diff myDiffSimilar;
       myDiffSimilar = DiffBuilder.compare(expected).withTest(actual)
	     .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byName))
	     .checkForSimilar().ignoreWhitespace()
	     .build();
       
        if (myDiffSimilar.hasDifferences())
        {
            System.out.println("");
            System.out.println(testName);
//          System.out.println(myDiffSimilar.toString());
          	System.out.println("actual: " + actual.length() + " expected: " + expected.length());
            System.out.println(myDiffSimilar.toString());
            for (Difference dif: myDiffSimilar.getDifferences())
            {
                System.out.println("DIFFERENCE");
                System.out.println(dif.toString());
            } 
            def = getSingleNode(actualDoc,"/ObjectDefinitions/definition[@outputType='json']/jsonSchema");
            if (def!=null)
            {
            	System.out.println("******OUTPUTPROFILE*******");
                System.out.println((new JSONObject(def)).toString());        	
            }
            def = getSingleNode(expectedDoc,"/ObjectDefinitions/definition[@outputType='json']/jsonSchema");
            if (def!=null)
            {
            	System.out.println("******OUTPUTPROFILE*******");
                System.out.println((new JSONObject(def)).toString());        	
            }
            System.out.println("ACTUAL\r\n"+toPrettyXML(actualDoc));
            System.out.println("EXPECTED\r\n"+toPrettyXML(expectedDoc));
        }

        assertTrue(!myDiffSimilar.hasDifferences());
        assertTrue(!myDiffSimilar.hasDifferences());
        assertEquals(actual.length(), expected.length());
	}

	private static String inputStreamToString(InputStream is) throws IOException
    {
    	try (Scanner scanner = new Scanner(is, "UTF-8")) {
    		return scanner.useDelimiter("\\A").next();
    	}
    }

	public static String readResource(String resourcePath, Class theClass) throws Exception
	{
		String resource = null;
		try {
			InputStream is = theClass.getClassLoader().getResourceAsStream(resourcePath);
			resource = inputStreamToString(is);
			
		} catch (Exception e)
		{
			throw new Exception("Error loading resource: "+resourcePath + " " + e.getMessage());
		}

		return resource;
	}
	
    private static void checkForDuplicateTypeNames(ObjectTypes objectTypes) throws Exception
    {
    	int i=0;
    	for (ObjectType objectType : objectTypes.getTypes())
    	{
    		System.out.println(objectType.getId());
    		int j=0;
        	for (ObjectType objectType2 : objectTypes.getTypes())
        	{
        		if (i!=j && objectType.getId().contentEquals(objectType2.getId()))
        		{
        			System.out.println(objectType.getLabel() + ":" + objectType2.getLabel());
        			throw new Exception("Duplicate:"+objectType.getId() + objectType.getLabel());        			
        		}
        		j++;
        	}
    		i++;
    	}
    }
	public static String toPrettyXML(Node document) {
		if (document==null)
			return null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
//			Document document = DocumentHelper.parseText("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"+node.asXML());
			OutputFormat format = new OutputFormat();
			format.setEncoding(StandardCharsets.UTF_8.name());
			format.setIndent(true);
			format.setIndentSize(2);
	        format.setNewlines(true);
			XMLWriter writer = new XMLWriter(baos, format);
			writer.write(document);
			writer.close();
			return baos.toString(StandardCharsets.UTF_8.name()).trim();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static boolean isNullOrEmpty(String str)
	{
		return str == null || str.trim().isEmpty() || str.contentEquals("null");
	}
	
	public static String getSingleNode(Node node, String path)
	{
		node = node.selectSingleNode(path);
		if (node != null)
			return node.getText();
		return null;
	}	
}
