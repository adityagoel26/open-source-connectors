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

public class DocumentationUtil {
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
	
	public static String getSingleNode(Node node, String path)
	{
		node = node.selectSingleNode(path);
		if (node != null)
			return node.getText();
		return null;
	}
	
	private static void createDocumentationDirectory(String connectorName)
	{
		String dirName = "documentation/"+connectorName;
        File dir = new File(dirName);
        if (!dir.exists())
        	dir.mkdirs();
	}
	
    public static void prettyPrintTypes(String operationName, Document typesDocument, FileWriter writer) throws DocumentException, IOException
    {
    	writer.write("### " + operationName.toUpperCase() + " Operation Types\n");
    	writer.write("| Label | Help Text | ID |\n");
    	writer.write("| --- | --- | --- |\n");
        List<Node> nodes = typesDocument.selectNodes("/ObjectTypes/*");
        
        for (Node node : nodes)
        {
        	Element element = (Element)node;
        	String id = element.attributeValue("id").replace("_", "\\_");
        	String helpText = getNodeText(node,"helpText");

        	writer.write(String.format("| %s | %s | %s |\n", element.attributeValue("label"), helpText, id));
        }
        writer.write("\n\n");
//    	<ObjectTypes>
//    	  <type id="/api/v1/users/{userId}/sessions___delete" label="ClearUserSessions - Removes all active identity provider sessions. Thi...">
//    	    <helpText>Removes all active identity provider sessions. Thi...</helpText>
//    	  </type>   	
 //   	 getSingleNode(actualDoc,"/ObjectDefinitions/definition[@inputType='json']/jsonSchema");
    }

	public static void generateOperationTypeDocs(String connectorName) throws JSONException, IOException, DocumentException {
		
		FileWriter writer = new FileWriter("README.md", true);

		writer.write("\n# Operations and Object Types Provided\n\n");

		File folder = new File("src/test/java/resources/"+connectorName+"/expected");
		File[] listOfFiles = folder.listFiles();

		for (File file : listOfFiles) {
		    if (file.isFile()) {
		    	String operation=null;
		    	if(file.getName().endsWith("CREATE.xml"))
		    		operation="CREATE";
		    	else if(file.getName().endsWith("UPDATE.xml"))
		    		operation="UPDATE";
		    	else if(file.getName().endsWith("DELETE.xml"))
		    		operation="DELETE";
		    	else if(file.getName().endsWith("GET.xml"))
		    		operation="GET";
		    	else if(file.getName().endsWith("QUERY.xml"))
		    		operation="QUERY";
		    	if (operation!=null)
		    	{
			        SAXReader reader = new SAXReader();
					Document document = reader.read(file);
			    	prettyPrintTypes(operation, document, writer);
		    	}
		    }
		}
		writer.flush();
		writer.close();
	}	   
	
	private static String getNodeText(Node field, String elementName)
	{
		if (field.selectSingleNode(elementName)!=null)
		{
			String text = field.selectSingleNode(elementName).getText();
			if (text!=null && text.trim().length()>0)
				return text;
		}		
		return " **" + elementName + " NOT SET IN DESCRIPTOR FILE**";
	}
	
	private static void writeDescriptorFields(List<Node> fields, StringBuilder sb, String indent)
	{
        for (Node field : fields)
        {
        	Element fieldElement = (Element)field;
        	sb.append("\n\n#" + indent +" "+fieldElement.attributeValue("label")+"\n\n");
        	sb.append(getNodeText(field,"helpText")+"\n\n");
        	if (fieldElement.attributeValue("type")!=null)
        		sb.append("**Type** - " + fieldElement.attributeValue("type")+"\n\n");
        	if (fieldElement.attributeValue("formatRegex")!=null)
        		sb.append("**Format** - " + fieldElement.attributeValue("formatRegex")+"\n\n");
        	if (field.selectSingleNode("defaultValue")!=null && field.selectSingleNode("defaultValue").getText().length()>0)
        		sb.append("**Default Value** - " + field.selectSingleNode("defaultValue").getText()+"\n\n");
        	List<Node> allowedValues = field.selectNodes("allowedValue");
        	if (allowedValues.size()>0)
            	sb.append("##"+indent + " Allowed Values\n");
        	for (Node allowedValue : allowedValues)
        	{
            	String label =  ((Element)allowedValue).attributeValue("label");
            	if (label!=null && label.trim().length()>0)
            		sb.append(" * " + label +"\n");
           	}
        	sb.append("\n");
        }
	}
	
	public static void generateDescriptorDoc(String productName) throws DocumentException, IOException
	{
		File descriptorFile = new File("src/main/resources/connector-descriptor.xml");
		StringBuilder sb=new StringBuilder();
        SAXReader reader = new SAXReader();
		Document document = reader.read(descriptorFile);
        
        sb.append("# "+productName+"\n\n");
    	sb.append(getNodeText(document,"/GenericConnectorDescriptor/description")+"\n\n");
        
        sb.append("# Connection Tab\n\n");
        List<Node> fields = document.selectNodes("/GenericConnectorDescriptor/field");
        if (fields.size()>0)
        {
            sb.append("## Connection Fields\n");
            writeDescriptorFields(fields, sb, "##");
        }
        sb.append("# Operation Tab\n\n");
        List<Node> operations = document.selectNodes("/GenericConnectorDescriptor/operation");
        for (Node operation : operations)
        {
        	Element operationElement = (Element)operation;
        	String operationName=operationElement.attributeValue("types");
        	String label=operationName;
        	if (operationElement.attributeValue("customTypeLabel")!=null)
        		label=operationElement.attributeValue("customTypeLabel");
        	sb.append("\n\n## "+label+"\n");
            List<Node> operationFields = operation.selectNodes("field");
            if (operationFields.size()>0)
            {
                sb.append("### Operation Fields\n\n");
                writeDescriptorFields(operationFields, sb, "###");
            }
            if (operationName.contentEquals("QUERY"))
    		{
            	sb.append("\n### Query Options\n\n");

            	sb.append("\n#### Fields\n\n");
            	// <operation types="QUERY" allowFieldSelection="false" fieldSelectionLevels="0" fieldSelectionNone="false">
            	if ("true".contentEquals(operationElement.attributeValue("allowFieldSelection")))
            	{
            		sb.append("Use the checkboxes in the *Fields* list to select which fields are returned by the Query operation. Selecting only the fields required can improve performance.");
            	} else {
            		sb.append("The connector does not support field selection. All fields will be returned by default.");
            	}
            	sb.append("\n");
            	sb.append("\n");
//        		<query filter grouping="none" sorting="none">
//    			<operator id="eq" label="Equal To">  
//    			</operator>
//    			</query filter>
            	
            	Node filter = operation.selectSingleNode("queryFilter");
            	if (filter!=null)
            	{
                	sb.append("\n#### Filters\n\n");
            		Element filterElement = (Element)filter;
            		String grouping = filterElement.attributeValue("grouping");
            		if (grouping==null)
            			grouping="none";
                	switch (grouping)
                	{
                	case "none":
                		sb.append("The query filter supports no groupings (only one expression allowed).\r\n" + 
                				"\r\n" + 
                				"Example:\r\n" + 
                				"(foo lessThan 30)");
                		break;
                	case "any":
                		sb.append(" The query filter supports any arbitrary grouping and nesting of AND's and OR's.\r\n" + 
                				"\r\n" + 
                				"Example:\r\n" + 
                				"((foo lessThan 30) OR (baz lessThan 42) OR ((bar isNull) AND (bazz isNotNull))) AND (buzz greaterThan 55)");
                		break;
                	case "noNestingImplicitOr":
                		sb.append(" The query filter supports any number of non-nested expressions which will be OR'ed together.\r\n" + 
                				"\r\n" + 
                				"Example:\r\n" + 
                				"((foo lessThan 30) OR (baz lessThan 42))");
                		break;
                	case "noNestingImplicitAnd":
                		sb.append("The query filter supports any number of non-nested expressions which will be AND'ed together.\r\n" + 
                				"\r\n" + 
                				"Example:\r\n" + 
                				"((foo lessThan 30) AND (baz lessThan 42))");
                		break;
                	case "singleNestingImplicitOr":
                		sb.append("The query filter supports one level of nesting, where the top level groupings will be OR'ed together and the nested expression groups will be AND'ed together.\r\n" + 
                				"\r\n" + 
                				"Example:\r\n" + 
                				"((foo lessThan 30) AND (baz lessThan 42)) OR (buzz greaterThan 55)");
                		break;
                	case "singleNestingImplicitAnd":
                		sb.append("The query filter supports one level of nesting, where the top level groupings will be AND'ed together and the nested expression groups will be OR'ed together.\r\n" + 
                				"\r\n" + 
                				"Example:\r\n" + 
                				"((foo lessThan 30) OR (baz lessThan 42)) AND (buzz greaterThan 55)");
                		break;
                	}
                	sb.append("\n");
                	sb.append("\n");
                	sb.append("#### Filter Operators\n\n");
                    List<Node> operators = filter.selectNodes("operator");
                    for (Node operator : operators)
                    {
                    	Element operatElement = (Element)operator;
                    	sb.append(" * " + ((Element)operator).attributeValue("label")+" ");
                    	List<Node> types = operator.selectNodes("supportedType");
                    	if (types!=null && types.size()>0)
                    	{
                        	boolean firstType=true;
                        	sb.append(" (Supported Types: ");
                            for (Node type : types)
                            {
                               	if (!firstType)
                            		sb.append(",");
                        		firstType=false;
                        		sb.append(" " + ((Element)type).attributeValue("type"));
                            }
                        	sb.append(") ");
                    	}
                    	sb.append(getNodeText(operator,"helpText")+"\n\n");
                    }
                    
//        			<operator id="greaterOrEqual" label="Greater Than Or Equal">
//                	<helpText>
//    				<supportedType type="date" />
//    			</operator>
                	sb.append("\n");
                	sb.append("\n");
                	sb.append("#### Sorts\n\n");
            		String sorting = filterElement.attributeValue("sorting");
            		if (sorting==null)
            			sorting="none";
                	switch (grouping)
                	{
                	case "none":
                		sb.append("The query operation does not support sorting.");
                		break;
                	case "one":
                		sb.append("The query operation supports one sorting statement. ");
                		break;
                	case "unbounded":
                		sb.append("The query operation supports any number of sorting levels. For example you can specify to sort by Last Name, First Name. ");
                		break;
                	}
                	sb.append("\n");
                	sb.append("\n");
                    if (!"none".contentEquals(sorting))
                    {
                        List<Node> sortOrderFields = filter.selectNodes("sortOrder");
                        if (sortOrderFields!=null && sortOrderFields.size()>1)
                        {
                        	sb.append("The sort order can be set to either ascending and descinding.");
                        } else {
                        	sb.append("Only one direction of sorting is supported.");
                        }
                    }
//    			<sortOrder id="asc" label="Ascending"/>
//    			<sortOrder id="desc" label="Descending"/>
            	}
            	else
            	{
                	sb.append("\n#### Filters\n\n");
            		sb.append("The connector does not support filtering documents\n");
                	sb.append("\n#### Sorts\n\n");
            		sb.append("The connector does not support sorting documents\n");
            	}
            	sb.append("\n");
    		}
        }
        sb.append("\n# Inbound Document Properties\n");
        
        List<Node> inputFields = document.selectNodes("/GenericConnectorDescriptor/dynamicProperty");
        if (inputFields!=null && inputFields.size()>0)
        {
        	sb.append("Inbound document properties can set by a process before a connector shape to control options supported by the connector.\n\n");
            for (Node field : inputFields)
            {
            	Element fieldElement = (Element)field;
            	sb.append(" * "+fieldElement.attributeValue("label")+"\n");
            }
        } else {
        	sb.append("The connector does not support inbound document properties that can be set by a process before an connector shape.\n\n");
        }
        	
        sb.append("\n# Outbound Document Properties\n\n");
        
        List<Node> outputFields = document.selectNodes("/GenericConnectorDescriptor/trackedProperty");
        if (outputFields!=null && outputFields.size()>0)
        {
        	sb.append("Outbound document properties can used by a process after a connector shape to access information set by the connector.\n\n");
            for (Node field : outputFields)
            {
            	Element fieldElement = (Element)field;
            	sb.append(" * "+fieldElement.attributeValue("label")+"\n");
            }
        } else {
        	sb.append("The connector does not support outbound document properties that can be read by a process after a connector shape.\n\n");
        }
        	
		FileWriter writer = new FileWriter("README.tmp");
		writer.write(sb.toString());
		writer.flush();
		writer.close();
	}
}
