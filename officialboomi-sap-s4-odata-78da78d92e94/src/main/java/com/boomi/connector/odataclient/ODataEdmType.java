package com.boomi.connector.odataclient;

import java.io.IOException;
import java.util.List;

import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmAnnotations;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmFacets;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.json.JSONException;
import org.json.JSONObject;

import com.boomi.connector.api.ConnectorException;

public class ODataEdmType {
	
	private ODataEdmType() {}

	static String boomiValuetoODataPredicate(String value, String edmTypeName) throws IOException {
		String predicate;
				
		switch (edmTypeName)
		{
		case "Byte":
		case "Int16":
		case "Int32":
		case "Int64":
		case "Single":
		case "Double":
		case "Decimal":
		case "Boolean":
		case "Time":
			predicate=value;
			break;
		case "DateTimeOffset":
		case "DateTime":
			predicate=edmTypeName.toLowerCase()+"'"+value+"'";
			break;
		case "Binary": //Binary means base64 encoded
		case "String":
			predicate="'"+value.replace("'", "''")+"'"; //escape single quotes
			break;
		case "Guid":
			predicate="guid'"+value+"'";
			break;
		default:
			throw new ConnectorException("Unsupported EDM Type: " + edmTypeName);
		}
		
		return predicate;
	}

	static void buildSchemaType(String edmTypeName, JSONObject property)
	{
		String typeName;
		String format=null;
		switch (edmTypeName)
		{
		case "Byte":
		case "Int16":
		case "Int32":
		case "Int64":
			typeName="integer";
			break;
		case "Single":
		case "Double":
		case "Decimal":
			typeName="number";
			break;
		case "Boolean":
			typeName="boolean";
			break;
		case "DateTimeOffset":
		case "DateTime":
			typeName=ODataConstants.string;
			format="date-time";
			break;
		case "Time":
			typeName=ODataConstants.string;;
			format="time";
			break;
		case "Binary": //Binary means base64 encoded
		case "String":
			typeName=ODataConstants.string;;
			break;
		case "Guid":
			property.put("maxLength", 36);
//			property.put("minLength", 36); //TODO minLength will force it to be required
			typeName=ODataConstants.string;;
			break;
		default:
			throw new ConnectorException("Unsupported EDM Type: " + edmTypeName);
		}
		property.put(ODataConstants.TYPE, typeName);
		if (format!=null)
			property.put(ODataConstants.FORMAT, format);
	}
	
	static void buildSchemaProperty(EdmProperty type, JSONObject property) throws EdmException
	{
		EdmAnnotations annotations = type.getAnnotations();
		List<EdmAnnotationAttribute> attributes = annotations.getAnnotationAttributes();
		
		if (attributes!=null)
		{
			for (EdmAnnotationAttribute attribute : attributes)
			{
				if ("quickinfo".equals(attribute.getName())) {
					property.put(ODataConstants.DESCRIPTION, attribute.getText());
				}
				
			}
		}
//		annotations.
		EdmFacets facets = type.getFacets();
		if (facets!=null)
		{
			//TODO Required, pattern others???
			if (facets.getMaxLength()!=null)
				property.put(ODataConstants.MAXLENGTH, facets.getMaxLength());
//			if (facets.isNullable())
		}
		String edmTypeName = type.getType().getName();
		buildSchemaType(edmTypeName, property);
	}
}
