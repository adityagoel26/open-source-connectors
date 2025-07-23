/*
 * Copyright @ 2021 Boomi, Inc.
 */
package com.boomi.connector.odataclient;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmAnnotations;
import org.apache.olingo.odata2.api.edm.EdmAssociationSet;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmEntityContainer;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmFunctionImport;
import org.apache.olingo.odata2.api.edm.EdmMultiplicity;
import org.apache.olingo.odata2.api.edm.EdmNavigationProperty;
import org.apache.olingo.odata2.api.edm.EdmParameter;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.json.JSONException;
import org.json.JSONObject;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;

public class ODataClientBrowser extends BaseBrowser  implements ConnectionTester {


	public enum OperationProperties {MAXBROWSEDEPTH, INCLUDE_RECURSIVE_NAVIGATIONS, DEEPCREATEMODE, CATALOG_SERVICEPATH, ALT_SERVICEPATH}
    public enum DeepCreateMode {LINK, DEEPCREATE, BATCHCREATE}


    private Logger logger = Logger.getLogger(this.getClass().getName());

	long maxBrowseDepth;
	boolean includeRecursiveNavigations;
	boolean inputProfileGetDelete;
	boolean inputProfileIncludeChildProperties;
	boolean inputProfileIncludeLinks;
	boolean outputProfile;
	
	EdmEntitySet edmEntitySet;
	List<String> visitedTypes = new ArrayList<>();
	private final PropertyMap opProps;
    public ODataClientBrowser(ODataClientConnection conn) {
        super(conn);
        logger = Logger.getLogger(this.getClass().getName());
        opProps = this.getContext().getOperationProperties();
        includeRecursiveNavigations = opProps.getBooleanProperty(OperationProperties.INCLUDE_RECURSIVE_NAVIGATIONS.name(), false);
    }

	@Override
	public ObjectTypes getObjectTypes() 
	{
		ObjectTypes objectTypes = new ObjectTypes();
		String servicePath = getServicePath();
		String customOperationType = getContext().getCustomOperationType();

		try {
			Edm edm=getEdm(servicePath);
			EdmEntityContainer edmEntityContainer = edm.getDefaultEntityContainer();
			if (getContext().getOperationType()==OperationType.EXECUTE && OperationType.EXECUTE.name().contentEquals(getContext().getCustomOperationType()))
			{
				//Function Import
				for (EdmFunctionImport functionImport : edm.getFunctionImports())
				{
					ObjectType objectType = new ObjectType();
					objectType.setHelpText(functionImport.getName());
					objectType.setId(functionImport.getName()); 
					objectType.setLabel(functionImport.getName());
					objectTypes.getTypes().add(objectType);					
				}
			} else {
				for (EdmEntitySet entitySet:edmEntityContainer.getEntitySets())
				{
					EdmEntityType entityType = entitySet.getEntityType();
					String entitySetName=entitySet.getName();
					boolean includeEntityType=true;
					String label = entitySetName;
					EdmAnnotations annotations = entityType.getAnnotations();
					List<EdmAnnotationAttribute> attributes = annotations.getAnnotationAttributes();
					String typeLabel=null;
					String typeQuickInfo=null;
					if (attributes!=null)
					{
						for (EdmAnnotationAttribute attribute : attributes)
						{
							switch (attribute.getName())
							{
							case "quickinfo":
								typeQuickInfo=attribute.getText();
								break;
							case "label":
								typeLabel=attribute.getText();
								break;
							case "updatable":
								if (ODataConstants.PATCH.contentEquals(customOperationType) && ODataConstants.FALSE.contentEquals(attribute.getText()))
									includeEntityType=false;
								break;
							case "creatable":
								if (ODataConstants.POST.contentEquals(customOperationType) && ODataConstants.FALSE.contentEquals(attribute.getText()))
									includeEntityType=false;
								break;
							case "deletable":
								if (ODataConstants.DELETE.contentEquals(customOperationType) && ODataConstants.FALSE.contentEquals(attribute.getText()))
									includeEntityType=false;
								break;
							default:
								break;
							}
						}
					}
					
					if (typeQuickInfo!=null && typeQuickInfo.length()>0)
						label += " - " + typeQuickInfo;
					else if (typeLabel!=null && typeLabel.length()>0)
						label += " - " + typeLabel;
					ObjectType objectType = new ObjectType();
					objectType.setHelpText(label);
					objectType.setId(entitySetName); 
					objectType.setLabel(label);
					if (includeEntityType)
						objectTypes.getTypes().add(objectType);
				}
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		}	
		return objectTypes;
	}
	
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId,
			Collection<ObjectDefinitionRole> roles)		
	{ 
		ObjectDefinitions objDefinitions = new ObjectDefinitions();
		OperationType operationType = getContext().getOperationType();
		String customOperationType = getContext().getCustomOperationType();
		String servicePath = getServicePath();
		objDefinitions.getOperationFields().add(createServiceField(servicePath));
		try {
			Edm edm = getEdm(servicePath);
			if (operationType==OperationType.EXECUTE && OperationType.EXECUTE.name().contentEquals(customOperationType))
			{
				//Function Import
				//TODO json input type, to be converted to query params
				EdmFunctionImport functionImport = edm.getDefaultEntityContainer().getFunctionImport(objectTypeId);
				for(ObjectDefinitionRole role : roles)
				{
					ObjectDefinition objDefinition = new ObjectDefinition();
					OperationCookie operationCookie = new OperationCookie();
					if (ObjectDefinitionRole.INPUT == role)
					{
						objDefinition.setInputType(ContentType.JSON);
						String requestSchema = this.getFunctionInputSchema(functionImport, operationCookie);
						objDefinition.setElementName("/"+functionImport.getName()); 
						objDefinition.setJsonSchema(requestSchema);
					} 					
					else if (ObjectDefinitionRole.OUTPUT == role)
					{
						objDefinition.setOutputType(ContentType.JSON);
						String responseSchema = this.getFunctionOutputSchema(edm, functionImport, operationCookie);
						objDefinition.setElementName("/"+functionImport.getReturnType().getType().getName()); 
						objDefinition.setJsonSchema(responseSchema);
					}
					objDefinitions.getDefinitions().add(objDefinition);								
					objDefinition.setCookie(operationCookie.toString());
				}
			} else {
				EdmEntityContainer edmEntityContainer = edm.getDefaultEntityContainer();
				EdmEntitySet entitySet = edmEntityContainer.getEntitySet(objectTypeId);
				if (entitySet==null)
					throw new ConnectorException("Entity type not found in the OData entity set:" + objectTypeId);

				for(ObjectDefinitionRole role : roles)
				{
					inputProfileGetDelete=false;
					inputProfileIncludeLinks=false;
					inputProfileIncludeChildProperties=false;
					outputProfile = false;
					ObjectDefinition objDefinition = new ObjectDefinition();
					OperationCookie operationCookie = new OperationCookie();
					if (ObjectDefinitionRole.INPUT == role)
					{
						objDefinition.setInputType(ContentType.JSON);
						switch (operationType)
						{
						case EXECUTE:
							switch (customOperationType)
							{
							case ODataConstants.GET:
							case ODataConstants.DELETE:
								//Both DELETE and GET require path parameters
								maxBrowseDepth=0;
								inputProfileGetDelete=true;
								break;
							case ODataConstants.PUT:
							case ODataConstants.PATCH:
								maxBrowseDepth=1;
								inputProfileIncludeLinks=true;
								break;
							case ODataConstants.POST:
								inputProfileIncludeLinks=true;
								DeepCreateMode deepCreateMode = DeepCreateMode.valueOf(opProps.getProperty(OperationProperties.DEEPCREATEMODE.name(), DeepCreateMode.LINK.name()));
						        operationCookie.setDeepCreateMode(deepCreateMode);
								if (deepCreateMode == DeepCreateMode.DEEPCREATE)
								{
									inputProfileIncludeChildProperties=true;
							        this.maxBrowseDepth = getMaxBrowseDepth(getContext().getOperationProperties());
									if (this.maxBrowseDepth < 1)
										maxBrowseDepth=1;
								} else {
									this.maxBrowseDepth=1;
								}								
								break;
							default:
								break;
							}
							objDefinition.setElementName("/"+objectTypeId); 							
							objDefinition.setJsonSchema(getEntityTypeSchema(edm, entitySet, objectTypeId, operationCookie));
							objDefinitions.getDefinitions().add(objDefinition);								
							break;
						default:
							break;
						}	
					} 
					else if (ObjectDefinitionRole.OUTPUT == role)
					{
						outputProfile = true;
						objDefinition.setOutputType(ContentType.JSON);
						switch (operationType)
						{
						case EXECUTE:
							if(ODataConstants.GET.contentEquals(customOperationType))
							{
								this.maxBrowseDepth=getMaxBrowseDepth(getContext().getOperationProperties());
							}
							else if (ODataConstants.POST.contentEquals(customOperationType))
							{
								DeepCreateMode deepCreateMode = DeepCreateMode.valueOf(opProps.getProperty(OperationProperties.DEEPCREATEMODE.name(), DeepCreateMode.LINK.name()));
								if (deepCreateMode == DeepCreateMode.DEEPCREATE)
								{
							        this.maxBrowseDepth = getMaxBrowseDepth(getContext().getOperationProperties());
								} else {
									this.inputProfileIncludeChildProperties=true;
									this.maxBrowseDepth=1;
								}								
							}
							objDefinition.setElementName("/"+objectTypeId); 							
							objDefinition.setJsonSchema(getEntityTypeSchema(edm, entitySet, objectTypeId, operationCookie));
							objDefinitions.getDefinitions().add(objDefinition);				
							break;
						case QUERY:
							//TODO do we do fieldspec fields? Or just leave them all filterable and sortable?
					        this.maxBrowseDepth = getMaxBrowseDepth(getContext().getOperationProperties());
							objDefinition.setOutputType(ContentType.JSON);
							objDefinition.setElementName("/"+objectTypeId); //specify root element as schema location
							objDefinition.setJsonSchema(getEntityTypeSchema(edm, entitySet, objectTypeId, operationCookie));
							objDefinitions.getDefinitions().add(objDefinition);								
							break;
						default:
							break;
						}
					}
					objDefinition.setCookie(operationCookie.toString());
				}
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return objDefinitions;
	}
	
	private String getFunctionInputSchema(EdmFunctionImport functionImport, OperationCookie operationCookie) throws EdmException {
		JSONObject root = new JSONObject();
		root.put(ODataConstants.SCHEMA, ODataConstants.HTTP_JSON_SCHEMA_ORG_DRAFT_04_SCHEMA);
		JSONObject schemaObj = new JSONObject();
		root.put(functionImport.getName() , schemaObj);
		schemaObj.put(ODataConstants.TYPE, ODataConstants.OBJECT);
		JSONObject properties = new JSONObject();
		schemaObj.put(ODataConstants.PROPERTIES , properties);
		for(String name : functionImport.getParameterNames())
		{
			EdmParameter parameter = functionImport.getParameter(name);
			JSONObject property = new JSONObject();
			properties.put(name, property);				
			ODataEdmType.buildSchemaType(parameter.getType().getName(), property);
			if (!parameter.getType().getName().contentEquals(ODataConstants.STRING)) //We are default to String to minimize the size of the cookie under the assumption that String is most prevalent
				operationCookie.addProperty(parameter.getType().getName(), "", parameter.getName());
		}
		operationCookie.setHttpMethod(functionImport.getHttpMethod());
		return root.toString();
	}

	private String getFunctionOutputSchema(Edm edm, EdmFunctionImport functionImport, OperationCookie operationCookie) throws EdmException {
		JSONObject root = new JSONObject();
		root.put(ODataConstants.SCHEMA, ODataConstants.HTTP_JSON_SCHEMA_ORG_DRAFT_04_SCHEMA);
		JSONObject schemaObj = new JSONObject();
		root.put(functionImport.getReturnType().getType().getName() , schemaObj);
		JSONObject properties = new JSONObject();
		
		//Collections
		if (functionImport.getReturnType().getMultiplicity()==EdmMultiplicity.MANY)
		{
			schemaObj.put(ODataConstants.TYPE, ODataConstants.ARRAY);
			JSONObject arrayItems = new JSONObject();
			schemaObj.put(ODataConstants.ITEMS, arrayItems);
			arrayItems.put(ODataConstants.TYPE, ODataConstants.OBJECT);
			arrayItems.put(ODataConstants.PROPERTIES , properties);
		} else {
			schemaObj.put(ODataConstants.TYPE, ODataConstants.OBJECT);
			schemaObj.put(ODataConstants.PROPERTIES , properties);
		}
		EdmTyped type = functionImport.getReturnType();
		
			EdmComplexType complexType = edm.getComplexType(type.getType().getNamespace(),type.getType().getName());
			for (String propertyName : complexType.getPropertyNames())
			{
				EdmTyped typed = complexType.getProperty(propertyName);
				if (typed instanceof EdmProperty)
				{
					EdmProperty propType = (EdmProperty) typed;
					JSONObject property = new JSONObject();
					properties.put(propertyName, property);				
					ODataEdmType.buildSchemaProperty(propType, property);
					if (!propType.getType().getName().contentEquals(ODataConstants.STRING)) //We are default to String to minimize the size of the cookie under the assumption that String is most prevalent
						operationCookie.addProperty(propType, "", propertyName);
				}
			}		
		return root.toString();
	}

	private String getEntityTypeSchema(Edm edm, EdmEntitySet entitySet, String objectTypeId, OperationCookie operationCookie) throws EdmException
	{
		this.edmEntitySet = entitySet;
		EdmEntityType edmEntityType = entitySet.getEntityType();
		JSONObject schema = new JSONObject();
		schema.put(ODataConstants.SCHEMA, "http://json-schema.org/schema#");
		JSONObject root = new JSONObject();
		schema.put(objectTypeId, root);
		getEntityTypeSchemaRecursive(edm, edmEntityType, root, 0, "", operationCookie, null);
		return schema.toString();
	}
	
	//<Property Name="BusinessPartner" Type="Edm.String" Nullable="false" MaxLength="10" sap:display-format="UpperCase" sap:label="Business Partner" sap:quickinfo="Business Partner Number"/>
	//<Property Name="DraftEntityCreationDateTime" Type="Edm.DateTimeOffset" Precision="7" sap:label="Draft Created On" sap:heading="" sap:quickinfo="" sap:creatable="false" sap:updatable="false"/>
	//<Property Name="WithholdingTaxExmptPercent" Type="Edm.Decimal" Precision="5" Scale="2" sap:label="Exemption rate"/>
	//TODO description=quickinfo, maxlength=maxlength, nullable
	
	/**
	 * 
	 * @param edm
	 * @param edmEntityType
	 * @param root - the json schema root into which the child properties
	 * @param depth - the depth of the recursion
	 * @param path - the path of the nodes within the recursion
	 * @param operationCookie
	 * @param parentEdmEntityType
	 * @throws EdmException
	 */
	private void getEntityTypeSchemaRecursive(Edm edm, EdmEntityType edmEntityType, JSONObject root, long depth, String path, OperationCookie operationCookie, EdmEntityType parentEdmEntityType) throws EdmException
	{
		root.put(ODataConstants.TITLE, edmEntityType.getName());
		root.put(ODataConstants.TYPE, ODataConstants.OBJECT);
		JSONObject properties = new JSONObject();
		root.put(ODataConstants.PROPERTIES, properties);
		List<String> keys = new ArrayList<>();
		
		//We need to build out a child KEYs node that contains keys for use with _metadata link creation
		//include links node only for 
		if (this.inputProfileIncludeLinks && depth>0)
		{
			JSONObject keysNode = new JSONObject();
			properties.put(ODataConstants.CHILD_KEYS_ELEMENT, keysNode);
			JSONObject keyProperties = new JSONObject();
			keysNode.put(ODataConstants.TYPE, ODataConstants.OBJECT);
			keysNode.put(ODataConstants.PROPERTIES, keyProperties);
			keysNode.put(ODataConstants.DESCRIPTION, ODataConstants.CHILD_KEYS_ELEMENT_DESCRIPTION);
			
			//TODO NG This needs to hit child keys
			for (EdmProperty edmKeyProperty : edmEntityType.getKeyProperties())
			{
				addSimplePropertyToSchema(keyProperties, edmKeyProperty, edmKeyProperty.getName(), operationCookie, path);
			}						
		}

		for (EdmProperty edmKeyProperty : edmEntityType.getKeyProperties())
		{
			operationCookie.addKey(edmKeyProperty, path, edmKeyProperty.getName());
			keys.add(edmKeyProperty.getName());
		}
		
		for (String propertyName : edmEntityType.getPropertyNames())
		{
			if (outputProfile || (keys.contains(propertyName) && inputProfileGetDelete) || (depth==0 && !this.inputProfileGetDelete) || inputProfileIncludeChildProperties)
			{
				addPropertyToSchema(properties, edmEntityType, propertyName, operationCookie, path);
			}
		}	
		
		if (depth<maxBrowseDepth)
		{
			for (String navigationPropertyName : edmEntityType.getNavigationPropertyNames())
			{
				EdmNavigationProperty type = (EdmNavigationProperty)edmEntityType.getProperty(navigationPropertyName);
				EdmAssociationSet associationSet=null;
				for (EdmAssociationSet set: edm.getDefaultEntityContainer().getAssociationSets())
				{
					if (set.getAssociation().equals(type.getRelationship()))
					{
						associationSet = set;
						break;
					}
				}
				
				if (associationSet==null)
				{
					throw new ConnectorException("Could not find association set for navigation property: " + navigationPropertyName);
				}
				
				EdmEntitySet toEntitySet= associationSet.getEnd(type.getToRole()).getEntitySet();
				EdmEntityType childEntityType = toEntitySet.getEntityType();				
				if (childEntityType==null)
					throw new ConnectorException(String.format("Child Entity not found for navigation property: %s", navigationPropertyName));

				//Don't allow references back to parentEdmEntityType
				String childEntityTypeName = childEntityType.toString();
				if (includeRecursiveNavigations || parentEdmEntityType==null || (!childEntityTypeName.contentEquals(parentEdmEntityType.toString()) && !visitedTypes.contains(childEntityTypeName)))
				{			
					visitedTypes.add(childEntityTypeName);					
					JSONObject navigationNode = new JSONObject();					
					
					EdmMultiplicity multiplicity = type.getMultiplicity();
					if (multiplicity==EdmMultiplicity.MANY)
					{
						//JSONArray
						JSONObject array = new JSONObject();
						array.put(ODataConstants.TYPE, ODataConstants.ARRAY);
						array.put(ODataConstants.ITEMS, navigationNode);
						//Output profiles put results in Arrays
						if (outputProfile)
						{
							JSONObject results = new JSONObject();
							properties.put(navigationPropertyName, results);
							results.put(ODataConstants.TYPE, ODataConstants.OBJECT);
							JSONObject arrProps = new JSONObject();
							results.put(ODataConstants.PROPERTIES, arrProps);
							arrProps.put(ODataConstants.RESULTS, array);
						} else {
							properties.put(navigationPropertyName, array);
						}
					} else {
						//JSONObject
						properties.put(navigationPropertyName, navigationNode);
					}	
					
					String newPath = path+"/"+navigationPropertyName;
					operationCookie.addNavigationProperty(childEntityTypeName, path, navigationPropertyName, multiplicity==EdmMultiplicity.MANY, toEntitySet.getName());
					getEntityTypeSchemaRecursive(edm, childEntityType, navigationNode, depth+1, newPath, operationCookie, edmEntityType);
				}
			}
		}
	}
		
	private void addSimplePropertyToSchema(JSONObject properties, EdmProperty edmProperty, String propertyName, OperationCookie operationCookie, String path) throws EdmException {
		JSONObject property = new JSONObject();
		properties.put(propertyName, property);				
		ODataEdmType.buildSchemaProperty(edmProperty, property);
		if (!edmProperty.getType().getName().contentEquals(ODataConstants.STRING)) //We are default to String to minimize the size of the cookie under the assumption that String is most prevalent
			operationCookie.addProperty(edmProperty, path, propertyName);
	}
	private void addPropertyToSchema(JSONObject properties, EdmEntityType edmEntityType, String propertyName, OperationCookie operationCookie, String path) throws EdmException {
		EdmTyped typed = edmEntityType.getProperty(propertyName);
		if (typed instanceof EdmProperty)
		{
			addSimplePropertyToSchema(properties, (EdmProperty) typed, propertyName, operationCookie, path);
		}

	}
	
	private String getServicePath()
	{
        String path = opProps.getProperty(OperationProperties.ALT_SERVICEPATH.name(), "").trim();
        if (StringUtil.isBlank(path))
    	{
    		path = opProps.getProperty(OperationProperties.CATALOG_SERVICEPATH.name(), "").trim();
    	}
        if (StringUtil.isBlank(path))
        	throw new ConnectorException("A Service URL Path is required");
        return path;
	}

	private Edm getEdm(String servicePath) throws JSONException, EntityProviderException, UnsupportedOperationException, IOException, GeneralSecurityException
	{
		Edm edm=null;
		CloseableHttpResponse response=null;
		try {
			this.getConnection().setAcceptHeader(ODataConstants.APPLICATIONXML);
			servicePath += ODataConstants.METADATA;

			response = this.getConnection().doExecute(servicePath, null, ODataConstants.GET, null);
			if (response.getStatusLine().getStatusCode()!=200)
				throw new ConnectorException(String.format("Error reading metadata %d %s", response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase()));
			edm = EntityProvider.readMetadata(response.getEntity().getContent(), false);
		} finally {
			IOUtil.closeQuietly(response);
		}
		return edm;
	}
	
	/**
	* Creates a simple field input for the Operation.
	* @return BrowseField
	*/
	private BrowseField createServiceField(String servicePath){
	   BrowseField simpleField = new BrowseField();
	   // Mandatory to set an ID
	   simpleField.setId(ODataClientQueryOperation.OperationProperties.SERVICEPATH.name());
	   // User Friendly Label, defaults to ID if not given.
	   simpleField.setLabel("Service URL Path");
	   // Mandatory to set a DataType. This Data Type can also be String, Boolean, Integer, Password
	   simpleField.setType(DataType.STRING);
	   // Optional Help Text for the String Field
	   simpleField.setHelpText("Informational - Manual edit to field will not impact the actual API used to create the Operation");
	   // Optional Default Value for String Field
	   simpleField.setDefaultValue(servicePath);
	   return simpleField;
	}
	
	/**
	 * @return Returns the maximum depth set by the user in the import operations page for resolving recursive $ref during Import/Browse
	*/
    private long getMaxBrowseDepth(PropertyMap opProps)
    {
    	long defaultDepth=0L;
    	long maxDepth=5L;
    	long depth = opProps.getLongProperty(OperationProperties.MAXBROWSEDEPTH.name(), defaultDepth);
    	if (depth>maxDepth)
    		depth=maxDepth;
    	return depth;
    }
	
	@Override
	public void testConnection() {
		try {
			this.getConnection().testConnection();
        }
        catch (Exception e) {
            throw new ConnectorException("Could not establish a connection", e);
        }
	}

	@Override
    public ODataClientConnection getConnection() {
        return (ODataClientConnection) super.getConnection();
    }
}