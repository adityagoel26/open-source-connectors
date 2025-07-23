package com.boomi.connector.odataclient;

import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.json.JSONException;
import org.json.JSONObject;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.odataclient.ODataClientBrowser.DeepCreateMode;

public class OperationCookie {
	
	private JSONObject _properties;
	private JSONObject _cookie;
	private String _httpMethod="GET"; //only used for Function Import request
	private DeepCreateMode _deepCreateMode=DeepCreateMode.LINK; //For creates with deep payloads set at import
	
	OperationCookie()
	{
		_cookie = new JSONObject();	
		_properties=new JSONObject();
		_cookie.put(ODataConstants.PROPERTIES, _properties);
	}
	
	OperationCookie(String cookieString)
	{
		if (cookieString==null || cookieString.length()==0)
			throw new ConnectorException("Operation cookie is null, please reimport the operation");		
		_cookie = new JSONObject(cookieString);	
		_properties = _cookie.getJSONObject(ODataConstants.PROPERTIES);
		if (_cookie.has(ODataConstants.HTTPMETHOD))
			_httpMethod = _cookie.getString(ODataConstants.HTTPMETHOD);
		if (_cookie.has(ODataConstants.DEEPCREATEMODE))
			_deepCreateMode = DeepCreateMode.valueOf(_cookie.getString(ODataConstants.DEEPCREATEMODE));
	}
	
	/**
	 * Returns true if the property identified by the name/path is an OData key for the entity
	 * @param key
	 * @return returns true if it is a key
	 */
	public boolean isKey(String key)
	{
		return (_properties.has(key) && _properties.getJSONObject(key).has(ODataConstants.ISKEY));
	}
	
	public String getEntitySetName(String cookieKey) {
		if (_properties.has(cookieKey))
		{
			JSONObject cookieEntry = _properties.getJSONObject(cookieKey);
			return cookieEntry.getString(ODataConstants.ENTITYSETNAME);
		}
		return null;
	}
	
	/**
	 * 
	 * @param cookieKey
	 * @return
	 */
	public String getEdmType(String cookieKey) {
		if (_properties.has(cookieKey))
		{
			JSONObject cookieEntry = _properties.getJSONObject(cookieKey);
			return cookieEntry.getString(ODataConstants.TYPE);
		}
		return "String"; //We default to string to minimize cookie size
	}
	
	/**
	 * 
	 * @param cookieKey
	 * @return
	 */
	public int getScale(String cookieKey) {
		if (_properties.has(cookieKey))
		{
			JSONObject cookieEntry = _properties.getJSONObject(cookieKey);
			return cookieEntry.getInt(ODataConstants.SCALE);
		}
		return 0; //We default to string to minimize cookie size
	}
	
	/**
	 * 
	 * @param edmTypeName
	 * @param path the path of the property within the json profile structure
	 * @param propertyName
	 * @param isArray For navigation properties indicating if it is a 1..* many multiplicity
	 * @throws EdmException
	 * @throws JSONException
	 */
	void addNavigationProperty(String edmTypeName, String path, String propertyName, boolean isArray, String entitySetName)
	{
		JSONObject cookieElement = this.createCookieElement(edmTypeName, path, propertyName);
		cookieElement.put(ODataConstants.ISNAVIGATIONPROPERTY, true);
		cookieElement.put(ODataConstants.ENTITYSETNAME, entitySetName);
		if (isArray)
			cookieElement.put(ODataConstants.ISARRAY, true);
	}
	
	/**
	 * 
	 * @param edmProperty
	 * @param path the path of the property within the json profile structure
	 * @param propertyName
	 * @throws EdmException
	 * @throws JSONException
	 */
	void addKey(EdmProperty edmProperty, String path, String propertyName) throws EdmException, JSONException
	{
		JSONObject cookieElement = this.createCookieElement(edmProperty, path, propertyName);
		cookieElement.put(ODataConstants.ISKEY, true);
	}
	
	/**
	 * 
	 * @param edmTypeName
	 * @param path the path of the property within the json profile structure
	 * @param propertyName
	 * @throws EdmException
	 * @throws JSONException
	 */
	void addProperty(String edmTypeName, String path, String propertyName) throws EdmException, JSONException
	{
		createCookieElement(edmTypeName, path, propertyName);
	}
	
	void addProperty(EdmProperty edmProperty, String path, String propertyName) throws EdmException, JSONException
	{
		createCookieElement(edmProperty, path, propertyName);
	}

	private JSONObject createCookieElement(EdmProperty edmProperty, String path, String propertyName) throws EdmException, JSONException
	{
		JSONObject cookieElement = new JSONObject();
		_properties.put(path+"/"+propertyName, cookieElement);
		cookieElement.put(ODataConstants.TYPE, edmProperty.getType().getName());
		if (edmProperty.getFacets()!=null)
		{
			if(edmProperty.getFacets().getScale()!=null)
				cookieElement.put(ODataConstants.SCALE, edmProperty.getFacets().getScale());
		}
		return cookieElement;
	}
	
	private JSONObject createCookieElement(String edmTypeName, String path, String propertyName) 
	{
		JSONObject cookieElement = new JSONObject();
		_properties.put(path+"/"+propertyName, cookieElement);
		cookieElement.put(ODataConstants.TYPE, edmTypeName);
		return cookieElement;
	}
	
	@Override
	public String toString()
	{
		return _cookie.toString();
	}

	public boolean isNavigationProperty(String key) {
		return (_properties.has(key) && _properties.getJSONObject(key).has(ODataConstants.ISNAVIGATIONPROPERTY));
	}

	public DeepCreateMode getDeepCreateMode() {
		return _deepCreateMode;
	}

	public String getHttpMethod() {
		return _httpMethod;
	}

	public void setHttpMethod(String _httpMethod) {
		_cookie.put("httpMethod", _httpMethod);
		this._httpMethod = _httpMethod;
	}

	public void setDeepCreateMode(DeepCreateMode deepCreateMode) {
		_cookie.put("deepCreateMode", deepCreateMode.name());
		this._deepCreateMode = deepCreateMode;
	}

}
