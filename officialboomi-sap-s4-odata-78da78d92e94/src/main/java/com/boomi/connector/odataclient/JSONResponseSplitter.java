package com.boomi.connector.odataclient;

import java.io.IOException;
import java.io.InputStream;

import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.splitter.JsonSplitter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JSONResponseSplitter extends JsonSplitter{
    private String _skipToken;
    private String itemPath;
    private String skipTokenPath;
    StringBuilder pathPointer;
    


	//TODO we use regex to qualify a next page element but that doesn't allow for a qualifier
    //Split documents at a specific path base on the itemPathRegEx path value
    //Also capture the next page link based a simple path value
	//nextPageElementPath is optional if offset/page size pagination is used
    public JSONResponseSplitter(InputStream inputStream, String itemPath, String skipTokenPath) throws IOException {
		super(inputStream);
		this.skipTokenPath = skipTokenPath;
		this.itemPath = itemPath;
		pathPointer = new StringBuilder();
	}

    //used for next URL or hasmore pagination
    public String getSkipToken()
    {
        return _skipToken;
    }
    
	@Override
	protected JsonToken findNextNodeStart() throws IOException {
		JsonParser jsonParser = this.getParser();
    	JsonToken element=null;

		//We want to parse all the entries but grab the resource child object.
		//TODO /entry/*/resource will work but not sure about the end of line match with d+$ ... lose the $ when * in middle of string?
    		
    	element = jsonParser.nextToken();
        while (element!=null)
        {
        	if (element == JsonToken.FIELD_NAME)
        	{
        		pushElement(jsonParser.getCurrentName());
        	} 
        	else if (element == JsonToken.END_ARRAY)
        	{
        		popElement();
        		popElement();
        	}
        	
			String name = jsonParser.getCurrentName();
			if (name!=null && !StringUtil.isEmpty(skipTokenPath))
			{
	        	if (_skipToken==null)
	        	{
	        		if ((element==JsonToken.VALUE_STRING)  
	        			&& pathPointer.toString().contentEquals(skipTokenPath))
	        			_skipToken = jsonParser.getValueAsString();
	        	}             	
			}
        	if (element==JsonToken.START_OBJECT) 
        	{
        		if (pathPointer.toString().contentEquals(itemPath))
        		{
        			return element;
        		}
        	} 
        	if (element.name().startsWith(ODataConstants.VALUE) || element == JsonToken.END_OBJECT)
        	{
        		popElement();
        	}
        	if (element==JsonToken.START_ARRAY)
        	{
        		pushElement("*");
        	}
        	
        	element = jsonParser.nextToken();
		}		
        return null;
	}
	
	private void pushElement(String name)
	{
		pathPointer.append("/");
		pathPointer.append(name);
	}
	
	private void popElement()
	{
		int lastPos = pathPointer.lastIndexOf("/");
		if (lastPos>-1)
			pathPointer.setLength(lastPos);
	}
}
