// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.util.JSONHandler;

public class SnowSQLCommands implements ISnowflakeCommand{
	//	Sonar issues: java:S5869, java:S5843
	private static final String PARAM_REGEX = "'[ ]{0,}\\$[\\w_]+[ ]{0,}'|\\$[\\w_]+|\"[ ]{0,}\\$[\\w_]+[ ]{0,}\"";
	private static final String PARAM_REPLACE_REGEX = "'[ ]{0,}\\$%s[ ]{0,}'|\\$%s|\"[ ]{0,}\\$%s[ ]{0,}\"";
	private static final String SQL_SCRIPT_PROP = "sqlScript";
	private static final String DOUBLE_QUOTES = "\"";
	private static final String SINGLE_QUOTES = "'";
	
	private String inputSqlScript;

	public SnowSQLCommands(PropertyMap operationProperties) {
		super();
		inputSqlScript = operationProperties.getProperty(SQL_SCRIPT_PROP, "");
	}
	
	private String getSQLScript(ObjectData inputDocument) {
		if(inputDocument.getDynamicProperties().getOrDefault(SQL_SCRIPT_PROP, null) != null) {
			return inputDocument.getDynamicProperties().get(SQL_SCRIPT_PROP);
		}
		return inputSqlScript;
	}
	
	private List<String> getParamNames(String sqlScript){
		List<String> paramNames = new ArrayList<>();
		Matcher matcher = Pattern.compile(PARAM_REGEX).matcher(sqlScript);
		while (matcher.find()) {
			String curParamName = matcher.group();
			int startingIndex = matcher.start();
			if(startingIndex > 0 && sqlScript.charAt(startingIndex) == '$' && sqlScript.charAt(startingIndex - 1) == '\\') {
				// escaping this parameter as it was preceded by backslash
				continue;
			}
			curParamName = curParamName.replace("'", "");
			curParamName = curParamName.replace("\"", "");
			curParamName = curParamName.replace("$", "");
			paramNames.add(curParamName);
		}
		paramNames.sort((a, b) -> b.length() - a.length());
		return paramNames;
	}
	
	@Override
	public String getSQLString(ObjectData inputDocument) {
		String sqlScript = getSQLScript(inputDocument);
		List<String> paramNames = getParamNames(sqlScript);
		sqlScript = sqlScript.replace("\\$", "$");
		SortedMap<String, String> parameterValues = JSONHandler.readDocPropertiesOrInputStream(inputDocument);
		for(String param : paramNames) {
			String value = parameterValues.getOrDefault(param, parameterValues.getOrDefault("$" + param, null));
			if(value == null) {
				throw new NoSuchElementException(param + " parameter' value cannot be found");
			}
			String curParamRegex = String.format(PARAM_REPLACE_REGEX, param, param, param);
			if(JSONHandler.isJSONValid(value)) {
				value = value.replace(SINGLE_QUOTES,DOUBLE_QUOTES);
			}
			value = SINGLE_QUOTES+value+SINGLE_QUOTES;
			sqlScript = sqlScript.replaceAll(curParamRegex, value);
		}
		return sqlScript;
	}

}
