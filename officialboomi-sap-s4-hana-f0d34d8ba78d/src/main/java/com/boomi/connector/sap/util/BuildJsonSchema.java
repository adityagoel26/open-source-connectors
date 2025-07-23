// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sap.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.models.HttpMethod;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Response;
import io.swagger.models.Swagger;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.models.properties.Property;
import io.swagger.parser.SwaggerParser;
import io.swagger.util.Json;

/**
 * @author kishore.pulluru
 *
 */
public class BuildJsonSchema {

	private static final String JSON_DRAFT4_DEFINITION = "\n\"$schema\": \"http://json-schema.org/draft-04/schema#\",";
	private static final String SWAGGER_DOC_ERR_PATTERN = "Please wait";
	private static final String INVALID_CREDENTIALS_HUB_ERR_MSG = "Please provide vadid SAP API Business Hub credentials.";
	private static final String SWAGGER_PARSER_ERROR = "Error while parsing the swagger document.";
	private static final String SCHEMA_PROPERTY_TAG = "\"properties\" : {";
	private static final String ERROR_PARSING_PROFILE_MSG = "Error while parsing the request/response profile. ";
	private static final String DEFINITIONS_NOT_FOUND_MSG = "Following Definition's not found in swagger document To build schema: ";
	private static final String DEFINITIONS_GETTING_ERROR_MSG = "Exception while getting definitions from swagger document.";
	private static final String DEFINITIONS_KEY = "definitions";
	private static final String NO_PARAM_ERR_MSG = "No input parameters to generate Request Profile";
	private static final String GET_WITH_PARAM_SCHEMA_ERR_MSG = "Exception while generating request schema for Get with Path Params.";
	private static final String NO_SCHEMA_FOUND_MSG = "No Schemas found in swagger document to build the profile.";
	private static final String URL_PATTERN = "\\{+[a-z A-Z]+\\}";
	private static final String URL_REPLACE_PATTERN = "";
	private static final String PROVIDE_VALID_PARAMS_MSG = "Please provide input for mandatory path parameters";

	private static final String EMPTY_SCHEMA = "{\r\n"
			+ "  \"$schema\" : \"http://json-schema.org/draft-04/schema#\",\r\n" + "  \"type\" : \"object\",\r\n"
			+ "  \"title\" : \"EmptySchema\"\r\n" + "}";

	private static final String BOOMI_ANNOTATIONS = "\"annotations\": {\r\n"
			+ "    \"boomi_fieldSpec\": { \"ignoreForFilters\": false, \"ignoreForSort\": false, \"ignoreForSelection\": true }\r\n"
			+ "  },";

	private static final Logger logger = Logger.getLogger(BuildJsonSchema.class.getName());

	private String swaggerDocument;

	public BuildJsonSchema(String swaggerDocument) {
		this.swaggerDocument = swaggerDocument;
	}

	/**
	 * This method will find and replace multiple object types with single type as
	 * boomi supporting only signle type.
	 * 
	 * @param schema
	 * @return schema
	 */
	public String replaceArrayTypes(String schema) {
		schema = schema.replace("[ \"string\", \"null\" ]", "\"string\"")
				.replace("[ \"boolean\", \"null\" ]", "\"boolean\"").replace("[ \"integer\", \"null\" ]", "\"integer\"")
				.replace("[ \"number\", \"string\", \"null\" ]", "\"number\"")
				.replace("[ \"number\", \"string\" ]", "\"number\"");
		return schema;
	}

	/**
	 * This method will build the json schema list.
	 * 
	 * @param apiPath
	 * @param sMethod
	 * @param role
	 * @return schemaList
	 */
	public List<String> getJsonSchema(String apiPath, String sMethod, String role) {
		logger.log(Level.INFO, "APIPath : {0}, Operation Type : {1}, Role : {2}",
				new String[] { apiPath, sMethod, role });
		String sActualMethod = sMethod;
		sMethod = getMethodName(sMethod);

		List<String> schemaList = new ArrayList<>();
		String sFileContent = getSwaggerDocument();
		Swagger swagger = parseSwaggerDocument(sFileContent);
		Path path = swagger.getPaths().get(apiPath);
		List<String> schemas = new ArrayList<>();
		if (role.equals(Action.INPUT)) {
			if (sActualMethod.equals(Action.GET_WITH_PARAMS) || sActualMethod.equals(Action.DELETE_WITH_PARAMS)) {
				schemas.add(buildRequestSchemaFromParameters(path, Action.PATH_PARAM, sMethod));
			} else {
				schemas = getRequestSchema(path, sMethod);
			}

		} else {
			schemas = getResponseSchemas(path, sMethod);
		}

		Map<String, String> definitions = getDefinitions();

		if (!schemas.isEmpty() && null != schemas.get(0) && !schemas.get(0).equals("null")) {
			Set<String> refSet = new HashSet<>();
			HashMap<String, Integer> map = new HashMap<>();
			String schema = getJsonSchema(schemas.get(0), definitions, refSet, map, sActualMethod);
			if ((sActualMethod.equals(Action.QUERY) || (sMethod.equals(Action.POST) && role.equals(Action.INPUT))
					|| (sMethod.equals(Action.PATCH) && role.equals(Action.INPUT))) && isPathParamsExists(apiPath)) {
				schema = addPathParamsToRequestSchema(schema, getPathParamSchema(path, sMethod));

			}
			schemaList.add(schema);
		} else if (sMethod.equals(Action.POST)) {
			// if no request schema for post then building request schema from query
			// parameters.
			schemaList.add(buildRequestSchemaFromParameters(path, Action.QUERY_PARAM, sMethod));
		} else {
			schemaList.add(EMPTY_SCHEMA);
			logger.info(NO_SCHEMA_FOUND_MSG);
			logger.info("Adding empty schema.");
		}
		logger.log(Level.INFO, "SchemaList size in getJsonSchema() : {0}", schemaList.size());
		return schemaList;
	}

	/**
	 * This method will return all available paths/API's available for given
	 * HttpMethod.
	 * 
	 * @param httpMethod
	 * @return pathList
	 */
	public Set<String> getPaths(String httpMethod) {

		String sActualOperation = httpMethod;
		httpMethod = getMethodName(httpMethod);

		String sFileContent = getSwaggerDocument();
		Swagger swagger = parseSwaggerDocument(sFileContent);
		Map<String, Path> mPaths = swagger.getPaths();
		Set<String> lPaths = new TreeSet<>();
		for (Map.Entry<String, Path> p : mPaths.entrySet()) {
			Path path = p.getValue();

			Map<HttpMethod, Operation> operations = path.getOperationMap();
			for (Entry<HttpMethod, Operation> o : operations.entrySet()) {
				if (httpMethod.equalsIgnoreCase(o.getKey().toString())) {
					if (sActualOperation.equals(Action.QUERY)) {
						if ((!isPathParamsExists(p.getKey())) || (isQueryFilterExists(path, httpMethod))) {
							lPaths.add(p.getKey());
						}
					} else if (sActualOperation.equals(Action.GET_WITH_PARAMS)) {
						if (isPathParamsExists(p.getKey()) && !isQueryFilterExists(path, httpMethod)) {
							lPaths.add(p.getKey());
						}
					} else {
						lPaths.add(p.getKey());
					}
				}
			}
		}
		return lPaths;
	}

	/**
	 * This method will return REST supported HTTP method names from boomi supported
	 * operations.
	 * 
	 * @param httpMethod
	 * @return HTTP Method name.
	 */
	public String getMethodName(String httpMethod) {
		if (httpMethod.equals(Action.QUERY) || httpMethod.equals(Action.GET_WITH_PARAMS)) {
			httpMethod = Action.GET;
		} else if (httpMethod.equals(Action.CREATE)) {
			httpMethod = Action.POST;
		} else if (httpMethod.equals(Action.UPDATE)) {
			httpMethod = Action.PATCH;
		} else if (httpMethod.equals(Action.DELETE_WITH_PARAMS)) {
			httpMethod = Action.DELETE;
		}
		return httpMethod;
	}

	/**
	 * This method will check weather path parameters exists in the URL or not.
	 * 
	 * @param path
	 * @return true if path parameters exists in the URL else return false.
	 */
	public static boolean isPathParamsExists(String path) {
		return path.contains("(");
	}

	/**
	 * This method will return available response schemas for the given HTTP method
	 * and path/API.
	 * 
	 * @param path
	 * @param method
	 * @return schemaList
	 */
	public List<String> getResponseSchemas(Path path, String method) {
		logger.log(Level.INFO, "Path : {0}, Method : {1}", new String[] { path.toString(), method });
		List<String> schemaList = new ArrayList<>();
		Map<HttpMethod, Operation> operations = path.getOperationMap();
		for (Entry<HttpMethod, Operation> entryset : operations.entrySet()) {
			HttpMethod httpMethod = entryset.getKey();
			Map<String, Response> responseMap = operations.get(httpMethod).getResponses();
			for (Entry<String, Response> responseSet : responseMap.entrySet()) {
				String resCode = responseSet.getKey();
				if (httpMethod.toString().equals(method)) {
					schemaList.add(Json.pretty(responseMap.get(resCode).getSchema()));
				}

			}
		}
		logger.log(Level.INFO, "schema list size : {0}", schemaList.size());
		return schemaList;
	}

	/**
	 * This method will return available request schemas for the given HTTP method
	 * and path/API.
	 * 
	 * @param path
	 * @param method
	 * @return schemaList
	 */
	public List<String> getRequestSchema(Path path, String method) {
		logger.log(Level.INFO, "Path : {0}, Method : {1}", new String[] { path.toString(), method });
		List<String> schemaList = new ArrayList<>();
		Map<HttpMethod, Operation> operations = path.getOperationMap();

		for (Entry<HttpMethod, Operation> entryset : operations.entrySet()) {
			HttpMethod httpMethod = entryset.getKey();
			if (httpMethod.toString().equals(method)) {
				List<Parameter> params = operations.get(httpMethod).getParameters();
				for (Parameter p : params) {
					if (p instanceof BodyParameter) {
						BodyParameter bp = (BodyParameter) p;
						schemaList.add(Json.pretty(bp.getSchema()));
					}
				}
			}
		}
		logger.log(Level.INFO, "schema list size : {0}", schemaList.size());
		return schemaList;
	}

	/**
	 * This method will fetch available path parameters from given path and
	 * operation.
	 * 
	 * @param path
	 * @param operation
	 * @return PathParameterList
	 */
	public List<String> getPathParameters(Path path, String operation) {
		Map<HttpMethod, Operation> operations = path.getOperationMap();

		List<String> parameterList = new ArrayList<>();
		for (Entry<HttpMethod, Operation> entryset : operations.entrySet()) {
			HttpMethod httpMethod = entryset.getKey();
			if (httpMethod.toString().equals(operation)) {
				List<Parameter> params = operations.get(httpMethod).getParameters();
				for (Parameter p : params) {
					if (p instanceof PathParameter) {
						PathParameter pathParam = (PathParameter) p;
						parameterList.add(pathParam.getName());
					}
				}
			}
		}

		return parameterList;
	}

	/**
	 * This is a helper method to build the json schema from available path
	 * parameters.
	 * 
	 * @param path
	 * @param sMethod
	 * @return schema
	 */
	public String getPathParamSchema(Path path, String sMethod) {
		StringBuilder sb = new StringBuilder();
		List<String> paramList = getPathParameters(path, sMethod);
		for (String param : paramList) {
			sb.append("\"").append(Action.PATH_PARAM).append(param).append("\"").append(":{ \"type\" : \"string\"},");
		}
		return sb.toString();

	}

	/**
	 * This method is to add the path parameters to the existing request schema.
	 * 
	 * @param requestSchema
	 * @param pathParamSchema
	 * @return schema
	 */
	public String addPathParamsToRequestSchema(String requestSchema, String pathParamSchema) {
		String finalSchema = null;
		StringBuilder sb = new StringBuilder(requestSchema);
		sb.insert(sb.indexOf(SCHEMA_PROPERTY_TAG) + SCHEMA_PROPERTY_TAG.length(), pathParamSchema);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = null;
		try {
			rootNode = mapper.readTree(sb.toString());
			if (rootNode != null) {
				finalSchema = Json.pretty(rootNode);
			}
		} catch (IOException e) {
			throw new ConnectorException(ERROR_PARSING_PROFILE_MSG + e.getMessage());
		}
		return finalSchema;
	}

	/**
	 * This method checks query filters in the given path/API and operation.
	 * 
	 * @param path
	 * @param operation
	 * @return true if query filters exits.
	 */
	public boolean isQueryFilterExists(Path path, String operation) {
		boolean isQueryFilterExists = false;
		Map<HttpMethod, Operation> operations = path.getOperationMap();

		for (Entry<HttpMethod, Operation> entryset : operations.entrySet()) {
			HttpMethod httpMethod = entryset.getKey();
			if (httpMethod.toString().equals(operation)) {
				List<Parameter> params = operations.get(httpMethod).getParameters();
				for (Parameter p : params) {
					if (p instanceof QueryParameter) {
						QueryParameter queryParam = (QueryParameter) p;
						if (queryParam.getName().equals(SAPConstants.FILTER)) {
							isQueryFilterExists = true;
							break;
						}
					}
				}
			}
		}
		return isQueryFilterExists;
	}

	/**
	 * This method will read the available select fields for the given api path for
	 * the query operation from the swagger document.
	 * 
	 * @param apiPath
	 * @param operation
	 * @return list of fields which are allowed for select in the filter selection
	 *         of query operation.
	 */
	public List<String> getSelectableFields(String apiPath, String operation) {
		List<String> selectableFields = new ArrayList<>();

		String sFileContent = getSwaggerDocument();
		Swagger swagger = parseSwaggerDocument(sFileContent);
		Path path = swagger.getPaths().get(apiPath);

		Map<HttpMethod, Operation> operations = path.getOperationMap();
		for (Entry<HttpMethod, Operation> entryset : operations.entrySet()) {
			HttpMethod httpMethod = entryset.getKey();
			if (httpMethod.toString().equals(operation)) {
				List<Parameter> params = operations.get(httpMethod).getParameters();
				for (Parameter p : params) {
					if (p instanceof QueryParameter) {
						QueryParameter queryParam = (QueryParameter) p;
						if (queryParam.getName().equals(SAPConstants.SELECT)) {
							selectableFields = queryParam.getEnum();
							Property sp = queryParam.getItems();
							ObjectMapper obj = new ObjectMapper();
							try {
								String json = obj.writeValueAsString(sp);
								JsonNode rootNode = obj.readTree(json);
								Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
								while (fieldsIterator.hasNext()) {

									Map.Entry<String, JsonNode> field = fieldsIterator.next();
									if (field.getKey().equalsIgnoreCase(SAPConstants.ENUM_LABEL)) {
										String[] s = obj.readValue(field.getValue().traverse(), String[].class);
										selectableFields = Arrays.asList(s);
									}
								}

							} catch (Exception e) {
								logger.log(Level.SEVERE,
										"Exception while getting selectable fields from swagger document for query operation : {0}",
										e.getMessage());
							}

							break;
						}
					}
				}
			}
		}
		return selectableFields;
	}

	/**
	 * This method returns list of available query parameters for the given API and
	 * Operation.
	 * 
	 * @param path
	 * @param operation
	 * @return query parameter list
	 */
	public List<String> getQueryParameters(Path path, String operation) {
		Map<HttpMethod, Operation> operations = path.getOperationMap();

		List<String> parameterList = new ArrayList<>();
		for (Entry<HttpMethod, Operation> entryset : operations.entrySet()) {
			HttpMethod httpMethod = entryset.getKey();
			if (httpMethod.toString().equals(operation)) {
				List<Parameter> params = operations.get(httpMethod).getParameters();
				for (Parameter p : params) {
					if (p instanceof QueryParameter) {
						QueryParameter queryParam = (QueryParameter) p;
						parameterList.add(queryParam.getName());
					}
				}
			}
		}

		return parameterList;
	}

	/**
	 * This method will build the request schema from the parameters that are given.
	 * 
	 * @param path
	 * @param paramType
	 * @param operation
	 * @return schema
	 */
	public String buildRequestSchemaFromParameters(Path path, String paramType, String operation) {

		String jsonSchema = null;
		List<String> parameterList = null;
		if (paramType.equals(Action.PATH_PARAM)) {
			parameterList = getPathParameters(path, operation);
		} else if (paramType.equals(Action.QUERY_PARAM)) {
			parameterList = getQueryParameters(path, operation);
		}

		if (parameterList != null && !parameterList.isEmpty()) {
			StringBuilder sbSchema = new StringBuilder();
			sbSchema.append("{");
			sbSchema.append(JSON_DRAFT4_DEFINITION);
			sbSchema.append(" \"type\": \"object\",");
			sbSchema.append(" \"properties\": {");
			for (String param : parameterList) {
				sbSchema.append("\"").append(paramType).append(param).append("\": {");
				sbSchema.append("\"type\": \"string\"");
				sbSchema.append("},");
			}
			sbSchema.deleteCharAt(sbSchema.length() - 1);
			sbSchema.append("}}");

			String json = sbSchema.toString();
			JsonFactory factory = new JsonFactory();
			ObjectMapper mapper = new ObjectMapper(factory);

			JsonNode rootNode = null;
			try {
				rootNode = mapper.readTree(json);

				if (rootNode != null) {
					jsonSchema = Json.pretty(rootNode);
				}
			} catch (Exception e) {
				logger.log(Level.SEVERE, GET_WITH_PARAM_SCHEMA_ERR_MSG);
				throw new ConnectorException(GET_WITH_PARAM_SCHEMA_ERR_MSG);
			}

		} else {
			logger.log(Level.SEVERE, NO_PARAM_ERR_MSG);
			throw new ConnectorException(NO_PARAM_ERR_MSG);
		}

		return jsonSchema;
	}

	/**
	 * This method will read and return the definitions from swagger file.
	 * 
	 * @return definitions map
	 */
	public Map<String, String> getDefinitions() {

		Map<String, String> defMap = new HashMap<>();
		try {
			String json = getSwaggerDocument();
			JsonFactory factory = new JsonFactory();

			ObjectMapper mapper = new ObjectMapper(factory);
			JsonNode rootNode = mapper.readTree(json);

			Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
			while (fieldsIterator.hasNext()) {

				Map.Entry<String, JsonNode> field = fieldsIterator.next();
				if (field.getKey().equalsIgnoreCase(DEFINITIONS_KEY)) {
					JsonNode node = mapper.readTree(field.getValue().toString());
					Iterator<Map.Entry<String, JsonNode>> fitr = node.fields();
					while (fitr.hasNext()) {
						Map.Entry<String, JsonNode> f = fitr.next();
						defMap.put(f.getKey(), Json.pretty(f.getValue()));
					}
				}
			}
		} catch (IOException e) {
			throw new ConnectorException(DEFINITIONS_GETTING_ERROR_MSG);
		}
		if (defMap.isEmpty()) {
			throw new ConnectorException("No Definitions Read from Schema");
		}

		return defMap;
	}

	/**
	 * This method will build schema by finding and replacing all the references
	 * with actual definitions recursively.
	 * 
	 * @param schema
	 * @param definitions
	 * @param refSet
	 * @param map
	 * @return schema
	 */
	public String buildSchema(String schema, Map<String, String> definitions, Set<String> refSet,
			Map<String, Integer> map) {

		StringTokenizer st = new StringTokenizer(schema, "\n");
		StringBuilder sb = new StringBuilder();
		String prevToken = "";

		while (st.hasMoreTokens()) {
			String s = st.nextToken();
			s = s.replace("\r", "");// checking fix for extra { in cloud atom.
			if (!s.contains("$ref")) {
				sb.append(s);
			} else {
				if (addToMap(s, map)) { // fixed for cyclic references\\"items" : {\r
					if (prevToken.contains("items")) {
						// removing {} and replacing [] in case of array
						sb.replace(sb.length() - 1, sb.length(), "[");
						if (definitions.get(getDefinitionKey(s)) == null) {
							throw new ConnectorException(DEFINITIONS_NOT_FOUND_MSG + s + "Definition size : "
									+ definitions.size() + "defkey : " + getDefinitionKey(s));
						} else {
							sb.append(buildSchema(definitions.get(getDefinitionKey(s)), definitions, refSet, map));
						}
						if (st.nextToken().trim().equals("},")) {
							sb.append("\n ], \n");
						} else {
							sb.append("\n ] \n");
						}
					} else {
						sb.replace(sb.length() - 1, sb.length(), "");
						if (definitions.get(getDefinitionKey(s)) == null) {
							throw new ConnectorException(DEFINITIONS_NOT_FOUND_MSG + s + "Definition size : "
									+ definitions.size() + "defkey : " + getDefinitionKey(s));
						} else {
							sb.append(buildSchema(definitions.get(getDefinitionKey(s)), definitions, refSet, map));
						}
						sb.append("\n");
						// fixed for consecutive ref case with comma  /** Example: "..."},"to_Supplier":{" **/
						if (st.nextToken().trim().equals("},")) {
							sb.append(",");
						}
					}
				} else {
					sb.append("\"type\" : \"object\"");
				}

			}
			prevToken = s;
		}
		return sb.toString();

	}

	/**
	 * @param s
	 * @param map
	 * @return true if the reference is not repeated.
	 */
	public boolean addToMap(String s, Map<String, Integer> map) {
		boolean isValid = false;
		if (map.containsKey(s)) {
			int count = map.get(s);
			if (count > 2) {
				isValid = false;
			} else {
				map.remove(s);
				map.put(s, count + 1);
				isValid = true;
			}
		} else {
			map.put(s, 1);
			isValid = true;
		}
		return isValid;
	}

	/**
	 * Returns definition key from the given reference String.
	 * 
	 * @param ref
	 * @return Definition Key
	 */
	public String getDefinitionKey(String ref) {
		ref = ref.replace("\r", ""); // in cloud atom \r is not present but in local atom ref is coming with ending
										// \r.
		ref = ref.substring(ref.lastIndexOf('/') + 1, ref.length() - 1);
		return ref;
	}

	/**
	 * This method will build the json schema from the definitions.
	 * 
	 * @param schema
	 * @param definitions
	 * @param references
	 * @param map
	 * @return schema
	 */
	public String getJsonSchema(String schema, Map<String, String> definitions, Set<String> references,
			Map<String, Integer> map, String sActualMehtod) {
		StringBuilder sbSchema = new StringBuilder(buildSchema(schema, definitions, references, map).trim());
		if (sActualMehtod.equals(Action.QUERY)) {
			sbSchema = mofityToMatchSplitResponse(sbSchema);
		}
		sbSchema.insert(1, JSON_DRAFT4_DEFINITION + BOOMI_ANNOTATIONS);
		JsonFactory factory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = null;
		try {
			rootNode = mapper.readTree(sbSchema.toString());
		} catch (IOException e) {
			throw new ConnectorException(
					ERROR_PARSING_PROFILE_MSG + e.getMessage() + " Schema: " + sbSchema.toString());
		}
		return Json.pretty(rootNode);
	}

	private StringBuilder mofityToMatchSplitResponse(StringBuilder sbSchema) {
		JsonFactory factory = new JsonFactory();
		JsonNode dataObject = null;
		try (JsonParser jsonParser = factory.createParser(sbSchema.toString())) {
			final JsonToken token = jsonParser.nextToken();
			if (token != JsonToken.START_OBJECT) {
				throw new IllegalStateException(
						"The first element of the Json structure was expected to be a start Object token, but it was: ");
			}
			while (token != JsonToken.END_OBJECT) {
				JsonToken nextToken = jsonParser.nextToken();
				if (nextToken == JsonToken.FIELD_NAME && jsonParser.getText().equals("items")) {
					jsonParser.nextToken();
					JsonToken objectToken = jsonParser.nextToken();
					if (objectToken == JsonToken.START_OBJECT) {
						dataObject = new ObjectMapper().readTree(jsonParser);
						jsonParser.skipChildren();
						break;
					}
				}
			}

		} catch (Exception e) {
			logger.log(Level.WARNING, "Exception at mofityToMatchSplitResponse method : {0}", e.getMessage());
		}

		return new StringBuilder(Json.pretty(dataObject));
	}

	/**
	 * @return Swagger Document
	 */
	public String getSwaggerDocument() {
		return this.swaggerDocument;
	}

	/**
	 * This method will parse the swagger document.
	 * 
	 * @param sFileContent
	 * @return Swagger Object
	 */
	public Swagger parseSwaggerDocument(String sFileContent) {
		Swagger swagger = null;
		try {
			swagger = new SwaggerParser().parse(sFileContent);
			if (swagger == null && sFileContent.contains(SWAGGER_DOC_ERR_PATTERN)) {
				throw new ConnectorException(INVALID_CREDENTIALS_HUB_ERR_MSG);
			}
		} catch (Exception e) {
			if (sFileContent.contains(SWAGGER_DOC_ERR_PATTERN)) {
				throw new ConnectorException(INVALID_CREDENTIALS_HUB_ERR_MSG);
			} else {
				logger.log(Level.SEVERE, SWAGGER_PARSER_ERROR+e.getMessage());
				throw new ConnectorException(SWAGGER_PARSER_ERROR+" : "+sFileContent);
			}
		}
		if(swagger == null) {
			throw new ConnectorException("Currently OpenAPI specification is not supported by the connector.");
		}
		return swagger;
	}

	/**
	 * This method will remove actual placeholder in URL to reduce the size of the
	 * URL.
	 * 
	 * @param url
	 * @return abridged URL.
	 */
	public static String getAbridgedUrl(String url) {
		// checking if URL is having more than one path parameter.
		if (url.contains("=")) {
			url = url.replaceAll(URL_PATTERN, URL_REPLACE_PATTERN).replace(',', '/').replace("'", "").replace("=", "");
			if (url.length() > 216) {
				url = url.substring(0, 216);
			}
		}
		return url;
	}

	/**
	 * This method will build the path parameter keys from the URL selected, to get
	 * the request values from the input map using these keys.
	 * 
	 * @param sUrl
	 * @return PathParamKeys
	 */
	public static List<String> getPathParamKeys(String sUrl) {
		List<String> pathParamKeyList = new ArrayList<>();
		String sAbridgedParamUrl = sUrl.substring(sUrl.indexOf('(') + 1, sUrl.indexOf(')'));
		if (sAbridgedParamUrl.contains("=")) {
			sAbridgedParamUrl = sAbridgedParamUrl.replaceAll(URL_PATTERN, URL_REPLACE_PATTERN).replace(',', '/')
					.replace("'", "").replace("=", "");
			pathParamKeyList = Arrays.asList(sAbridgedParamUrl.split("/"));
		} else {
			sAbridgedParamUrl = sAbridgedParamUrl.replace("'", "").replace("{", "").replace("}", "");
			pathParamKeyList.add(sAbridgedParamUrl);
		}
		return pathParamKeyList;
	}

	/**
	 * This method will build the URL by appending all the path parameters to the URL.
	 * @param objMap
	 * @param inputKeys
	 * @return apiUrl
	 */
	public static StringBuilder getPathParamUrlFromInput(Map<String, String> objMap, List<String> inputKeys) {
		StringBuilder params = new StringBuilder();
		if (objMap.size() > 1) {
			for (String key : inputKeys) {
				String paramValue = objMap.get(key);
				if (paramValue == null) {
					throw new ConnectorException(PROVIDE_VALID_PARAMS_MSG);
				}
				// removing single quotes for guid and datatime.
				if (paramValue.startsWith(SAPConstants.GUID) || paramValue.startsWith(SAPConstants.DATETIME)) {
					params.append(key).append("=").append(paramValue).append(",");
				} else {
					params.append(key).append("=").append("'").append(paramValue).append("'").append(",");
				}

			}
			params.deleteCharAt(params.length() - 1);
		} else if (objMap.size() == 1) {
			for (String key : inputKeys) {
				String paramValue = objMap.get(key);
				if (paramValue == null) {
					throw new ConnectorException(PROVIDE_VALID_PARAMS_MSG);
				}
				if (paramValue.contains(SAPConstants.GUID) || paramValue.contains(SAPConstants.DATETIME)) {
					params.append(paramValue);
				} else {
					params.append("'").append(paramValue).append("'");
				}
			}
		} else {
			logger.info(SAPConstants.INPUT_JSON_ERROR);
			throw new ConnectorException(SAPConstants.INPUT_JSON_ERROR);
		}
		return params;
	}

}
