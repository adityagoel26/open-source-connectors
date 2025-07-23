// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sap;

import static com.boomi.connector.sap.util.SAPConstants.PREPEND_UNDERSCORE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.FieldSpecField;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.sap.util.Action;
import com.boomi.connector.sap.util.BuildJsonSchema;
import com.boomi.connector.sap.util.SAPConstants;
import com.boomi.connector.sap.util.SAPUtils;
import com.boomi.connector.util.BaseBrowser;

/**
 * @author kishore.pulluru
 *
 */
public class SAPConnectorBrowser extends BaseBrowser {

	private static final Logger logger = Logger.getLogger(SAPConnectorBrowser.class.getName());

	protected SAPConnectorBrowser(SAPConnectorConnection conn) {
		super(conn);
	}

	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {

		logger.log(Level.INFO, "ObjectTypeId length : {0}", objectTypeId.length());
		logger.log(Level.INFO, "ObjTypeId : {0} ", objectTypeId);
		SAPConnectorConnection con = getConnection();
		String sUrl = (String) getContext().getOperationProperties().get(SAPConstants.URL);
		BrowseContext browseContext = getContext();
		logger.log(Level.INFO, "URL : {0}", sUrl);
		if (StringUtils.isBlank(sUrl)) {
			throw new ConnectorException(SAPConstants.URL_ERROR_MSG);
		}

		String swaggerDocName = SAPUtils.getSwaggerDocumentName(sUrl);

		String optype = browseContext.getOperationType().toString();
		if (optype.equals(Action.EXECUTE)) {
			optype = browseContext.getCustomOperationType();
		}

		String swaggerDoc = SAPUtils.getSwaggerDocument(swaggerDocName, con.getBusinessHubUserName(),
				con.getBusinessHubPassword());
		BuildJsonSchema buildJsonSchema = new BuildJsonSchema(swaggerDoc);

		ObjectDefinitions objdefs = new ObjectDefinitions();
		String jsonSchema = null;

		for (ObjectDefinitionRole role : roles) {
			ObjectDefinition objdef = new ObjectDefinition();

			switch (role) {
			case INPUT:
				jsonSchema = buildJsonSchema.getJsonSchema(objectTypeId, optype, role.toString()).get(0);
				jsonSchema = buildJsonSchema.replaceArrayTypes(jsonSchema);
				logger.log(Level.INFO, "Json Schema in browser : {0}", jsonSchema);
				objdef.setElementName("");// addressing json pointer exception
				objdef.setJsonSchema(jsonSchema);
				objdef.setOutputType(ContentType.JSON);
				objdef.setInputType(ContentType.JSON);
				objdefs.getDefinitions().add(objdef);
				break;
			case OUTPUT:
				if (optype.equals(Action.QUERY) || optype.equals(Action.GET_WITH_PARAMS) || optype.equals(Action.CREATE)) {
					jsonSchema = buildJsonSchema.getJsonSchema(objectTypeId, optype, role.toString()).get(0);
					jsonSchema = buildJsonSchema.replaceArrayTypes(jsonSchema);
					logger.log(Level.INFO, "Json Schema in browser : {0}", jsonSchema);
					objdef.setElementName("");// addressing json pointer exception
					objdef.setJsonSchema(jsonSchema);
					objdef.setOutputType(ContentType.JSON);
					objdef.setInputType(ContentType.JSON);

					if (optype.equals(Action.QUERY)) {
						List<String> selectableFields = buildJsonSchema.getSelectableFields(objectTypeId,
								buildJsonSchema.getMethodName(optype));
						List<FieldSpecField> fsfList = new ArrayList<>();
						for (String field : selectableFields) {
							fsfList.add(new FieldSpecField().withName(PREPEND_UNDERSCORE + field).withSelectable(true)
									.withFilterable(false).withSortable(false));
						}
						objdef.getFieldSpecFields().clear();
						objdef.getFieldSpecFields().addAll(fsfList);
					}

					objdefs.getDefinitions().add(objdef);
				}
				break;
			default:
				break;
			}
		}

		return objdefs;
	}

	@Override
	public ObjectTypes getObjectTypes() {
		Set<String> sPaths = null;
		SAPConnectorConnection con = getConnection();
		String sUrl = (String) getContext().getOperationProperties().get(SAPConstants.URL);
		BrowseContext browseContext = getContext();
		logger.log(Level.INFO, "URL : {0}", sUrl);
		if (StringUtils.isBlank(sUrl)) {
			throw new ConnectorException(SAPConstants.URL_ERROR_MSG);
		}
		String swaggerDocName = SAPUtils.getSwaggerDocumentName(sUrl);

		String optype = browseContext.getOperationType().toString();
		if (optype.equals(Action.EXECUTE)) {
			optype = browseContext.getCustomOperationType();
		}

		String swaggerDoc = SAPUtils.getSwaggerDocument(swaggerDocName, con.getBusinessHubUserName(),
				con.getBusinessHubPassword());
		BuildJsonSchema buildJsonSchema = new BuildJsonSchema(swaggerDoc);

		sPaths = buildJsonSchema.getPaths(optype);
		
		

		if (sPaths.isEmpty()) {
			throw new ConnectorException(SAPConstants.NO_SUPPORTED_API_MSG);
		}
		ObjectTypes objtypes = new ObjectTypes();
		List<ObjectType> objTypeList = new ArrayList<>();
		for (String path : sPaths) {
			logger.log(Level.INFO, " URL Length : {0}, URL : {1}", new String[] { path.length() + "", path });
			ObjectType objtype = new ObjectType();
			objtype.setId(path);
			objtype.setLabel(BuildJsonSchema.getAbridgedUrl(path));
			objTypeList.add(objtype);
		}

		logger.log(Level.INFO, "ObjectTypeList size : {0}", objTypeList.size());

		objtypes.getTypes().addAll(objTypeList);

		return objtypes;
	}

	@Override
	public SAPConnectorConnection getConnection() {
		return (SAPConnectorConnection) super.getConnection();
	}

}