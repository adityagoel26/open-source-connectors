// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import java.io.IOException;

import org.bson.types.ObjectId;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.mongodb.actions.RetryableGetOperation;
import com.boomi.connector.mongodb.constants.DataTypes;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.boomi.connector.mongodb.util.ProfileUtils;
import com.boomi.connector.util.BaseGetOperation;
import com.boomi.util.StringUtil;
/**
 * The Class MongoDBConnectorGetOperation.
 * 
 */
public class MongoDBConnectorGetOperation extends BaseGetOperation {
	
	/** The record schema for get. */
	private String recordSchemaForGet = null;

	/**
	 * Instantiates a new mongo DB connector for GET operation.
	 *
	 * @param conn the conn
	 */
	protected MongoDBConnectorGetOperation(MongoDBConnectorConnection conn) {
		super(conn);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.boomi.connector.util.BaseGetOperation#executeGet(com.boomi.connector.api.
	 * GetRequest, com.boomi.connector.api.OperationResponse)
	 */
	@Override
	protected void executeGet(GetRequest request, OperationResponse response) {
		String objectTypeId = this.getContext().getObjectTypeId();
		ObjectIdData id = request.getObjectId();
		MongoDBConnectorConnection mdbConnection = getConnection();
		String sObjId = id.getObjectId();
		ProfileUtils profileUtils = null;
		String dataType =this.getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
		ObjectId objId = null;
		String objectIdType = null;
		try {
			if (dataType!=null && dataType.equals(DataTypes.NONELKSTRENULL))
			{
				profileUtils = new ProfileUtils(getRecordSchemaForGet());
				objectIdType = (profileUtils.getProfile() != null) ? profileUtils.getType(MongoDBConstants.ID_FIELD_NAME) : null;
				RetryableGetOperation getWithRetry = new RetryableGetOperation(mdbConnection,request, sObjId, objectTypeId, response, id, null);
				getWithRetry.execute();
			}
			else if (dataType==null || dataType.contains("$oid"))
			{
				profileUtils = new ProfileUtils(getRecordSchemaForGet());
				objectIdType = (profileUtils.getProfile() != null) ? profileUtils.getType(MongoDBConstants.ID_FIELD_NAME) : null;
				if(ObjectId.isValid(sObjId)) {
					objId = getObjectId(sObjId);
					RetryableGetOperation getWithRetry = new RetryableGetOperation(mdbConnection,request, objId.toHexString(), objectTypeId, response, id, null);
					getWithRetry.execute();	
				}else{
					RetryableGetOperation getWithRetry = new RetryableGetOperation(mdbConnection,request, sObjId, objectTypeId, response, id, null);
					getWithRetry.execute();	
				}
			
			}
			else {
				objectIdType = extractedSplitSchema(request, response, objectTypeId, id, mdbConnection, sObjId,
						dataType);
			}
		}catch (ConnectorException | IOException | MongoDBConnectException e) {
			mdbConnection.updateOperationResponseforGet(request, response, mdbConnection.processErrorForGet(e), null, sObjId, id, objectIdType);
		}catch(Exception e){
			ResponseUtil.addExceptionFailure(response, id, e);
		}finally {
			mdbConnection.closeConnection();
		}
	}

	private String extractedSplitSchema(GetRequest request, OperationResponse response, String objectTypeId,
			ObjectIdData id, MongoDBConnectorConnection mdbConnection, String sObjId, String dataType)
			throws IOException, MongoDBConnectException {
		ProfileUtils profileUtils;
		String objectIdType;
		String[] dataTypearray = dataType.split(MongoDBConstants.COOKIE);
		String type = null;
		if(dataTypearray[0]!=null && !"null".equalsIgnoreCase(dataTypearray[0]))
		{
			type = dataTypearray[0];
		}
		String profile = null;
		if(dataTypearray[1]!=null && !"null".equalsIgnoreCase(dataTypearray[1]))
		{
			profile = dataTypearray[1];
		}
		profileUtils = new ProfileUtils(profile);
		objectIdType = (profileUtils.getProfile() != null) ? profileUtils.getType(MongoDBConstants.ID_FIELD_NAME) : null;
		RetryableGetOperation getWithRetry = new RetryableGetOperation(mdbConnection,request, sObjId, objectTypeId, response, id, type);
		getWithRetry.execute();
		return objectIdType;
	}
	
	/**
	 * Gets the record schema for get.
	 *
	 * @return the record schema for get
	 */
	
	public String getRecordSchemaForGet() {
		if (StringUtil.isBlank(recordSchemaForGet)) {
			recordSchemaForGet = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
			if(recordSchemaForGet != null) {
				String[] dataTypearray = recordSchemaForGet.split(MongoDBConstants.COOKIE);
				if(dataTypearray.length>1) {
					String profile = null;
					if(dataTypearray[1]!=null && !"null".equalsIgnoreCase(dataTypearray[1]))
					{
						profile = dataTypearray[1];
					}
					recordSchemaForGet = profile;
				}
			}
		}
		return recordSchemaForGet;
	}
	
	/**
	 * Creates the ObjectId for the given id.
	 * @param id
	 * @return ObjectId
	 */
	private ObjectId getObjectId(String id) {
		try {
			return new ObjectId(id);
		}catch(Exception e) {
			throw new ConnectorException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.util.BaseOperation#getConnection()
	 */
	@Override
	public MongoDBConnectorConnection getConnection() {
		return (MongoDBConnectorConnection) super.getConnection();
	}
}