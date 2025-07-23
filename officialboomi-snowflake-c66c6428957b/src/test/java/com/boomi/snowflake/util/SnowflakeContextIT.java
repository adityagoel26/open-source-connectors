// Copyright (c) 2021 Boomi, Inc.
package com.boomi.snowflake.util;

import java.util.List;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;
import com.boomi.snowflake.SnowflakeConnector;

/**
 * The Class SnowflakeContextIT.
 *
 * @author Vanangudi,S
 */
public class SnowflakeContextIT extends ConnectorTestContext{

	/** The Constant BATCH_SIZE_PROP. */
	private static final String BATCH_SIZE_PROP = "batchSize";
	/** The Constant STR_AWS_BUCKET_NAME. */
	private static final String STR_AWS_BUCKET_NAME = "awsBucketName";
	/** The Constant STR_AWS_REGION. */
	private static final String STR_AWS_REGION = "awsRegion";

	/**
	 * Instantiates a Snowflake Context Object
	 * */
	public SnowflakeContextIT() {
		super();
		this.addConnectionProperty("user", System.getProperty("UserName", ""));
		this.addConnectionProperty("password", System.getProperty("Password", ""));
		this.addConnectionProperty("db", "\"test_DB\"");
		this.addConnectionProperty("schema", "PUBLIC");
		this.addConnectionProperty("warehouse", "SPEC_WH");
		this.addConnectionProperty("role", "SYSADMIN");
		this.addConnectionProperty("connectionString", "jdbc:snowflake://boomi.us-east-1.snowflakecomputing.com");
		this.addConnectionProperty("awsAccessKey", System.getProperty("AccessKey", ""));
		this.addConnectionProperty("awsSecret", System.getProperty("SecretKey", ""));
		this.addConnectionProperty("maximumConnections", (long)8);
		this.addConnectionProperty("minimumConnections", (long)0);
		this.addConnectionProperty("maximumIdleTime", (long)300000);
		this.addConnectionProperty("maximumWaitTime", (long)0);
		this.addConnectionProperty("whenExhaustedAction", String.valueOf(0));
		this.addConnectionProperty("testOnBorrow", false);
		this.addConnectionProperty("testOnReturn", false);
		this.addConnectionProperty("testWhileIdle", false);
		this.addConnectionProperty("validationQuery", "");
		this.addConnectionProperty("enablePooling", true);
	}
	
	/**
	 * Instantiates a Snowflake Context Object
	 * 
	 * @param operationType
	 * 			Type of the operation executed
	 * @param customOP
	 * 			Type of the custom operation
	 * */
	public SnowflakeContextIT(OperationType operationType, String customOP) {
	        this();
	        setOperationCustomType(customOP);
	        setOperationType(operationType);
	}
	
	/**
	 * Instantiates a Snowflake Context Object
	 * 
	 * @param operationType
	 * 			Type of the operation executed
	 * @param customOP
	 * 			Type of the custom operation
	 * @param sObjectName
	 * 			Object Name
	 * */
    public SnowflakeContextIT(OperationType operationType, String customOP, String sObjectName) {
        this(operationType, customOP);
        setObjectTypeId(sObjectName);
    }
    
	/**
	 * Instantiates a Snowflake Context Object
	 * 
	 * @param operationType
	 * 			Type of the operation executed
	 * @param customOP
	 * 			Type of the custom operation
	 * @param selectedFields
	 * 			List of selected fields
	 * */
    public SnowflakeContextIT(OperationType operationType, String customOP, String sObjectName,
            List<String> selectedFields) {
    	this(operationType, customOP, sObjectName);
    	this.getSelectedFields().addAll(selectedFields);
    }
    
	/**
	 *Sets the batchSize
	 *@param batchSize Batch Size for the operation
	 */
    public void setBatchSize(long batchSize) {
    	this.addOperationProperty(BATCH_SIZE_PROP, batchSize);
    }
    
	/**
	 *Adds the S3Credrentials
	 */
    public void addS3Cred() {
		this.addOperationProperty(STR_AWS_BUCKET_NAME, "boomisnowflake");
		this.addOperationProperty(STR_AWS_REGION, "ap-south-1");
    }
    
	/**
	 *Removes the S3Credrentials
	 */
    public void removeS3Cred() {
    	this.addOperationProperty(STR_AWS_BUCKET_NAME, null);
    	this.addOperationProperty(STR_AWS_REGION, null);
    }
    
	/**
	 *Sets the InvalidBucketName
	 */
    public void setInvalidBucketName() {
    	this.addOperationProperty(STR_AWS_BUCKET_NAME, "invalid");
    }
    
	/**
	 *Sets the InternalStageName
	 */
    public void setInternalStageName() {
    	this.addOperationProperty("stageName", "\"Internal_Stage\"");
    }
    
	/**
	 *Sets the InvalidInternalStage
	 */
    public void setInvalidInternalStage() {
    	this.addOperationProperty("stageName", "invalid");
    }
    
	/**
	 *Sets the InvalidBucketRegion
	 */
    public void setInvalidBucketRegion() {
    	this.addOperationProperty(STR_AWS_REGION, "ap-south-2");
    }
    
	@Override
	protected Class<? extends Connector> getConnectorClass() {
		return SnowflakeConnector.class;
	}

}
