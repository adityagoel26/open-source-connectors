// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.bean;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bson.types.ObjectId;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "status", "objectId", "errorDetails" })
public class OutputDocument implements Serializable {

	/**
	 *
	 * (Required)
	 *
	 */
	@JsonProperty("errorDetails")
	private ErrorDetails errorDetails;
	@JsonIgnore
	private transient Map<String, Object> additionalProperties = new LinkedHashMap<>();
	private static final long serialVersionUID = 1112522816441941195L;

	/**
	 * Instantiates a new output document.
	 * @param status
	 * @param errorDetails
	 */
	public OutputDocument(OperationStatus status, ErrorDetails errorDetails) {
		super();
		if (status != OperationStatus.SUCCESS) {
			this.errorDetails = errorDetails;
		}
	}


	/**
	 * Instantiates a new output document.
	 *
	 * @param status       the status
	 * @param inputDetails the input details
	 */
	public OutputDocument(OperationStatus status, TrackedDataWrapper inputDetails) {
		super();
		if (status != OperationStatus.SUCCESS) {
			errorDetails = inputDetails.getErrorDetails();
		} else {
			Object value = null;
			if (null != inputDetails.getDoc()) {
				value = inputDetails.getDoc().get(MongoDBConstants.ID_FIELD_NAME);
			} else {
				value = inputDetails.getObjectId();
			}
			
			if (value != null && ObjectId.isValid(value.toString())){
                this.additionalProperties.put(MongoDBConstants.ID_FIELD_NAME, new ObjectId(value.toString()).toHexString());
            } else {
                this.additionalProperties.put(MongoDBConstants.ID_FIELD_NAME, value);
            }
		}
	}

	/**
	 * @return ErrorDetails
	 */
	@JsonProperty("errorDetails")
	public ErrorDetails getErrorDetails() {
		return errorDetails;
	}

	/**
	 * This method sets the error details
	 * @param errorDetails
	 */
	@JsonProperty("errorDetails")
	public void setErrorDetails(ErrorDetails errorDetails) {
		this.errorDetails = errorDetails;
	}

	/**
	 * This method used to get additionalProperties
	 * @return additionalProperties
	 */
	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	/**
	 * This method used to set additionalProperties
	 * @param name
	 * @param value
	 */
	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

}