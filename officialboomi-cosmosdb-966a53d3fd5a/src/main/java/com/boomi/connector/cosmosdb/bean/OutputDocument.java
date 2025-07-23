//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.boomi.connector.api.OperationStatus;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author abhijit.d.mishra
 **/
@JsonInclude(JsonInclude.Include.NON_NULL)
//@JsonPropertyOrder({ "status","objectId", "errorDetails" })
public class OutputDocument implements Serializable {
	
	private static final  long serialVersionUID = 1112522816441941195L;

	/** The errorDetails property. */
	@JsonProperty("errorDetails")
	private ErrorDetails errorDetails;
	
	/** The additionalProperties property. */
	@JsonIgnore
	private Map<String, Object> additionalProperties = new HashMap<>();

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
	 * @param status the status
	 * @param errorDetails  the error details
	 * @param id the id
	 */
	public OutputDocument(OperationStatus status, String id, ErrorDetails errorDetails) {
		super();
		if (status != OperationStatus.SUCCESS) {
			this.errorDetails = errorDetails;
		}
		Object value = null;
		if (!StringUtil.isEmpty(id)) {
			value = id;
		}
		this.additionalProperties.put("id", value);
	}

	/**
	 * Gets the errorDetails
	 * @return errorDetails
	 */
	@JsonProperty("errorDetails")
	public ErrorDetails getErrorDetails() {
		return errorDetails;
	}

	/**
	 * Sets the errorDetails
	 * @param errorDetails
	 */
	@JsonProperty("errorDetails")
	public void setErrorDetails(ErrorDetails errorDetails) {
		this.errorDetails = errorDetails;
	}

	/**
	 * Gets the additionalProperties
	 * @return additionalProperties
	 */
	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	/**
	 * Sets the addtional Properties
	 * @param name
	 * @param value
	 */
	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

}