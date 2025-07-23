//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author abhijit.d.mishra
 **/
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "errorCode", "errorMessage" })
public class ErrorDetails implements Serializable {


	private static final long serialVersionUID = 1L;
	
	/** The id property. */
	@JsonProperty("errorCode")
	private Integer errorCode;
	/**
	 * The errorMessage property.
	 * (Required)
	 */
	@JsonProperty("errorMessage")
	private String errorMessage;
	
	/** The additionalProperties property. */
	@JsonIgnore
	private Map<String, Serializable> additionalProperties = new HashMap<>();

	/**
	 * Instantiates a new error details.
	 *
	 * @param errorCode    the error code
	 * @param errorMessage the error message
	 */
	public ErrorDetails(Integer errorCode, String errorMessage) {
		super();
		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
	}

	/**
	 * Gets the Error Code
	 * @return errorCode
	 */
	@JsonProperty("errorCode")
	public Integer getErrorCode() {
		return errorCode;
	}

	/**
	 * Sets the Error Code
	 * @param errorCode
	 */
	@JsonProperty("errorCode")
	public void setErrorCode(Integer errorCode) {
		this.errorCode = errorCode;
	}

	/**
	 * Gets the Error Message
	 *@return errorMessage
	 */
	@JsonProperty("errorMessage")
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 * Sets the Error Message
	 * @param errorMessage
	 */
	@JsonProperty("errorMessage")
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	/**
	 * Gets the additional properties
	 * @return additionalProperties
	 */
	@JsonAnyGetter
	public Map<String, Serializable> getAdditionalProperties() {
		return this.additionalProperties;
	}

	/**
	 * Sets the additional properties
	 * @param name
	 * @param value
	 */
	@JsonAnySetter
	public void setAdditionalProperty(String name, Serializable value) {
		this.additionalProperties.put(name, value);
	}

}