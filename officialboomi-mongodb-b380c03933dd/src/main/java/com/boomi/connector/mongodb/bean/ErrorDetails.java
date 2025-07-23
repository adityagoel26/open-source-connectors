// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.bean;

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
 * The Class ErrorDetails.
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "errorCode", "errorMessage" })
public class ErrorDetails implements Serializable {

	/** The error code. */
	@JsonProperty("errorCode")
	private Integer errorCode;
	
	/** (Required). */
	@JsonProperty("errorMessage")
	private String errorMessage;
	
	/** The additional properties. */
	@JsonIgnore
	private transient Map<String, Object> additionalProperties = new HashMap<>();
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -2954066474470102732L;

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
	 * Gets the error code.
	 *
	 * @return the error code
	 */
	@JsonProperty("errorCode")
	public Integer getErrorCode() {
		return errorCode;
	}

	/**
	 * Sets the error code.
	 *
	 * @param errorCode the new error code
	 */
	@JsonProperty("errorCode")
	public void setErrorCode(Integer errorCode) {
		this.errorCode = errorCode;
	}

	/**
	 * (Required).
	 *
	 * @return the error message
	 */
	@JsonProperty("errorMessage")
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 * (Required).
	 *
	 * @param errorMessage the new error message
	 */
	@JsonProperty("errorMessage")
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	/**
	 * Gets the additional properties.
	 *
	 * @return the additional properties
	 */
	@JsonAnyGetter
	public Map<String, Object> getAdditionalProperties() {
		return this.additionalProperties;
	}

	/**
	 * Sets the additional property.
	 *
	 * @param name the name
	 * @param value the value
	 */
	@JsonAnySetter
	public void setAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
	}

}