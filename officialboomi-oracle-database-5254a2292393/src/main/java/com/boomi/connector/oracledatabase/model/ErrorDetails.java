// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.model;

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
 * @author swastik.vn
 **/
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "errorCode", "errorMessage" })
public class ErrorDetails implements Serializable {


	private static final long serialVersionUID = 1L;
	
	@JsonProperty("errorCode")
	private Integer errorCode;
	/**
	 *
	 * (Required)
	 *
	 */
	@JsonProperty("errorMessage")
	private String errorMessage;
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

	@JsonProperty("errorCode")
	public Integer getErrorCode() {
		return errorCode;
	}

	@JsonProperty("errorCode")
	public void setErrorCode(Integer errorCode) {
		this.errorCode = errorCode;
	}

	/**
	 *
	 * (Required)
	 *
	 */
	@JsonProperty("errorMessage")
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 *
	 * (Required)
	 *
	 */
	@JsonProperty("errorMessage")
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	@JsonAnyGetter
	public Map<String, Serializable> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, Serializable value) {
		this.additionalProperties.put(name, value);
	}

}