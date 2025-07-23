// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import org.bson.Document;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.mongodb.bean.ErrorDetails;
/**
 * The Class TrackedDataWrapper.
 *
 */
public class TrackedDataWrapper {

	/** The tracked data. */
	TrackedData trackedData;

	/** The doc. */
	Document doc;

	/** The object id. */
	Object objectId;
	
	/** The input Integer*/
	Integer input;
	
	/** The input Double*/
	Double inputDouble;
	
	
	public Double getInputDouble() {
		return inputDouble;
	}

	public void setInputDouble(Double inputDouble) {
		this.inputDouble = inputDouble;
	}

	public Integer getInput() {
		return input;
	}

	public void setInput(Integer input) {
		this.input = input;
	}

	/** The error details. */
	ErrorDetails errorDetails;
	
	/** The input data. */
	String inputData;

	/**
	 * Instantiates a new tracked data wrapper.
	 *
	 * @param trackedData the tracked data
	 * @param doc the doc
	 * @param errorCode the error code
	 * @param errorMessage the error message
	 */
	public TrackedDataWrapper(TrackedData trackedData, Document doc, int errorCode, String errorMessage) {
		super();
		this.trackedData = trackedData;
		this.doc = doc;
		this.errorDetails = new ErrorDetails(errorCode, errorMessage);
	}
	
	/**
	 * Instantiates a new tracked data wrapper.
	 *
	 * @param trackedData the tracked data
	 * @param doc the doc
	 * @param inputData the input data
	 * @param input 
	 * @param inputDouble 
	 */
	public TrackedDataWrapper(TrackedData trackedData, Document doc, String inputData, Integer input, Double inputDouble) {
		super();
		this.trackedData = trackedData;
		this.errorDetails = null;
		if(null!=doc){
			this.doc = doc;
		}else if(null!=input)
		{
			this.input = input;
		}else if(null!=inputDouble)
		{
			this.inputDouble =inputDouble;
		}
		else{
			this.inputData = inputData;
		}
	}

	/**
	 * Instantiates a new tracked data wrapper.
	 *
	 * @param trackedData the tracked data
	 * @param objectId the object id
	 * @param errorCode the error code
	 * @param errorMessage the error message
	 */
	public TrackedDataWrapper(TrackedData trackedData, Object objectId, int errorCode, String errorMessage) {
		super();
		this.trackedData = trackedData;
		this.objectId = objectId;
		this.errorDetails = new ErrorDetails(errorCode, errorMessage);
	}
	
	/**
	 * Instantiates a new tracked data wrapper.
	 *
	 * @param trackedData the tracked data
	 * @param objectId the object id
	 */
	public TrackedDataWrapper(TrackedData trackedData, Object objectId) {
		super();
		this.trackedData = trackedData;
		this.objectId = objectId;
		this.errorDetails = null;
	}
	
	/**
	 * Instantiates a new tracked data wrapper.
	 *
	 * @param trackedData the tracked data
	 * @param doc the doc
	 */
	public TrackedDataWrapper(TrackedData trackedData, Document doc) {
		super();
		this.trackedData = trackedData;
		this.doc = doc;
		this.errorDetails = null;
	}

	/**
	 * Gets the tracked data.
	 *
	 * @return the tracked data
	 */
	public TrackedData getTrackedData() {
		return trackedData;
	}

	/**
	 * Sets the doc.
	 *
	 * @param doc the new doc
	 */
	public void setDoc(Document doc) {
		this.doc = doc;
	}

	/**
	 * Gets the doc.
	 *
	 * @return the doc
	 */
	public Document getDoc() {
		return doc;
	}

	/**
	 * Sets the object id.
	 *
	 * @param objectId the new object id
	 */
	public void setObjectId(Object objectId) {
		this.objectId = objectId;
	}

	/**
	 * Gets the object id.
	 *
	 * @return the object id
	 */
	public Object getObjectId() {
		return objectId;
	}

	/**
	 * Gets the error details.
	 *
	 * @return the error details
	 */
	public ErrorDetails getErrorDetails() {
		return errorDetails;
	}

	/**
	 * Sets the error details.
	 *
	 * @param errorCode the error code
	 * @param errorMessage the error message
	 */
	public void setErrorDetails(Integer errorCode,String errorMessage) {
		this.errorDetails = new ErrorDetails(errorCode, errorMessage);
	}

	/**
	 * Gets the input data.
	 *
	 * @return the input data
	 */
	public String getInputData() {
		return inputData;
	}
	

}
