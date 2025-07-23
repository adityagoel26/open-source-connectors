// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sapjco.util;

import java.sql.Timestamp;

/**
 * @author kishore.pulluru
 *
 */
public class PayloadDetails {
	
	String tid;
	String docNumber;
	String docType;
	String idocType;
	String extension;
	String idoc;
	Timestamp timestamp;
	
	/**
	 * This method returns the tid.
	 *
	 */
	public String getTid() {
		return tid;
	}
	
	/**
	 * This method sets the tid.
	 *
	 */
	public void setTid(String tid) {
		this.tid = tid;
	}
	
	/**
	 * This method returns the docNumber.
	 *
	 */
	public String getDocNumber() {
		return docNumber;
	}
	
	/**
	 * This method sets the docNumber.
	 *
	 */
	public void setDocNumber(String docNumber) {
		this.docNumber = docNumber;
	}
	
	/**
	 * This method returns the docType.
	 *
	 */
	public String getDocType() {
		return docType;
	}
	
	/**
	 * This method sets the docType.
	 *
	 */
	public void setDocType(String docType) {
		this.docType = docType;
	}
	
	/**
	 * This method returns the idocType.
	 *
	 */
	public String getIdocType() {
		return idocType;
	}
	
	/**
	 * This method sets the idocType.
	 *
	 */
	public void setIdocType(String idocType) {
		this.idocType = idocType;
	}
	
	/**
	 * This method returns the extension.
	 *
	 */
	public String getExtension() {
		return extension;
	}
	
	/**
	 * This method sets the extension.
	 *
	 */
	public void setExtension(String extension) {
		this.extension = extension;
	}
	
	/**
	 * This method returns the idoc.
	 *
	 */
	public String getIdoc() {
		return idoc;
	}
	
	/**
	 * This method sets the idoc.
	 *
	 */
	public void setIdoc(String idoc) {
		this.idoc = idoc;
	}
	
	/**
	 * This method returns the timestamp.
	 *
	 */
	public Timestamp getTimestamp() {
		return timestamp;
	}
	
	/**
	 * This method sets the timestamp.
	 *
	 */
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	
}
