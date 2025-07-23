// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Class UpdatePojo.
 *
 * @author swastik.vn
 */
public class UpdatePojo {
	
	/** The set. */
	private List<Set> set;

	/** The where. */
	private List<Where> where;
	
	 /**
 	 * Gets the sets the.
 	 *
 	 * @return the sets the
 	 */
 	public List<Set> getSet() {
		return set;
	}
	
	/**
	 * Sets the sets the.
	 *
	 * @param set the new sets the
	 */
	@JsonProperty("SET")
	public void setSet(List<Set> set) {
		this.set = set;
	}
	
	/**
	 * Gets the where.
	 *
	 * @return the where
	 */
	public List<Where> getWhere() {
		return where;
	}
	
	/**
	 * Sets the where.
	 *
	 * @param where the new where
	 */
	@JsonProperty("WHERE")
	public void setWhere(List<Where> where) {
		this.where = where;
	}

}
