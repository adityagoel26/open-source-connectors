// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * The Class DeletePojo.
 *
 * @author swastik.vn
 */
public class DeletePojo {

	/** The where. */
	private List<Where> where;

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
