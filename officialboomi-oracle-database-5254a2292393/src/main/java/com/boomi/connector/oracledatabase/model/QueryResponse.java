// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Class QueryResponse.
 *
 * @author swastik.vn
 */
public class QueryResponse {
	
	/** The query. */
	private String query;
	
	/** The rows effected. */
	private int rowsEffected;

	/** The status. */
	private String status;
	
	/**
	 * Instantiates a new query response.
	 *
	 * @param query the query
	 * @param status the status
	 * @param rowsEffected the rows effected
	 */
	public QueryResponse(String query, int rowsEffected, String status) {
		this.query = query;
		this.rowsEffected = rowsEffected;
		this.status = status;
	}

	/**
	 * Gets the query.
	 *
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * Sets the query.
	 *
	 * @param query the new query
	 */
	@JsonProperty("Query")
	public void setQuery(String query) {
		this.query = query;
	}
	
	/**
	 * Gets the status.
	 *
	 * @return the status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * Sets the status.
	 *
	 * @param status the new status
	 */
	@JsonProperty("Status")
	public void setStatus(String status) {
		this.status = status;
	}
	
	/**
	 * Gets the rows effected.
	 *
	 * @return the rows effected
	 */
	public int getRowsEffected() {
		return rowsEffected;
	}

	/**
	 * Sets the rows effected.
	 *
	 * @param rowsEffected the new rows effected
	 */
	@JsonProperty("Rows Affected")
	public void setRowsEffected(int rowsEffected) {
		this.rowsEffected = rowsEffected;
	}
	
	

}
