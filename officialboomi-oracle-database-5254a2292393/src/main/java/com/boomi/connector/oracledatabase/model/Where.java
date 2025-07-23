// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.model;

/**
 * The Class Where.
 *
 * @author swastik.vn
 */
public class Where {
	
	
/** The column. */
private String column;

/** The value. */
private String value;

/** The operator. */
private String operator;


/**
 * Gets the value.
 *
 * @return the value
 */
public String getValue() {
	return value;
}

/**
 * Sets the value.
 *
 * @param value the new value
 */
public void setValue(String value) {
	this.value = value;
}

/**
 * Gets the operator.
 *
 * @return the operator
 */
public String getOperator() {
	return operator;
}

/**
 * Sets the operator.
 *
 * @param operator the new operator
 */
public void setOperator(String operator) {
	this.operator = operator;
}

/**
 * Gets the column.
 *
 * @return the column
 */
public String getColumn() {
	return column;
}

/**
 * Sets the column.
 *
 * @param column the new column
 */
public void setColumn(String column) {
	this.column = column;
}

}
