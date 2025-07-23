// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.model;

/**
 * The Class Set.
 *
 * @author swastik.vn
 */
public class Set {

	private String column;

	private String[] value;

	private String innerTableTypeName1;

	private String[] innerTableValue1;

	private String innerTableTypeName2;

	private String[] innerTableValue2;

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getInnerTableTypeName1() {
		return innerTableTypeName1;
	}

	public void setInnerTableTypeName1(String innerTableTypeName1) {
		this.innerTableTypeName1 = innerTableTypeName1;
	}

	public String[] getInnerTableValue1() {
		return innerTableValue1;
	}

	public void setInnerTableValue1(String[] innerTableValue1) {
		this.innerTableValue1 = innerTableValue1;
	}

	public String getInnerTableTypeName2() {
		return innerTableTypeName2;
	}

	public void setInnerTableTypeName2(String innerTableTypeName2) {
		this.innerTableTypeName2 = innerTableTypeName2;
	}

	public String[] getInnerTableValue2() {
		return innerTableValue2;
	}

	public void setInnerTableValue2(String[] innerTableValue2) {
		this.innerTableValue2 = innerTableValue2;
	}

	public String[] getValue() {
		return value;
	}

	public void setValue(String[] value) {
		this.value = value;
	}

}
