package com.feiniu.model.param;


public class NOSQLParam { 
	private String mainTable; 
	private String keyColumn;
	private String incrementField = ""; 
	
	public String getMainTable() {
		return mainTable;
	}
	public void setMainTable(String mainTable) {
		this.mainTable = mainTable;
	} 
	public String getKeyColumn() {
		return keyColumn;
	}
	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}
	public String getIncrementField() {
		return incrementField;
	}
	public void setIncrementField(String incrementField) {
		this.incrementField = incrementField;
	} 
}
