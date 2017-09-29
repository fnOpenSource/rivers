package com.feiniu.model.param;

import java.util.ArrayList;
import java.util.List;

import com.feiniu.util.Common;

public class SQLParam {
	private String sql;
	private String mainTable;
	private String mainAlias = "";
	private String keyColumn;
	/**value= int or string */
	private String keyColumnType;
	private String incrementField = "";
	private String pageSql;
	private List<String> seq = new ArrayList<String>();
	
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql.trim();
		if(this.sql.length()>1 && this.sql.substring(this.sql.length()-1).equals(";")){
			this.sql = this.sql.substring(0,this.sql.length()-1);
		}
	}
	
	public String getMainTable() {
		return mainTable;
	}
	public void setMainTable(String mainTable) {
		this.mainTable = mainTable;
	}
	public String getMainAlias() {
		return mainAlias;
	}
	public void setMainAlias(String mainAlias) {
		this.mainAlias = mainAlias;
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
	public List<String> getSeq() {
		return seq;
	}
	public void setSeq(String seqs) {
		this.seq = Common.String2List(seqs, ",");
	}
	public String getPageSql() {
		return this.pageSql;
	}
	public void setPageSql(String pageSql) {
		this.pageSql = pageSql;
	}
	public String getKeyColumnType() {
		return keyColumnType;
	}
	public void setKeyColumnType(String keyColumnType) {
		this.keyColumnType = keyColumnType;
	}  
}
