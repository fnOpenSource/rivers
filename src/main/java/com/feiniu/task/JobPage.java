package com.feiniu.task;

import java.util.Map;

import com.feiniu.field.RiverField;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-08 16:49
 */
public class JobPage {
	private String sql;
	private String incrementField;
	private String keyColumn;
	private Map<String, RiverField> transField;
	private long timeStamp = System.currentTimeMillis();

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getIncrementField() {
		return incrementField;
	}

	public void setIncrementField(String incrementField) {
		this.incrementField = incrementField;
	}

	public String getKeyColumn() {
		return keyColumn;
	}

	public void setKeyColumn(String keyColumn) {
		this.keyColumn = keyColumn;
	}

	public Map<String, RiverField> getTransField() {
		return transField;
	}

	public void setTransField(Map<String, RiverField> transField) {
		this.transField = transField;
	}

	public long getTimeStamp() {
		return timeStamp;
	}
}
