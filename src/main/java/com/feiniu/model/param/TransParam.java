package com.feiniu.model.param;

/**
 * data-flow trans parameters
 * @author chengwen
 * @version 1.0 
 */
public class TransParam {
	private SQLParam sqlParam;
	private NOSQLParam noSqlParam;
	private String writeTo;
	private String searcher;
	private String dataFrom;
	private String handler;
	private String deltaCron;
	private String fullCron;
	private String splitBy;
	
	public SQLParam getSqlParam() {
		return sqlParam;
	}
	public void setSqlParam(SQLParam sqlParam) {
		this.sqlParam = sqlParam;
	}
	public NOSQLParam getNoSqlParam() {
		return noSqlParam;
	}
	public void setNoSqlParam(NOSQLParam sqlParam) {
		this.noSqlParam = sqlParam;
	}
	public String getWriteTo() {
		return writeTo;
	}
	public void setWriteTo(String writeTo) {
		this.writeTo = writeTo;
	}
	public String getDataFrom() {
		return dataFrom;
	}
	public void setDataFrom(String dataFrom) {
		this.dataFrom = dataFrom;
	}
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public String getDeltaCron() {
		return deltaCron;
	}
	public void setDeltaCron(String deltaCron) {
		this.deltaCron = deltaCron;
	}
	public String getFullCron() {
		return fullCron;
	}
	public void setFullCron(String fullCron) {
		this.fullCron = fullCron;
	}  
	
	public String getSplitBy() {
		return splitBy;
	}
	public void setSplitBy(String splitBy) {
		this.splitBy = splitBy;
	}
	
	public String getSearcher() {
		if(this.searcher==null){
			this.searcher = this.writeTo;
		}
		return this.searcher;
	}
	public void setSearcher(String searcher) {
		this.searcher = searcher;
	}
	public void setKeyValue(String k,String v){
		switch (k) {
		case "writeTo":
			this.writeTo = v;
			break; 
		case "dataFrom":
			this.dataFrom = v;
			break;
		case "deltaCron":
			this.deltaCron = v;
			break;
		case "fullCron":
			this.fullCron = v;
			break;
		case "searcher":
			this.searcher = v;
			break;
		case "handler":
			this.handler = v;
			break;
		case "splitBy":
			this.splitBy = v;
			break;
		}
	}
}
