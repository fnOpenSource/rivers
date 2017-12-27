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
	private String writeTohandler;
	private String searcher;
	private String searcherHandler;
	private String dataFrom;
	private String dataFromHandler;
	private String deltaCron;
	private String fullCron;
	private String splitBy;
	private String[] nextJob;
	/**data write into type,full complete data,increment part of data*/
	private String writeType="full";
	
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
	public String[] getNextJob() {
		return nextJob;
	}
	public String getDataFromhandler() {
		return dataFromHandler;
	}
	public String getWriteTohandler() {
		return writeTohandler;
	}
	public String getSearcherHandler() {
		return searcherHandler;
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
	
	public String getSearcher() {
		if(this.searcher==null){
			this.searcher = this.writeTo;
		}
		return this.searcher;
	} 
	
	public String getWriteType() {
		return writeType;
	} 
	public void setKeyValue(String k,String v){
		switch (k.toLowerCase()) {
		case "writeto":
			this.writeTo = v;
			break; 
		case "datafrom":
			this.dataFrom = v;
			break;
		case "deltacron":
			this.deltaCron = v;
			break;
		case "fullcron":
			this.fullCron = v;
			break;
		case "searcher":
			this.searcher = v;
			break;
		case "searcherhandler":
			this.searcherHandler = v;
			break;
		case "datafromhandler":
			this.dataFromHandler = v;
			break;
		case "writetohandler":
			this.writeTohandler = v;
			break;
		case "nextjob":
			this.nextJob = v.split(","); 
		case "writetype":
			if(v.length()>0 && (v.equals("full") || v.equals("increment")))
				this.writeType = v;
			break;
		}
	}
}
