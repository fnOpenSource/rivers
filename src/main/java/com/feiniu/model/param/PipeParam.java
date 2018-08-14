package com.feiniu.model.param;

/**
 * data-flow trans parameters
 * @author chengwen
 * @version 1.0 
 */
public class PipeParam {
	private SQLParam sqlParam;
	private NOSQLParam noSqlParam;
	private String writeTo;
	private String writeTohandler;
	private boolean writerPoolShareAlias = true;
	private String searcher;
	private String searcherHandler;
	private boolean searcherShareAlias = true;
	private String dataFrom;
	private String dataFromHandler;
	private boolean readerPoolShareAlias = false;
	private String deltaCron;
	private String fullCron;
	private String optimizeCron; 
	private String instanceName;
	private String[] nextJob;
	/** default is slave pipe,if is master will only manage pipe with no detail transfer job! */
	private boolean isMaster = false;
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
	public String getInstanceName() {
		return instanceName;
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
	public String getOptimizeCron() {
		return optimizeCron;
	}
	public void setOptimizeCron(String optimizeCron) {
		this.optimizeCron = optimizeCron;
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
	
	public void setInstancename(String v) {
		this.instanceName = v;
	}
	
	public boolean isWriterPoolShareAlias() {
		return writerPoolShareAlias;
	} 
	
	public boolean isReaderPoolShareAlias() {
		return readerPoolShareAlias;
	} 

	public boolean isSearcherShareAlias() {
		return searcherShareAlias;
	}

	public boolean isMaster() {
		return isMaster;
	} 
	
	public void setKeyValue(String k,String v){ 
		switch (k.toLowerCase()) {
		case "writeto":
			this.writeTo = v;
			break;
		case "writerPoolShareAlias":
			this.writerPoolShareAlias = Boolean.valueOf(v);
			break;
		case "readerPoolShareAlias":
			this.readerPoolShareAlias = Boolean.valueOf(v);
			break;
		case "searcherShareAlias":
			this.searcherShareAlias = Boolean.valueOf(v);
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
		case "optimizecron":
			this.optimizeCron = v;
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
		case "instancename":
			this.instanceName = v;
			break;
		case "nextjob":
			this.nextJob = v.replace(",", " ").trim().split(" "); 
			break;
		case "ismaster":
			if(v.length()>0 && v.toLowerCase().equals("true"))
				this.isMaster = true;
			break;
		case "writetype":
			if(v.length()>0 && (v.equals("full") || v.equals("increment")))
				this.writeType = v;
			break;
		}
	}
}
