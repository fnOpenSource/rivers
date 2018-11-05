package com.feiniu.param.pipe;

import com.feiniu.param.warehouse.ScanParam;

/**
 * data-flow trans parameters
 * @author chengwen
 * @version 4.0
 * @date 2018-10-25 16:14
 */
public class PipeParam {
	private ScanParam readParam; 
	private String writeTo;
	private String modelFrom;
	private String writeHandler;
	private boolean writerPoolShareAlias = true;
	private String searchFrom;
	private String searcherHandler;
	private boolean searcherShareAlias = true;
	private String readFrom;
	private String readHandler;
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
	
	public ScanParam getReadParam() {
		return this.readParam;
	}
 
	public void setReadParam(ScanParam readParam) {
		this.readParam = readParam;
	}
	public String getWriteTo() {
		return writeTo;
	}
	public String getModelFrom() {
		return modelFrom;
	}
	public void setWriteTo(String writeTo) {
		this.writeTo = writeTo;
	}
	public String getReadFrom() {
		return readFrom;
	}
	public String[] getNextJob() {
		return nextJob;
	}
	public String getReadHandler() {
		return readHandler;
	}
	public String getWriteHandler() {
		return writeHandler;
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
	
	public String getSearchFrom() {
		if(this.searchFrom==null){
			this.searchFrom = this.writeTo;
		}
		return this.searchFrom;
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
		case "writerpoolsharealias":
			this.writerPoolShareAlias = Boolean.valueOf(v);
			break;
		case "readerpoolsharealias":
			this.readerPoolShareAlias = Boolean.valueOf(v);
			break;
		case "searchersharealias":
			this.searcherShareAlias = Boolean.valueOf(v);
			break;
		case "readfrom":
			this.readFrom = v;
			break;
		case "modelfrom":
			this.modelFrom = v;
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
		case "searchfrom":
			this.searchFrom = v;
			break;
		case "searcherhandler":
			this.searcherHandler = v;
			break;
		case "readhandler":
			this.readHandler = v;
			break;
		case "writehandler":
			this.writeHandler = v;
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
