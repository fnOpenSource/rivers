package com.feiniu.model.param;

import com.feiniu.writerUnit.handler.Handler;

public class WriteParam {

	private String defaultvalue = null;
	private String name = null;
	private String analyzer = null;
	private String stored = "false";
	private String separator = null;
	private String indextype = null;
	private String indexed = "true";
	private Handler handler;

	public String getDefaultvalue() {
		return defaultvalue;
	}

	public void setDefaultvalue(String defaultvalue) {
		this.defaultvalue = defaultvalue;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}

	public String getStored() {
		return stored;
	}

	public void setStored(String stored) {
		this.stored = stored;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public String getIndextype() {
		return indextype;
	}

	public void setIndextype(String indextype) {
		this.indextype = indextype;
	}

	public String getIndexed() {
		return indexed;
	}

	public void setIndexed(String indexed) {
		this.indexed = indexed;
	}

	public Handler getHandler() {
		return this.handler;
	}

	public void setHandler(String handler) throws Exception {
		if(handler!=null && handler.length()>1){
			this.handler = (Handler) Class.forName(handler).newInstance();
		}
	}
}
