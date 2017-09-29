package com.feiniu.model.param;
/**
 * alias,read data field name
 * name,write data field name,is also search name
 * @author chengwen
 * @version 1.0 
 */
public class FNParam {
	
	private String alias = null;
	private String name = null;
	private String paramtype = null;//for searchtype
	private String defaultValue = null; 
	private String fields = null;
	private String analyzer = "";
	private float boost = 1.0f;
	private String stored = "false";
	private boolean isClosedInvterval = true;
	private String separator = null;
	private String indextype = null;//for data storetype

	public boolean isValid(String value) { 
		return true;
	}  

	public String getAlias() {
		if(this.alias==null){
			this.alias = this.name;
		}
		return this.alias;
	} 

	public void setAlias(String alias) {
		this.alias = alias;
	} 

	public String getParamtype() {
		if(this.paramtype==null){
			this.paramtype = "java.lang.String";
		}
		return this.paramtype;
	} 

	public void setParamtype(String paramtype) {
		this.paramtype = paramtype;
	} 

	public String getDefaultValue() {
		return this.defaultValue;
	} 

	public void setDefaultValue(String value) {
		this.defaultValue = value;
	} 

	public void setName(String name) {
		this.name = name;
	}

	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}

	public void setBoost(float boost) {
		this.boost = boost;
	}

	public void setClosedInvterval(boolean isClosedInvterval) {
		this.isClosedInvterval = isClosedInvterval;
	}

	public String getName() {
		return name;
	}

	public String getAnalyzer() {
		return analyzer;
	}

	public float getBoost() {
		return boost;
	}

	public void setBoost(String boost) {
		this.boost = Float.valueOf(boost);
	}

	public boolean isClosedInvterval() {
		return isClosedInvterval;
	}

	public void setIsClosedInvterval(String isClosedInvterval) {
		if (isClosedInvterval.equalsIgnoreCase("false"))
			this.isClosedInvterval = false;
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
	
	public void setIndextype(String indextype){
		this.indextype = indextype;
	}

	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}
	
	
}
