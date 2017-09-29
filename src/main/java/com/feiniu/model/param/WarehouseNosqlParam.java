package com.feiniu.model.param;

import java.util.ArrayList;
import java.util.List;
 


import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.util.Common;

public class WarehouseNosqlParam implements WarehouseParam{
	
	private DATA_TYPE type = DATA_TYPE.UNKNOWN;
	private String name;
	private String alias;
	private String ip;
	private String defaultValue;
	private String handler;
	private List<String> seqs = new ArrayList<String>();
	
	public DATA_TYPE getType() {
		return type;
	}
	
	public void setType(String type) {
		if (type.equalsIgnoreCase("SOLR"))
			this.type = DATA_TYPE.SOLR;
		else if (type.equalsIgnoreCase("ES"))
			this.type = DATA_TYPE.ES;
		else if (type.equalsIgnoreCase("HBASE"))
			this.type = DATA_TYPE.HBASE;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	@Override
	public String getHandler() {
		return handler;
	}
	public void setHandler(String handler) {
		this.handler = handler;
	}
	public String getDefaultValue() {
		return this.defaultValue;
	}
	public void setDefaultValue(String defaultvalue) {
		this.defaultValue = defaultvalue;
	} 
	public String getAlias() {
		if(this.alias == null){
			this.alias = this.name;
		}
		return this.alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	@Override
	public List<String> getSeq() {
		return this.seqs;
	}
	@Override
	public void setSeq(String seqs) {
		this.seqs = Common.String2List(seqs, ",");
	}

	@Override
	public String getPoolName(String seq) { 
		return ((seq != null) ? this.alias.replace("#{seq}", seq):this.alias)+"_"+this.type+"_"+this.ip;
	}	
}
