package com.feiniu.model;

import java.util.HashMap;
import java.util.Map;

import com.feiniu.model.param.WriteParam;

public class WriteUnit {  
	public String key_column_val;
	private HashMap<String,Object> data;  
	private double SYSTEM_UPDATE_TIME; 
	
	public static WriteUnit getInstance(){
		return new WriteUnit();
	}
	public WriteUnit() {
		this.data = new HashMap<String,Object>();
		this.SYSTEM_UPDATE_TIME = System.currentTimeMillis();
	}
	
	public boolean addFieldValue(String k,Object v,Map<String, WriteParam> writeParamMap){
		WriteParam param = writeParamMap.get(k);
		if (param != null){
			if (param.getHandler()!=null){
				param.getHandler().handle(this, v,writeParamMap); 
			}
			else{ 
				this.data.put(k,v);
			}
			return true;
		}else{
			return false;
		}
	}
	
	public void setKeyColumnVal(Object key_column_val){
		this.key_column_val = String.valueOf(key_column_val);
	}
 
	public HashMap<String,Object> getData() {
		return data;
	}
	
	public String getKeyColumnVal() {
		return key_column_val;
	}
 
	public double getUpdateTime(){
		return this.SYSTEM_UPDATE_TIME;
	} 
}
