package com.feiniu.model;

import java.util.HashMap;
import java.util.Map;

import com.feiniu.model.param.TransParam;

public class PipeDataUnit {  
	public String key_column_val;
	private HashMap<String,Object> data;  
	private long SYSTEM_UPDATE_TIME; 
	
	public static PipeDataUnit getInstance(){
		return new PipeDataUnit();
	}
	public PipeDataUnit() {
		this.data = new HashMap<String,Object>();
		this.SYSTEM_UPDATE_TIME = System.currentTimeMillis();
	}
	
	public boolean addFieldValue(String k,Object v,Map<String, TransParam> transParams){
		TransParam param = transParams.get(k);
		if (param != null){
			if (param.getHandler()!=null){
				param.getHandler().handle(this, v,transParams); 
			}
			else{ 
				this.data.put(param.getAlias(),v);
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
 
	public long getUpdateTime(){
		return this.SYSTEM_UPDATE_TIME;
	} 
}
