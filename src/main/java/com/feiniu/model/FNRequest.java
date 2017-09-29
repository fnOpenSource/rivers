package com.feiniu.model;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.feiniu.config.GlobalParam;
import com.feiniu.model.param.FNParam;

public class FNRequest {
	private String handle = null;
	private String detail = null;
	private String originalKeyword = null; 
	private Map<String, String> params = new HashMap<String, String>(); 
	private ArrayList<String> errors = new ArrayList<String>(); 

	public static FNRequest getInstance() {
		return new FNRequest();
	} 
	
	public String getErrors(){
		String err="";
		for(String s:errors){
			err+=s+",";
		}
		return err;
	}
 
	public String getHandle() {
		return this.handle;
	}

	public void setHandle(String handle) {
		this.handle = handle;
	}

	public String getDetail() {
		return this.detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public String toString() {
		return this.handle + ":" + params.toString();
	}

	public boolean isValid() {
		return this.handle != null && this.handle.length() > 0;
	}

	public boolean addParam(String key, String value) {
		if (key != null && key.length() > 0 && value != null && value.length() > 0){
			params.put(key, value);
			if (key.equals(GlobalParam.PARAM_KEYWORD))
				this.originalKeyword = value;
		} 
		return true;
	}

	public String getOriginalKeyword() {
		return this.originalKeyword;
	}

	public void setOriginalKeyword(String originalKeyword) {
		this.originalKeyword = originalKeyword;
	}

	public String getParam(String key) {
		return this.params.get(key);
	}

	public Map<String, String> getParams() {
		return this.params;
	}  

	public Object get(String key, FNParam pr) {
		if (pr == null)
			return null;
		try {
			Class<?> c = Class.forName(pr.getParamtype());
			String v;
			if (params.containsKey(key)) {
				v = params.get(key);
			}else{
				v = pr.getDefaultValue();
			}
			if (c.getSimpleName().equalsIgnoreCase("String"))
				return v;
			Method method = c.getMethod("valueOf", String.class);
			return method.invoke(c,String.valueOf(v)); 
		} catch (Exception e) {
			addError("param "+key+" parse Exception!");
		}
		return null;
	}
	
	public void addError(String e){
		this.errors.add(e); 
	} 
}