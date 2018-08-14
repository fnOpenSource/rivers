package com.feiniu.model.param;

import java.util.ArrayList;

import com.feiniu.model.InstructionTree;
import com.feiniu.util.Common;

public class InstructionParam {
	
	private String id;
	private String cron;
	private ArrayList<InstructionTree> code= new ArrayList<>();
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getCron() {
		return cron;
	}
	public void setCron(String cron) {
		this.cron = cron;
	} 
	public ArrayList<InstructionTree> getCode() {
		return this.code;
	}
	public void setCode(String code) {
		 this.code = Common.compileCodes(code, this.id);
	}
}
