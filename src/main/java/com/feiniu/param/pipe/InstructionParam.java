package com.feiniu.param.pipe;

import java.util.ArrayList;

import com.feiniu.model.InstructionTree;
import com.feiniu.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
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
