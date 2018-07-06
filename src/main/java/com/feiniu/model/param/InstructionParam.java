package com.feiniu.model.param;

import java.util.ArrayList;

import com.feiniu.model.InstructionTree;
import com.feiniu.model.InstructionTree.Node;

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
		for(String line:code.trim().split("\\n")) {  
			InstructionTree instructionSets=null; 
			Node tmp=null;
			for(String str:line.trim().split("->")) {  
				if(instructionSets==null) {
					instructionSets = new InstructionTree(str);
					tmp = instructionSets.getRoot();
				}else { 
					String[] params = str.trim().split(",");
					for(int i=0;i<params.length;i++) {
						if(i==params.length-1) {
							tmp = instructionSets.addNode(params[i], tmp);
						}else {
							instructionSets.addNode(params[i], tmp);
						}
					}
					 
				} 
			}
			this.code.add(instructionSets);
		}
	}
}
