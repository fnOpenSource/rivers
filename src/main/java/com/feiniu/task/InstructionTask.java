package com.feiniu.task;

import java.util.ArrayList;

import com.feiniu.config.GlobalParam;
import com.feiniu.model.InstructionTree;

public class InstructionTask {

	private String codeID;

	public static InstructionTask createTask(String id) {
		InstructionTask tk = new InstructionTask();
		tk.codeID = id;
		return tk;
	}

	public void runInstructions() {
		ArrayList<InstructionTree> Instructions = GlobalParam.nodeConfig.getInstructions().get(this.codeID).getCode(); 
		for(InstructionTree Instruction:Instructions ) {
			Instruction.depthRun(Instruction.getRoot());
		}
	}
}
