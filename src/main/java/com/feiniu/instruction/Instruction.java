package com.feiniu.instruction;

import java.util.Stack;

import com.feiniu.model.param.WarehouseParam;

public class Instruction {
	 
	public Stack<Object> context = new Stack<>();
	
	private WarehouseParam in;
	
	private WarehouseParam out;
 
	public WarehouseParam getIn() {
		return in;
	}

	public void setIn(WarehouseParam in) {
		this.in = in;
	}

	public WarehouseParam getOut() {
		return out;
	}

	public void setOut(WarehouseParam out) {
		this.out = out;
	}  
}
