package com.feiniu.instruction;

import com.feiniu.model.param.WarehouseParam;

public class PondCreate extends Instruction {

	public static Instruction getInstruction(WarehouseParam in,WarehouseParam out) {
		PondCreate o = new PondCreate(); 
		o.setIn(in);
		o.setOut(out);
		return o;
	}
}
