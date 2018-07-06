package com.feiniu.instruction.flow;

import com.feiniu.instruction.Context;
import com.feiniu.instruction.Instruction;

public class SplitDataFlow extends Instruction{ 
	
	public static double getSplitDayPoint(Context context, Object[] args) {
		double time = 0;
		if (!isValid(1, args)) {
			int days = (int) args[0];
			time = System.currentTimeMillis()-days*3600*1000;
		}
		return time; 
	}
}
