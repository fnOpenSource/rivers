package com.feiniu.instruction;

import com.feiniu.util.Common;

public class SplitData extends Instruction{ 
	
	public static double getSplitDayPoint(Context context, Object[] args) {
		double time = 0;
		if (!isValid(1, args)) {
			Common.LOG.error("getSplitDayPoint parameter not match!");
			return time;
		}
		int days = Integer.parseInt(String.valueOf(args[0]));
		time = System.currentTimeMillis()-days*3600*1000;
		return time; 
	}
}
