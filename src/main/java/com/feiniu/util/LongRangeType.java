package com.feiniu.util;

public class LongRangeType extends NumberRangeType<Long>{

	public LongRangeType() {
		min = 0l;
		max = Long.MAX_VALUE;
	}

	// just use NumberFormatException
	public static LongRangeType valueOf(String s)
			throws NumberFormatException {
		LongRangeType ir = new LongRangeType();
		ir.parseValue(s);
		return ir;
	}

	@Override
	public Long getValueof(String s) {
		return Long.valueOf(s);
	} 
}
