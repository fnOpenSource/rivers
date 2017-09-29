package com.feiniu.util;

/**
 * search parameter NumberRangeType change to min and max parameters
 * @author chengwen
 * @version 1.0 
 */
public abstract class NumberRangeType<T extends Comparable<T>> {
	protected T min;
	protected T max;
	public final static String RangeSeperator = "_";
	
	public abstract T getValueof(String s);
	
	public void parseValue(String s)
			throws NumberFormatException { 
		if (s == null) {
			throw new NumberFormatException("null");
		}
		int seg = s.indexOf(RangeSeperator);
		if (seg >= s.length()) {
			throw new NumberFormatException(s);
		} 
		if(seg<0){
			this.setMax(getValueof(s));
			this.setMin(getValueof(s));
		}
		try {
			if (seg > 0){
				String minStr = s.substring(0, seg); 
				T min =  getValueof(minStr);
				this.setMin(min);
			}
			if (seg < s.length()-1){
				String maxStr = s.substring(seg+1);
				T max = getValueof(maxStr);
				this.setMax(max);
			}
		} catch (Exception e) {
			throw new NumberFormatException(s);
		}
	}
	
	public String toString() {
		return min + RangeSeperator + max;
	}
	public boolean isValid() {
		return max.compareTo(min) >= 0;
	}
	public T getMin() {
		return min;
	}
	public void setMin(T min) {
		this.min = min;
	}
	public T getMax() {
		return max;
	}
	public void setMax(T max) {
		this.max = max;
	}
}
