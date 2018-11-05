package com.feiniu.util;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:12
 */
public class FNException extends Exception{ 
	private static final long serialVersionUID = 1L; 
	public FNException(String msg){
		super(msg); 
	} 
	public FNException(Exception e){
		super(e); 
	}
}
