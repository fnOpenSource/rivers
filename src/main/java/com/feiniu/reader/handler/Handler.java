package com.feiniu.reader.handler;

import java.util.HashMap;

/**
 * user defined read data process function
 * @author chengwen
 * @version 1.0 
 */
public interface Handler{
	public <T>T Handle(Object... args);
	public boolean needLoop(HashMap<String, String> params);
}
