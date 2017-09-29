package com.feiniu.writerUnit.handler;

import java.util.Map;

import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;

/**
 * user defined data unit process function
 * @author chengwen
 * @version 1.0 
 */
public interface Handler { 
	void handle(WriteUnit u,Object obj,Map<String, WriteParam> writeParamMap); 
}
