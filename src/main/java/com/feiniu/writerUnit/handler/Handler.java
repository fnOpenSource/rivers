package com.feiniu.writerUnit.handler;

import java.util.Map;

import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.param.TransParam;

/**
 * user defined data unit process function
 * @author chengwen
 * @version 1.0 
 */
public interface Handler { 
	void handle(PipeDataUnit u,Object obj,Map<String, TransParam> transParams); 
}
