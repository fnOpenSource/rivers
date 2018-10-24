package com.feiniu.writerUnit.handler;

import java.util.Map;

import com.feiniu.field.RiverField;
import com.feiniu.model.PipeDataUnit;

/**
 * user defined data unit process function
 * @author chengwen
 * @version 1.0 
 */
public interface Handler { 
	void handle(PipeDataUnit u,Object obj,Map<String, RiverField> transParams); 
}
