package com.feiniu.instruction;

import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.model.computer.SampleSets;
import com.feiniu.model.reader.DataPage;

public class ML extends Instruction {
	
	private final static Logger log = LoggerFactory.getLogger("ML");
	
	public static DataPage train(Context context, Object[] args) {
		if (!isValid(3, args)) {
			log.error("train parameter not match!");
			return null;
		}
		try {
			Class<?> clz = Class.forName("com.feiniu.ml.algorithm."+String.valueOf(args[0])); 
			Method m = clz.getMethod("train", Context.class,SampleSets.class,Map.class);   
			return (DataPage) m.invoke(null,context,args[1],args[2]);
		}catch (Exception e) {
			log.error("train Exception",e);
		} 
		return null; 
	}
	
}
