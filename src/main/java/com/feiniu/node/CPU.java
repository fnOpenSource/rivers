package com.feiniu.node;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.UUID;

import com.feiniu.config.InstanceConfig;
import com.feiniu.instruction.Context;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.util.Common;
import com.feiniu.writer.WriterFlowSocket;

/**
 * Instructions Dispatch, RUN and Track Center
 * 
 * @author chengwen
 * @version 1.0
 */
public class CPU { 
	
	static HashMap<String, Context> Contexts = new HashMap<>();
	
	public static void prepare(String runId,InstanceConfig instanceConfig,WriterFlowSocket writer,ReaderFlowSocket reader) { 
		Contexts.put(runId, Context.initContext(instanceConfig, writer,reader));
	}
	
	public static Context getContext(String runId) {
		return Contexts.get(runId);
	}
 
	/**
	 * RUN Instruction processing unit
	 * @param runId tag for instruction context
	 * @param instructionSet 指令集名称
	 * @param instruction 执行指令
	 * @return
	 * @throws Exception
	 */
	public static Object RUN(String runId,String instructionsSet,String instruction,boolean runCheck,Object... args){ 
		Object rs=null;
		try {
			Class<?> clz = Class.forName("com.feiniu.instruction."+instructionsSet); 
			Method m = clz.getMethod(instruction, Context.class,Object[].class);  
			if(instructionsSet.equals("Track")) {
				Object[] argsNew = new Object[args.length+1];
				argsNew[args.length] = runId;
				System.arraycopy(args,0,argsNew,0,args.length);
				rs = m.invoke(null,null,argsNew);
			}else if(Contexts.containsKey(runId)) {
				rs = m.invoke(null,Contexts.get(runId),args);
			}else {
				Common.LOG.error("CPU not ready to run!");
			}
		}catch (Exception e) {
			Common.LOG.error("CPU RUN Exception",e);
		} 
		return rs;
	}
	
	public static String getUUID() {
		return UUID.randomUUID().toString().replace("-", "");
	} 
	
	public static void free(String ObjectId) {
		Contexts.remove(ObjectId);
	}
}
