package com.feiniu.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;

public final class NodeUtil {
	
	/**
	 * init node start parameters
	 * @param instanceConfig
	 */
	public static void initParams(InstanceConfig instanceConfig) {
		String instance = instanceConfig.getName();
		String[] seqs = Common.getSeqs(instanceConfig, true);
		for (String seq : seqs) { 
			GlobalParam.FLOW_STATUS.set(instance, seq,GlobalParam.JOB_TYPE.FULL.name(), new AtomicInteger(1));
			GlobalParam.FLOW_STATUS.set(instance, seq,GlobalParam.JOB_TYPE.INCREMENT.name(), new AtomicInteger(1));
			GlobalParam.LAST_UPDATE_TIME.set(instance, seq, "0");
		}
	}
	
	public static void runShell(String path) { 
		Process  pc = null;
		try { 
			Common.LOG.info("Start Run Script "+path);
			pc = Runtime.getRuntime().exec(path);
			pc.waitFor();
		} catch (Exception e) {
			Common.LOG.error("restartNode Exception",e);
		}finally {
			if(pc != null){
				pc.destroy();
            }
		}
	} 
}
