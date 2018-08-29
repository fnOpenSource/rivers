package com.feiniu.correspond;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.util.ZKUtil;

/**
 * report node machine status with heartBeat and correspond job status
 * @author chengwen
 * @version 1.0
 */ 
public class ReportStatus {  
	
	private final static Logger log = LoggerFactory.getLogger(ReportStatus.class);
	 
	
	public static void start() {
		try {  
			ZKUtil.createPath(GlobalParam.CONFIG_PATH+"/hosts/"+GlobalParam.IP, true);
			ZKUtil.createPath(GlobalParam.CONFIG_PATH+"/hosts/"+GlobalParam.IP+"/instances", true); 
			log.info("Report Status to Zookeeper Success!");
		} catch (Exception e) {
			log.error("ReportStatus start Exception",e);
		} 
	}
	
	public static void report(String instances,String type){
		ZKUtil.createPath(GlobalParam.CONFIG_PATH+"/hosts/relyInstances", true);
		ZKUtil.createPath(GlobalParam.CONFIG_PATH+"/hosts/relyInstances/"+instances+"_"+type, false); 
	}
}
