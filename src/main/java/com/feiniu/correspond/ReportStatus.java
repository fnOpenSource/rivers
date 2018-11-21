package com.feiniu.correspond;

import com.feiniu.config.GlobalParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;
 
/**
 * report node machine status with heartBeat and correspond job status
 * @author chengwen
 * @version 2.0
 * @date 2018-11-21 15:43
 */
public final class ReportStatus {  
	 
	public static void jobState() {
		 
	}
	
	public static void nodeConfigs(){
		try {
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH, true) == null) {
				String path = "";
				for (String str : GlobalParam.CONFIG_PATH.split("/")) {
					path += "/" + str;
					ZKUtil.createPath(path, true);
				}
			}
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES", false) == null) {
				ZKUtil.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES", true);
			}
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP, false) == null) {
				ZKUtil.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP, true);
			}
			if (ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",
					false) == null) {
				ZKUtil.createPath(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs", true);
			}
		} catch (Exception e) {
			Common.LOG.error("environmentCheck Exception", e);
		}
	}
}
