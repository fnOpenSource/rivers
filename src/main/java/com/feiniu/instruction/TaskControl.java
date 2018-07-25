package com.feiniu.instruction;

import java.util.List;

import com.feiniu.config.GlobalParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

public class TaskControl extends Instruction{ 
	
	public static void moveFullPosition(Context context, Object[] args) {
		if (!isValid(3, args)) {
			Common.LOG.error("moveFullPosition parameter not match!");
			return ;
		} 
		int start = Integer.parseInt(args[0].toString());
		int days = Integer.parseInt(args[1].toString());
		int ride = Integer.parseInt(args[2].toString());
		List<String> seqs = Common.getSeqs(context.getInstanceConfig());  
		for(String seq:seqs) {
			String info = Common.getFullStartInfo(context.getInstanceConfig().getName(), seq);
			String saveInfo="";
			if(info!=null && info.length()>5) {
				for(String tm:info.split(",")) {
					if(Integer.parseInt(tm)<start) {
						saveInfo += String.valueOf(start+days*3600*24*ride)+",";
					}else {
						saveInfo += String.valueOf(Integer.parseInt(tm)+days*3600*24*ride)+",";
					} 
				}
			}else {
				saveInfo = String.valueOf(start + days*3600*24*ride);
			}
			ZKUtil.setData(Common.getTaskStorePath(context.getInstanceConfig().getName(), seq,GlobalParam.JOB_FULLINFO_PATH),saveInfo);
		} 
	}
}
