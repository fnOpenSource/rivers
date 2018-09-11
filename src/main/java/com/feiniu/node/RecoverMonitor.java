package com.feiniu.node;

import java.util.List;

import org.apache.commons.net.telnet.TelnetClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.feiniu.config.GlobalParam;
import com.feiniu.node.startup.Run;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

@Component
public class RecoverMonitor {
	@Autowired
	private Run RIVERS;
	
	public void start() {
		while(true) {
			try {
				List<String> childrenList = ZKUtil.getZk().getChildren(GlobalParam.CONFIG_PATH+"/RIVER_NODES", true);
				TelnetClient client = new TelnetClient(); 
				client.setDefaultTimeout(2000); 
				for(String ip:childrenList) {
					try { 
						client.connect(ip, 8617); 
					} catch (Exception e) {
						takeOverNode(ip);
						return;
					}
				}
				Thread.sleep(5000);
			}catch (Exception e) {
				Common.LOG.error("RecoverMonitor start Exception",e);
			}  
		} 
	} 
	
	private void returnNode(String ip) {
		TelnetClient client = new TelnetClient(); 
		client.setDefaultTimeout(2000); 
		while(true) {
			try {
				try { 
					client.connect(ip, 8617); 
					Common.LOG.info(GlobalParam.IP+"has return Node "+ip);
					Common.restartNode();
				} catch (Exception e) { 
					Thread.sleep(5000);
					continue;
				} 
			}catch (Exception e) {
				continue;
			}  
		}
	}
	
	private void takeOverNode(String ip) {
		returnNode(ip);
		RIVERS.loadGlobalConfig(GlobalParam.CONFIG_PATH+"/RIVER_NODES/"+ip+"/configs",true); 
		RIVERS.init();
		RIVERS.startService();
		Common.LOG.info(GlobalParam.IP+" has take Over Node "+ip);
	} 

}
