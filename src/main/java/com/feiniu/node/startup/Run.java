package com.feiniu.node.startup;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.config.NodeConfig;
import com.feiniu.node.NodeMonitor;
import com.feiniu.node.SocketCenter;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.task.FlowTask;
import com.feiniu.task.TaskManager;
import com.feiniu.util.Common;
import com.feiniu.util.FNIoc; 
import com.feiniu.util.ZKUtil;
import com.feiniu.util.email.FNEmailSender;

/**
 * app entry startup file
 * @author chengwen
 * @version 1.0 
 */
public final class Run {
	@Autowired
	private NodeConfig nodeConfig;
	@Autowired
	private SearcherService SearcherService;
	@Autowired
	private TaskManager TaskManager;
	@Autowired
	private HttpReaderService HttpReaderService;
	@Autowired
	private SocketCenter SocketCenter;
	
	@Value("#{configPathConfig['config.path']}")
	private String configPath;
	
	@Resource(name="globalConfigBean")
	private Properties globalConfigBean; 
	
	@Value("#{chechksrvConfig['checksrv.version']}")
	private String version;
	
	@Autowired
	private FNEmailSender mailSender; 
	
	@Autowired
	NodeMonitor nodeMonitor;
	
	public static void main(String[] args) throws URISyntaxException{	
		Run run = (Run)FNIoc.getInstance().getBean("FNStart"); 
		run.start();
	}
	
	private void start(){ 
		GlobalParam.run_environment = String.valueOf(globalConfigBean.get("run_environment")); 
		GlobalParam.mailSender = mailSender;
		GlobalParam.tasks = new HashMap<String, FlowTask>();
		GlobalParam.SOCKET_CENTER = SocketCenter;
		GlobalParam.VERSION = version;
		GlobalParam.nodeMonitor = nodeMonitor;
		environmentCheck();
		init();   
		if((GlobalParam.SERVICE_LEVEL&1)>0){ 
			SearcherService.start();
		} 
		if((GlobalParam.SERVICE_LEVEL&2)>0)
			TaskManager.startWriteJob(); 
		if((GlobalParam.SERVICE_LEVEL&4)>0)
			HttpReaderService.start();  
		if((GlobalParam.SERVICE_LEVEL&8)>0)
			TaskManager.startInstructions(); 
		 
	} 
	 
	private void init(){ 
		nodeConfig.init();
		GlobalParam.nodeConfig = nodeConfig;
		GlobalParam.SERVICE_LEVEL = Integer.parseInt(globalConfigBean.get("service_level").toString()); 
		
		if((GlobalParam.SERVICE_LEVEL&6)>0) {
			Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
			for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) { 
				InstanceConfig instanceConfig = entry.getValue(); 
				if(instanceConfig.checkStatus())
						initParams(instanceConfig); 
			}
		} 
	}
	
	private void environmentCheck() {
		try {
			if(ZKUtil.getZk().exists(GlobalParam.CONFIG_PATH, true)==null) { 
				String path="";
				for(String str:GlobalParam.CONFIG_PATH.split("/")) {
					path+="/"+str;
					ZKUtil.createPath(path, true);
				} 
				ZKUtil.createPath(path+"/RIVER_LOCKS", true);
			}
		} catch (Exception e) { 
			Common.LOG.error("environmentCheck Exception",e);
		}
	}  
	
	private void initParams(InstanceConfig instanceConfig){ 
		String instance = instanceConfig.getName(); 
		List<String> seqs = Common.getSeqs(instance, instanceConfig); 
		if (seqs.size() == 0) {
			seqs.add(GlobalParam.DEFAULT_RESOURCE_SEQ);
		} 
		for (String seq : seqs) {
			GlobalParam.FLOW_STATUS.set(instance,seq, new AtomicInteger(1));
			GlobalParam.LAST_UPDATE_TIME.set(instance,seq, "0");
		}
	}
} 