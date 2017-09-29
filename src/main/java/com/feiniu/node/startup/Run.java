package com.feiniu.node.startup;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.dic.Dictionary;

import com.feiniu.config.FNIocConfig;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeTreeConfigs;
import com.feiniu.node.NodeCenter;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.task.Task;
import com.feiniu.task.TaskManager;
import com.feiniu.util.IKAnalyzer5;
import com.feiniu.util.email.FNEmailSender;

/**
 * app entry startup file
 * @author chengwen
 * @version 1.0 
 */
public class Run {
	@Autowired
	private NodeTreeConfigs NodeTreeConfigs;
	@Autowired
	private SearcherService SearcherService;
	@Autowired
	private TaskManager TaskManager;
	@Autowired
	private HttpReaderService HttpReaderService;
	@Autowired
	private NodeCenter nodeCenter;
	
	@Value("#{configPathConfig['config.path']}")
	private String configPath;
	
	@Resource(name="globalConfigBean")
	private Properties globalConfigBean; 
	
	@Value("#{chechksrvConfig['checksrv.version']}")
	private String version;
	
	@Autowired
	private FNEmailSender mailSender;  
	
	private void loadConfiguration(){ 
		NodeTreeConfigs.init();
		GlobalParam.nodeTreeConfigs = NodeTreeConfigs;
	}
 
	private void start(){ 
		GlobalParam.run_environment = String.valueOf(globalConfigBean.get("run_environment")); 
		GlobalParam.mailSender = mailSender;
		GlobalParam.tasks = new HashMap<String, Task>();
		GlobalParam.NODE_CENTER = nodeCenter;
		GlobalParam.VERSION = version;
		loadConfiguration();  
		int service_level = Integer.parseInt(globalConfigBean.get("service_level").toString()); 
		if((service_level&1)>0){
			GlobalParam.SEARCH_ANALYZER = IKAnalyzer5.getInstance(true);
			SearcherService.start();
		} 
		if((service_level&2)>0)
			TaskManager.start(); 
		if((service_level&4)>0)
			HttpReaderService.start();
	}
	
	public static void main(String[] args) throws URISyntaxException{	
		Run run = (Run)FNIocConfig.getInstance().getBean("FNStart");
		Dictionary.initial(new Configuration(run.configPath));
		run.start();
	}
}
