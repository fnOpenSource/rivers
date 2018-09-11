package com.feiniu.node.startup;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.dic.Dictionary;

import com.alibaba.fastjson.JSON;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.NODE_TYPE;
import com.feiniu.config.InstanceConfig;
import com.feiniu.config.NodeConfig;
import com.feiniu.node.NodeMonitor;
import com.feiniu.node.RecoverMonitor;
import com.feiniu.node.SocketCenter;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.task.FlowTask;
import com.feiniu.task.TaskManager;
import com.feiniu.util.Common;
import com.feiniu.util.FNIoc;
import com.feiniu.util.IKAnalyzer5;
import com.feiniu.util.ZKUtil;
import com.feiniu.util.email.FNEmailSender;

/**
 * app entry startup file
 * 
 * @author chengwen
 * @version 2.1
 */
public final class Run {
	@Autowired
	private SearcherService searcherService;
	@Autowired
	private TaskManager taskManager;
	@Autowired
	private RecoverMonitor recoverMonitor;
	@Autowired
	private HttpReaderService httpReaderService;
	@Autowired
	private SocketCenter socketCenter;

	@Value("#{chechksrvConfig['checksrv.version']}")
	private String version;

	@Autowired
	private FNEmailSender mailSender;

	@Autowired
	NodeMonitor nodeMonitor;
	
	private String startConfigPath; 
	 
	public Run() {
		
	}
	public Run(String startConfigPath,String dictionaryPath) {
		this.startConfigPath = startConfigPath; 
		Dictionary.initial(new Configuration(dictionaryPath));
	} 
	
	public void init() {
		GlobalParam.run_environment = String.valueOf(GlobalParam.StartConfig.get("run_environment"));
		GlobalParam.mailSender = mailSender;
		GlobalParam.tasks = new HashMap<String, FlowTask>();
		GlobalParam.SOCKET_CENTER = socketCenter;
		GlobalParam.TASKMANAGER = taskManager;
		GlobalParam.VERSION = version;
		GlobalParam.nodeMonitor = nodeMonitor; 
		GlobalParam.POOL_SIZE = Integer.parseInt(GlobalParam.StartConfig.getProperty("pool_size"));
		GlobalParam.WRITE_BATCH = GlobalParam.StartConfig.getProperty("write_batch").equals("false") ? false
				: true; 

		GlobalParam.nodeConfig = NodeConfig.getInstance(String.valueOf(GlobalParam.StartConfig.get("instances")),
				String.valueOf(GlobalParam.StartConfig.get("pond")),
				String.valueOf(GlobalParam.StartConfig.get("instructions")));
		GlobalParam.nodeConfig.init();
		ZKUtil.setData(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",
				JSON.toJSONString(GlobalParam.StartConfig));

		GlobalParam.SERVICE_LEVEL = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());

		Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
			InstanceConfig instanceConfig = entry.getValue();
			if (instanceConfig.checkStatus())
				initParams(instanceConfig);
		}
	}

	public void startService() {
		if ((GlobalParam.SERVICE_LEVEL & 1) > 0) {
			GlobalParam.SEARCH_ANALYZER = IKAnalyzer5.getInstance(true);
			searcherService.start();
		}
		if ((GlobalParam.SERVICE_LEVEL & 2) > 0)
			taskManager.startWriteJob();
		if ((GlobalParam.SERVICE_LEVEL & 4) > 0)
			httpReaderService.start();
		if ((GlobalParam.SERVICE_LEVEL & 8) > 0)
			taskManager.startInstructions();
	}

	public void loadGlobalConfig(String path,boolean fromZk) {
		try {
			GlobalParam.StartConfig = new Properties();
			if(fromZk) {
				GlobalParam.StartConfig.load(new ByteArrayInputStream(ZKUtil.getData(path, false)));
			}else { 
				String replaceStr = System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS")==-1?"file:":"file:/";
				try(FileInputStream in = new FileInputStream(path.replace(replaceStr, ""))) {
					GlobalParam.StartConfig.load(in);
				}catch (Exception e) {
					Common.LOG.error("load Global Properties file Exception", e);
				} 
			}  
		} catch (Exception e) {
			Common.LOG.error("load Global Properties Config Exception", e);
		}
		GlobalParam.CONFIG_PATH = GlobalParam.StartConfig.getProperty("zkConfigPath");
		ZKUtil.setZkHost(GlobalParam.StartConfig.getProperty("zkhost"));
	}

	private void start() {
		loadGlobalConfig(this.startConfigPath,false);
		environmentCheck();
		if (GlobalParam.StartConfig.get("node_type").equals(NODE_TYPE.backup.name())) {
			recoverMonitor.start();
		} else {
			init();
			startService();
		}
	}

	private void environmentCheck() {
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

	private void initParams(InstanceConfig instanceConfig) {
		String instance = instanceConfig.getName();
		String[] seqs = Common.getSeqs(instanceConfig, true);
		for (String seq : seqs) {
			GlobalParam.FLOW_STATUS.set(instance, seq, new AtomicInteger(1));
			GlobalParam.LAST_UPDATE_TIME.set(instance, seq, "0");
		}
	}
	
	public static void main(String[] args) throws URISyntaxException {
		GlobalParam.RunBean = (Run) FNIoc.getInstance().getBean("FNStart"); 
		GlobalParam.RunBean.start();
	}

}