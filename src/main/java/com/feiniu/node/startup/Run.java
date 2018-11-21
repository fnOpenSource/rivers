package com.feiniu.node.startup;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.feiniu.computer.service.ComputerService;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.NODE_TYPE;
import com.feiniu.config.InstanceConfig;
import com.feiniu.config.NodeConfig;
import com.feiniu.correspond.ReportStatus;
import com.feiniu.node.FlowCenter;
import com.feiniu.node.NodeMonitor;
import com.feiniu.node.RecoverMonitor;
import com.feiniu.node.SocketCenter;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.service.FNMonitor;
import com.feiniu.task.FlowTask;
import com.feiniu.util.Common;
import com.feiniu.util.FNIoc;
import com.feiniu.util.NodeUtil;
import com.feiniu.util.ZKUtil;
import com.feiniu.util.email.FNEmailSender;

/**
 * Application startup position
 * @author chengwen
 * @version 4.0
 * @date 2018-11-19 15:33
 */
public final class Run {
	@Autowired
	private SearcherService searcherService;
	@Autowired
	private ComputerService computerService;
	@Autowired
	private FlowCenter flowCenter;
	@Autowired
	private RecoverMonitor recoverMonitor;
	@Autowired
	private HttpReaderService httpReaderService;
	@Autowired
	private SocketCenter socketCenter;

	@Value("#{nodeSystemInfo['version']}")
	private String version;

	@Autowired
	private FNEmailSender mailSender;

	@Autowired
	NodeMonitor nodeMonitor;

	private String startConfigPath;

	public Run() {

	}

	public Run(String startConfigPath) {
		this.startConfigPath = startConfigPath;
	}

	public void init(boolean initInstance) {
		GlobalParam.run_environment = String.valueOf(GlobalParam.StartConfig.get("run_environment"));
		GlobalParam.mailSender = mailSender;
		GlobalParam.tasks = new HashMap<String, FlowTask>();
		GlobalParam.SOCKET_CENTER = socketCenter;
		GlobalParam.FlOW_CENTER = flowCenter;
		GlobalParam.VERSION = version;
		GlobalParam.nodeMonitor = nodeMonitor;
		GlobalParam.POOL_SIZE = Integer.parseInt(GlobalParam.StartConfig.getProperty("pool_size"));
		GlobalParam.WRITE_BATCH = GlobalParam.StartConfig.getProperty("write_batch").equals("false") ? false : true;
		GlobalParam.SERVICE_LEVEL = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if (initInstance) {
			ZKUtil.setData(GlobalParam.CONFIG_PATH + "/RIVER_NODES/" + GlobalParam.IP + "/configs",
					JSON.toJSONString(GlobalParam.StartConfig));
			GlobalParam.nodeConfig = NodeConfig.getInstance(GlobalParam.StartConfig.getProperty("instances"),
					GlobalParam.StartConfig.getProperty("pond"), GlobalParam.StartConfig.getProperty("instructions"));
			GlobalParam.nodeConfig.init();
			Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
			for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
				InstanceConfig instanceConfig = entry.getValue();
				if (instanceConfig.checkStatus())
					NodeUtil.initParams(instanceConfig);
			}
		}
	}

	public void startService() {
		if ((GlobalParam.SERVICE_LEVEL & 1) > 0) 
			searcherService.start(); 
		if ((GlobalParam.SERVICE_LEVEL & 2) > 0)
			GlobalParam.FlOW_CENTER.buildRWFlow();
		if ((GlobalParam.SERVICE_LEVEL & 4) > 0)
			httpReaderService.start();
		if ((GlobalParam.SERVICE_LEVEL & 16) > 0)
			computerService.start(); 
		if ((GlobalParam.SERVICE_LEVEL & 8) > 0)
			GlobalParam.FlOW_CENTER.startInstructionsJob();
		new FNMonitor().start();
	}

	public void loadGlobalConfig(String path, boolean fromZk) {
		try {
			GlobalParam.StartConfig = new Properties();
			if (fromZk) {
				JSONObject _JO = (JSONObject) JSON.parse(ZKUtil.getData(path, false));
				for (Map.Entry<String, Object> row : _JO.entrySet()) {
					GlobalParam.StartConfig.setProperty(row.getKey(), String.valueOf(row.getValue()));
				}
			} else {
				String replaceStr = System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") == -1
						? "file:"
						: "file:/";
				try (FileInputStream in = new FileInputStream(path.replace(replaceStr, ""))) {
					GlobalParam.StartConfig.load(in);
				} catch (Exception e) {
					Common.LOG.error("load Global Properties file Exception", e);
				}
			}
		} catch (Exception e) {
			Common.LOG.error("load Global Properties Config Exception", e);
		}
		GlobalParam.CONFIG_PATH = GlobalParam.StartConfig.getProperty("zkConfigPath");
		GlobalParam.INSTANCE_PATH = (GlobalParam.CONFIG_PATH+"/INSTANCES").intern();
		ZKUtil.setZkHost(GlobalParam.StartConfig.getProperty("zkhost"));
	}

	private void start() {
		loadGlobalConfig(this.startConfigPath, false);
		ReportStatus.nodeConfigs();
		if (!GlobalParam.StartConfig.containsKey("node_type"))
			GlobalParam.StartConfig.setProperty("node_type", NODE_TYPE.slave.name());
		if (GlobalParam.StartConfig.get("node_type").equals(NODE_TYPE.backup.name())) {
			init(false);
			recoverMonitor.start();
		} else {
			init(true);
			startService();
		}
		Common.LOG.info("River Start Success!");
	} 

	public static void main(String[] args) throws Exception {
		GlobalParam.RIVERS = (Run) FNIoc.getBean("RIVERS");
		GlobalParam.RIVERS.start();
	}

}