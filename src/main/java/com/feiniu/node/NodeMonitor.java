package com.feiniu.node;
 
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Resource;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.mortbay.jetty.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.NodeConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.task.TaskManager;
import com.feiniu.util.Common;
import com.feiniu.util.MD5Util;
import com.feiniu.util.SystemInfoUtil;
import com.feiniu.util.ZKUtil;

/**
 * data-flow router maintain apis
 * 
 * @author chengwen
 * @version 1.0
 */
@Component
public class NodeMonitor {

	@Autowired
	private NodeCenter nodeCenter;

	@Autowired
	private TaskManager taskManager;

	@Autowired
	private SearcherService SearcherService;

	@Autowired
	private HttpReaderService HttpReaderService;

	@Resource(name = "globalConfigBean")
	private Properties globalConfigBean;

	@Value("#{configPathConfig['config.path']}")
	private String configPath;

	private String response;

	private SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private HashSet<String> actions = new HashSet<String>() {
		private static final long serialVersionUID = -8313429841889556616L;
		{
			add("reloadConfig");
			add("getNodeConfig");
			add("setNodeConfig");
			add("stopInstance");
			add("removeInstance");
			add("deleteInstanceData");
			add("resumeInstance");
			add("getInstanceInfo");
			add("getInstances");
			add("getStatus");
			add("runNow");
			add("startSearcherService");
			add("stopSearcherService");
			add("startHttpReaderServiceService");
			add("stopHttpReaderServiceService");
		}
	};

	private final static Logger log = LoggerFactory
			.getLogger(NodeMonitor.class);

	public String getResponse() {
		return this.response;
	}

	/**
	 * 
	 * @param status
	 *            0 faild 1 success
	 * @param info
	 *            response information
	 */
	public void setResponse(int status, Object info) {
		HashMap<String, Object> rs = new HashMap<String, Object>();
		rs.put("status", status);
		rs.put("info", info);
		this.response = JSON.toJSONString(rs);
	}

	public void ac(Request rq) {
		try {
			if (this.actions.contains(rq.getParameter("ac"))) {
				Method m = NodeMonitor.class.getMethod(rq.getParameter("ac"),
						Request.class);
				m.invoke(this, rq);
			} else {
				setResponse(0, "Actions Not Exists!");
			}
		} catch (Exception e) {
			setResponse(0, "Actions Exception!");
			log.error("ac " + rq.getParameter("ac") + "Exception ", e);
		}
	}

	public void getNodeConfig(Request rq) {
		setResponse(1, globalConfigBean);
	}

	/**
	 * @param type
	 *            set or remove
	 */
	public void setNodeConfig(Request rq) {
		if (rq.getParameter("k") != null && rq.getParameter("v") != null
				&& rq.getParameter("type") != null) {
			if (rq.getParameter("type").equals("set")) {
				globalConfigBean.setProperty(rq.getParameter("k"),
						rq.getParameter("v"));
			} else {
				globalConfigBean.remove(rq.getParameter("k"));
			}
			OutputStream os = null;
			try {
				os = new FileOutputStream(configPath.replace("file:/", "")+"/config/config.properties");
				globalConfigBean.store(os, "Auto Save Config with no format,BeCarefull!");
				setResponse(1, "Config set success!");
			} catch (Exception e) {
				setResponse(0, "Config save Exception "+e.getMessage());
			} 
		} else {
			setResponse(0, "Config parameters k v or type not exists!");
		}
	}

	public void stopHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get(
				"service_level").toString());
		if ((service_level & 4) > 0) {
			service_level -= 4;
		}
		if (HttpReaderService.close()) {
			setResponse(0, "Stop Searcher Service Successed!");
		} else {
			setResponse(1, "Stop Searcher Service Failed!");
		}
	}

	public void startHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get(
				"service_level").toString());
		if ((service_level & 4) == 0) {
			service_level += 4;
			HttpReaderService.start();
		}
		setResponse(0, "Start Searcher Service Successed!");
	}

	public void stopSearcherService(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get(
				"service_level").toString());
		if ((service_level & 1) > 0) {
			service_level -= 1;
		}
		if (SearcherService.close()) {
			setResponse(0, "Stop Searcher Service Successed!");
		} else {
			setResponse(1, "Stop Searcher Service Failed!");
		}
	}

	public void startSearcherService(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get(
				"service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			SearcherService.start();
		}
		setResponse(0, "Start Searcher Service Successed!");
	}

	public void getStatus(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get(
				"service_level").toString());
		HashMap<String, Object> dt = new HashMap<String, Object>();
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("SERVICE_LEVEL", service_level);
		dt.put("STATUS", "running");
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("TASKS", GlobalParam.tasks.size());
		try {
			dt.put("CPU", SystemInfoUtil.getCpuUsage());
			dt.put("MEMORY", SystemInfoUtil.getMemUsage());
		} catch (Exception e) {
			log.error(" getStatus Exception ", e);
		}
		setResponse(1, dt);
	}

	public void getIndexInfo(Request rq) {
		if (GlobalParam.nodeTreeConfigs.getConfigMap().containsKey(
				rq.getParameter("instance"))) {
			StringBuffer sb = new StringBuffer();
			NodeConfig config = GlobalParam.nodeTreeConfigs.getConfigMap().get(
					rq.getParameter("instance"));
			boolean writer = false;
			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(
					config.getTransParam().getDataFrom()) != null) {
				String poolname = GlobalParam.nodeTreeConfigs
						.getNoSqlParamMap()
						.get(config.getTransParam().getDataFrom())
						.getPoolName(null);
				sb.append(",[DataFrom Pool Status] "
						+ FnConnectionPool.getStatus(poolname));
				writer = true;
			} else if (GlobalParam.nodeTreeConfigs.getSqlParamMap().get(
					config.getTransParam().getDataFrom()) != null) {
				WarehouseSqlParam ws = GlobalParam.nodeTreeConfigs
						.getSqlParamMap().get(
								config.getTransParam().getDataFrom());
				String poolname = "";
				if (ws.getSeq() != null && ws.getSeq().size() > 0) {
					for (String seq : ws.getSeq()) {
						poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap()
								.get(config.getTransParam().getDataFrom())
								.getPoolName(seq);
						sb.append(",[" + seq + " of DataFroms Pool Status] "
								+ FnConnectionPool.getStatus(poolname));
					}
				} else {
					poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap()
							.get(config.getTransParam().getDataFrom())
							.getPoolName(null);
					sb.append(",[DataFrom Pool Status] "
							+ FnConnectionPool.getStatus(poolname));
				}

				writer = true;
			}

			if (writer) {
				WarehouseSqlParam tmpDBParam = GlobalParam.nodeTreeConfigs
						.getSqlParamMap().get(
								config.getTransParam().getDataFrom());
				if (tmpDBParam.getSeq().size() > 0) {
					sb.append(",[当前存储状态]");
					for (String seriesDataSeq : tmpDBParam.getSeq()) {
						String strs = getZkData(Common.getTaskStorePath(
								rq.getParameter("instance"), seriesDataSeq));
						sb.append("\r\n;(" + seriesDataSeq + ") "
								+ strs.split(":")[0] + ":");
						for (String str : strs.split(":")[1].split(",")) {
							String update;
							if (str.length() > 9) {
								update = this.SDF
										.format(str.length() < 12 ? new Long(
												str + "000") : new Long(str));
							} else {
								update = str;
							}
							sb.append(", ");
							sb.append(update);
						}
					}
				} else {
					String strs = getZkData(Common.getTaskStorePath(
							rq.getParameter("instance"), null));
					String update;
					String time = strs.split(":")[1];
					if (time.length() > 9) {
						update = this.SDF.format(time.length() < 12 ? new Long(
								time + "000") : new Long(time));
					} else {
						update = time;
					}
					sb.append(",[当前存储状态] " + strs.split(":")[0] + ":" + update);
				}
				if (!GlobalParam.FLOW_INFOS.containsKey(rq
						.getParameter("instance") + "_full")
						|| GlobalParam.FLOW_INFOS.get(
								rq.getParameter("instance") + "_full").size() == 0) {
					sb.append(",[全量状态] " + "full:null");
				} else {
					sb.append(",[全量状态] "
							+ "full:"
							+ GlobalParam.FLOW_INFOS.get(rq
									.getParameter("instance") + "_full"));
				}
				if (!GlobalParam.FLOW_INFOS.containsKey(rq
						.getParameter("instance") + "_increment")
						|| GlobalParam.FLOW_INFOS.get(
								rq.getParameter("instance") + "_increment")
								.size() == 0) {
					sb.append(",[增量状态] " + "increment:null");
				} else {
					sb.append(",[增量状态] "
							+ "increment:"
							+ GlobalParam.FLOW_INFOS.get(rq
									.getParameter("instance") + "_increment"));
				}
			}

			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(
					config.getTransParam().getWriteTo()) != null) {
				String poolname = GlobalParam.nodeTreeConfigs
						.getNoSqlParamMap()
						.get(config.getTransParam().getWriteTo())
						.getPoolName(null);
				sb.append(",[WriteTo Pool Status] "
						+ FnConnectionPool.getStatus(poolname));
			} else if (GlobalParam.nodeTreeConfigs.getSqlParamMap().get(
					config.getTransParam().getWriteTo()) != null) {
				String poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap()
						.get(config.getTransParam().getWriteTo())
						.getPoolName(null);
				sb.append(",[WriteTo Pool Status] "
						+ FnConnectionPool.getStatus(poolname));
			}

			String destination = GlobalParam.nodeTreeConfigs.getSearchConfigs()
					.get(config.getAlias()).getTransParam().getSearcher();
			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(destination) != null) {
				String poolname = GlobalParam.nodeTreeConfigs
						.getNoSqlParamMap().get(destination).getPoolName(null);
				sb.append(",[Searcher Pool Status] "
						+ FnConnectionPool.getStatus(poolname));
			} else {
				String poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap()
						.get(destination).getPoolName(null);
				sb.append(",[Searcher Pool Status] "
						+ FnConnectionPool.getStatus(poolname));
			}

			setResponse(1, sb.toString());
		}
	}

	public void getInstances(Request rq) {
		Map<String, NodeConfig> configMap = GlobalParam.nodeTreeConfigs
				.getConfigMap();

		HashMap<String, List<String>> rs = new HashMap<String, List<String>>();
		for (Map.Entry<String, NodeConfig> entry : configMap.entrySet()) {
			NodeConfig config = entry.getValue();
			StringBuffer sb = new StringBuffer();
			sb.append(entry.getKey() + ":[Alias]" + config.getAlias());
			sb.append(":[OptimizeCron]" + config.getOptimizeCron());
			sb.append(":[DeltaCron]" + config.getTransParam().getDeltaCron());
			sb.append(":[FullCron]" + config.getTransParam().getFullCron());
			sb.append(":[Searcher]" + config.getTransParam().getSearcher());
			sb.append(":[Indexer]" + config.isIndexer());
			sb.append(":[Kafka]" + config.hasKafka() + ":[Rabitmq]"
					+ config.hasRabitmq());
			sb.append(":[WriteTo]" + config.getTransParam().getWriteTo());
			sb.append(":[DataFrom]" + config.getTransParam().getDataFrom());
			sb.append(":[IndexType]" + config.getIndexType() + ",");
			if (rs.containsKey(config.getAlias())) {
				rs.get(config.getAlias()).add(sb.toString());
				rs.put(config.getAlias(), rs.get(config.getAlias()));
			} else {
				ArrayList<String> tmp = new ArrayList<String>();
				tmp.add(sb.toString());
				rs.put(config.getAlias(), tmp);
			}
		}
		setResponse(1, JSON.toJSONString(rs));
	}

	public void runNow(Request rq) {
		if (rq.getParameter("instance") != null
				&& rq.getParameter("jobtype") != null) {
			if (GlobalParam.nodeTreeConfigs.getConfigMap().containsKey(
					rq.getParameter("instance"))
					&& GlobalParam.nodeTreeConfigs.getConfigMap()
							.get(rq.getParameter("instance")).isIndexer()) {
				boolean state = this.taskManager.runIndexJobNow(
						rq.getParameter("instance"),
						GlobalParam.nodeTreeConfigs.getConfigMap().get(
								rq.getParameter("instance")),
						rq.getParameter("jobtype"));
				if (state) {
					setResponse(1, "Writer " + rq.getParameter("instance")
							+ " job has been started now!");
				} else {
					setResponse(0, "Writer " + rq.getParameter("instance")
							+ " job not exists or run failed!");
				}
			} else {
				setResponse(0, "Writer " + rq.getParameter("instance")
						+ " job not open in this node!Run start faild!");
			}
		} else {
			setResponse(
					0,
					"Writer "
							+ rq.getParameter("instance")
							+ " job started now error,instance and jobtype parameter not both set!");
		}
	}

	public void removeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 0);
			this.taskManager.removeInstance(rq.getParameter("instance"));
			setResponse(1, "Writer " + rq.getParameter("instance")
					+ " job have removed!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance")
					+ " remove error,instance parameter not set!");
		}
	}

	public void stopInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 0);
			setResponse(1, "Writer " + rq.getParameter("instance")
					+ " job have stopped!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance")
					+ " stop error,index parameter not set!");
		}
	}

	public void resumeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 1);
			setResponse(1, "Writer " + rq.getParameter("instance")
					+ " job have resumed!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance")
					+ " resume error,index parameter not set!");
		}
	}

	public void reloadConfig(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 0);
			if (rq.getParameter("reset") != null
					&& rq.getParameter("reset").equals("true")
					&& rq.getParameter("instance").length() > 2) {
				GlobalParam.nodeTreeConfigs.loadConfig(
						rq.getParameter("instance"), true, true);
			} else {
				GlobalParam.nodeTreeConfigs.loadConfig(
						rq.getParameter("instance"), false, true);
			}
			startIndex(rq.getParameter("instance"));
			currentThreadState(rq.getParameter("instance"), 1);
			setResponse(1, "Writer " + rq.getParameter("instance")
					+ " reload Config Success!");
		} else {
			setResponse(0, "Index " + rq.getParameter("instance")
					+ " stop error,index parameter not set!");
		}
	}

	/**
	 * delete Instance Data through alias or Instance data name
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq) {
		if (!rq.getParameter("passcode").equals(
				MD5Util.MD5(rq.getParameter("alias")))) {
			setResponse(0, "passcode not correct!");
			return;
		}
		String alias = rq.getParameter("alias");
		Map<String, NodeConfig> configMap = GlobalParam.nodeTreeConfigs
				.getConfigMap();
		boolean state = true;
		for (Map.Entry<String, NodeConfig> ents : configMap.entrySet()) {
			String indexname = ents.getKey();
			NodeConfig nodeConfig = ents.getValue();
			if (indexname.equals(alias) || nodeConfig.getAlias().equals(alias)) {
				GlobalParam.LAST_UPDATE_TIME.put(indexname, "0");
				GlobalParam.FLOW_STATUS.get(indexname).set(0);
				WarehouseNosqlParam param = GlobalParam.nodeTreeConfigs
						.getNoSqlParamMap().get(
								nodeConfig.getTransParam().getWriteTo());
				state = state && deleteAction(param, nodeConfig, indexname);
				GlobalParam.FLOW_STATUS.get(indexname).set(1);
			}
		}
		if (state) {
			setResponse(1, "delete writer " + alias + " Success!");
		} else {
			setResponse(0, "delete writer " + alias + " Failed!");
		}
	}

	public void ESIndexDelete(String indexname, WarehouseNosqlParam param,
			NodeConfig nodeConfig) {

		WarehouseSqlParam tmpDBParam = GlobalParam.nodeTreeConfigs
				.getSqlParamMap().get(nodeConfig.getTransParam().getDataFrom());
		HashMap<String, Object> connectParams = new HashMap<String, Object>();
		connectParams.put("alias", param.getAlias());
		connectParams.put("defaultValue", param.getDefaultValue());
		connectParams.put("ip", param.getIp());
		connectParams.put("name", param.getName());
		connectParams.put("type", param.getType());
		connectParams.put("poolName", param.getPoolName(null));
		FnConnection<?> FC = FnConnectionPool.getConn(connectParams,
				param.getIp() + "_" + param.getDefaultValue(), false);
		Client es = (Client) FC.getConnection();
		try {
			DeleteIndexRequest deleteRequest;
			if (tmpDBParam.getSeq().size() > 0) {
				for (String dbseq : tmpDBParam.getSeq()) {
					String batchid = getBatchId(GlobalParam.CONFIG_PATH + "/"
							+ indexname + "/" + dbseq + "/" + "batch");
					if (batchid == null) {
						setResponse(0,
								"Delete action failed,for no storeid get from store position!");
						break;
					}
					deleteRequest = new DeleteIndexRequest(indexname + dbseq
							+ "_" + batchid);
					DeleteIndexResponse deleteResponse = es.admin().indices()
							.delete(deleteRequest).actionGet();
					if (deleteResponse.isAcknowledged()) {
						resetZk(Common.getTaskStorePath(indexname, dbseq));
						log.info("index " + indexname + dbseq + "_" + batchid
								+ " removed ");
					}
				}
			} else {
				deleteRequest = new DeleteIndexRequest(indexname);
				DeleteIndexResponse deleteResponse = es.admin().indices()
						.delete(deleteRequest).actionGet();
				if (deleteResponse.isAcknowledged()) {
					resetZk(Common.getTaskStorePath(indexname, null));
					log.info("index " + indexname + " removed ");
				}
			}
		} catch (Exception e) {
			setResponse(0, "ESIndex Delete Exception!");
		} finally {
			FnConnectionPool.freeConn(FC,
					param.getIp() + "_" + param.getDefaultValue());
		}
	}

	public void SOLRIndexDelete(String indexname, WarehouseNosqlParam param,
			NodeConfig nodeConfig) {

	}

	public void HBASEIndexDelete(String indexname, WarehouseNosqlParam param,
			NodeConfig nodeConfig) {

	}

	/**
	 * control current run thread,prevent error data write
	 * 
	 * @param index
	 * @param state
	 */
	private void currentThreadState(String index, Integer state) {
		for (String inst : index.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			if (state == 0) {
				int waittime = 0;
				log.info("Index " + strs[0] + " waitting to stop...");
				while ((GlobalParam.FLOW_STATUS.get(strs[0]).get() & 2) > 0) {
					try {
						waittime++;
						Thread.sleep(3000);
						if (waittime > 5) {
							GlobalParam.FLOW_STATUS.get(strs[0]).set(4);
						}
					} catch (InterruptedException e) {
						log.error("currentThreadState InterruptedException", e);
					}
				}
				log.info("Index " + strs[0] + " success stop!");
			}
			GlobalParam.FLOW_STATUS.get(strs[0]).set(state);
		}
	}

	private void startIndex(String index) {
		for (String inst : index.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			this.taskManager.startInstance(strs[0], GlobalParam.nodeTreeConfigs
					.getConfigMap().get(strs[0]), true);
		}
	}

	private boolean deleteAction(WarehouseNosqlParam param,
			NodeConfig nodeConfig, String indexname) {
		if (param.getType() == DATA_TYPE.ES
				|| param.getType() == DATA_TYPE.SOLR
				|| param.getType() == DATA_TYPE.HBASE) {
			try {
				Method m = NodeMonitor.class.getMethod(param.getType().name()
						+ "IndexDelete", String.class,
						WarehouseNosqlParam.class, NodeConfig.class);
				m.invoke(this, indexname, param, nodeConfig);
				return true;
			} catch (Exception e) {
				log.error("deleteAction Exception ", e);
			}
		}
		return false;
	}

	private void resetZk(String path) {
		ZKUtil.setData(path, "");
	}

	private String getZkData(String path) {
		String str = null;
		byte[] b = ZKUtil.getData(path);
		if (b != null && b.length > 0) {
			str = new String(b);
		}
		return str;
	}

	private String getBatchId(String path) {
		String[] strs = getZkData(path).split(":");
		if (strs.length > 0) {
			return strs[0];
		}
		return null;
	}

}
