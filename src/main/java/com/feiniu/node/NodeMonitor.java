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

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.mortbay.jetty.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.DATA_TYPE;
import com.feiniu.config.GlobalParam.JOB_TYPE;
import com.feiniu.config.NodeConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.task.TaskManager;
import com.feiniu.util.Common;
import com.feiniu.util.MD5Util;
import com.feiniu.util.SystemInfoUtil;
import com.feiniu.util.ZKUtil;

/**
 * data-flow router maintain apis,default port
 * 8617,localhost:8617/search.doaction?ac=[actions]
 * 
 * @actions reloadConfig reload instance config and re-add all jobs clean relate
 *          pools
 * @actions runNow/stopInstance/removeInstance/resumeInstance
 *          start/stop/remove/resume once now instance
 * @actions getInstances get all instances in current node
 * @actions getInstanceInfo get specify instance detail informations
 * @author chengwen
 * @version 1.0
 */
@Component
public final class NodeMonitor {

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
			add("resetInstanceState");
			add("getInstanceSeqs");
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
				Method m = NodeMonitor.class.getMethod(rq.getParameter("ac"), Request.class);
				m.invoke(this, rq);
			} else {
				setResponse(0, "Actions Not Exists!");
			}
		} catch (Exception e) {
			setResponse(0, "Actions Exception!");
			GlobalParam.LOG.error("ac " + rq.getParameter("ac") + " Exception ", e); 
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
		if (rq.getParameter("k") != null && rq.getParameter("v") != null && rq.getParameter("type") != null) {
			if (rq.getParameter("type").equals("set")) {
				globalConfigBean.setProperty(rq.getParameter("k"), rq.getParameter("v"));
			} else {
				globalConfigBean.remove(rq.getParameter("k"));
			}
			OutputStream os = null;
			try {
				os = new FileOutputStream(configPath.replace("file:", "") + "/config/config.properties");
				globalConfigBean.store(os, "Auto Save Config with no format,BeCarefull!");
				setResponse(1, "Config set success!");
			} catch (Exception e) {
				setResponse(0, "Config save Exception " + e.getMessage());
			}
		} else {
			setResponse(0, "Config parameters k v or type not exists!");
		}
	}

	public void stopHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get("service_level").toString());
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
		int service_level = Integer.parseInt(globalConfigBean.get("service_level").toString());
		if ((service_level & 4) == 0) {
			service_level += 4;
			HttpReaderService.start();
		}
		setResponse(0, "Start Searcher Service Successed!");
	}

	public void stopSearcherService(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get("service_level").toString());
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
		int service_level = Integer.parseInt(globalConfigBean.get("service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			SearcherService.start();
		}
		setResponse(0, "Start Searcher Service Successed!");
	}

	public void getStatus(Request rq) {
		int service_level = Integer.parseInt(globalConfigBean.get("service_level").toString());
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
			GlobalParam.LOG.error(" getStatus Exception ", e);
		}
		setResponse(1, dt);
	}
	
	public void getInstanceSeqs(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			try {
				String instance = rq.getParameter("instance");
				NodeConfig nodeConfig = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instance); 
				WarehouseParam dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				if (dataMap == null) {
					dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				} 
				setResponse(1, StringUtils.join(dataMap.getSeq().toArray(), ","));
			}catch (Exception e) { 
				setResponse(0, rq.getParameter("instance") + " not exists!");
			} 
		}
	}
	
	public void resetInstanceState(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			try {
				String instance = rq.getParameter("instance");
				NodeConfig nodeConfig = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instance); 
				WarehouseParam dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				if (dataMap == null) {
					dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				}
				List<String> seqs = dataMap.getSeq();
				if (seqs.size() == 0) {
					seqs.add(GlobalParam.DEFAULT_RESOURCE_SEQ);
				}
				for (String seq : seqs) { 
					GlobalParam.LAST_UPDATE_TIME.set(instance,seq, "0");
					Common.saveTaskInfo(instance, seq, Common.getStoreId(instance, seq));
				}
				setResponse(1, rq.getParameter("instance") + " reset Success!");
			}catch (Exception e) { 
				setResponse(0, rq.getParameter("instance") + " not exists!");
			} 
		} else {
			setResponse(0, rq.getParameter("instance") + " not exists!");
		}
	}

	public void getInstanceInfo(Request rq) {
		if (GlobalParam.nodeTreeConfigs.getNodeConfigs().containsKey(rq.getParameter("instance"))) {
			StringBuffer sb = new StringBuffer();
			NodeConfig config = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(rq.getParameter("instance"));
			boolean writer = false;
			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(config.getPipeParam().getDataFrom()) != null) {
				String poolname = GlobalParam.nodeTreeConfigs.getNoSqlParamMap()
						.get(config.getPipeParam().getDataFrom()).getPoolName(null);
				sb.append(",[DataFrom Pool Status] " + FnConnectionPool.getStatus(poolname));
				writer = true;
			} else if (GlobalParam.nodeTreeConfigs.getSqlParamMap().get(config.getPipeParam().getDataFrom()) != null) {
				WarehouseSqlParam ws = GlobalParam.nodeTreeConfigs.getSqlParamMap()
						.get(config.getPipeParam().getDataFrom());
				String poolname = "";
				if (ws.getSeq() != null && ws.getSeq().size() > 0) {
					for (String seq : ws.getSeq()) {
						poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap()
								.get(config.getPipeParam().getDataFrom()).getPoolName(seq);
						sb.append(",[" + seq + " of DataFroms Pool Status] " + FnConnectionPool.getStatus(poolname));
					}
				} else {
					poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(config.getPipeParam().getDataFrom())
							.getPoolName(null);
					sb.append(",[DataFrom Pool Status] " + FnConnectionPool.getStatus(poolname));
				} 
				writer = true;
			}

			if (writer) {
				WarehouseSqlParam tmpDBParam = GlobalParam.nodeTreeConfigs.getSqlParamMap()
						.get(config.getPipeParam().getDataFrom());
				if (tmpDBParam.getSeq().size() > 0) {
					sb.append(",[当前存储状态]");
					for (String seriesDataSeq : tmpDBParam.getSeq()) {
						String strs = getZkData(Common.getTaskStorePath(rq.getParameter("instance"), seriesDataSeq));
						sb.append("\r\n;(" + seriesDataSeq + ") " + strs.split(GlobalParam.JOB_STATE_SPERATOR)[0] + ":");
						for (String str : strs.split(GlobalParam.JOB_STATE_SPERATOR)[1].split(",")) {
							String update;
							if (str.length() > 9 && str.matches("[0-9]+")) {
								update = this.SDF.format(str.length() < 12 ? new Long(str + "000") : new Long(str));
							} else {
								update = str;
							}
							sb.append(", ");
							sb.append(update);
						}
					}
				} else {
					String strs = getZkData(Common.getTaskStorePath(rq.getParameter("instance"), null));
					StringBuffer stateStr = new StringBuffer();
					String time = strs.split(GlobalParam.JOB_STATE_SPERATOR)[1];
					for (String tm : strs.split(GlobalParam.JOB_STATE_SPERATOR)[1].split(",")) {
						if (tm.length() > 9 && tm.matches("[0-9]+")) {
							stateStr.append(this.SDF.format(tm.length() < 12 ? new Long(time + "000") : new Long(tm)));
						} else {
							stateStr.append(tm);
						}
						stateStr.append(", ");
					}

					sb.append(",[当前存储状态] " + strs.split(GlobalParam.JOB_STATE_SPERATOR)[0] + ":" + stateStr.toString());
				}
				if (!GlobalParam.FLOW_INFOS.containsKey(rq.getParameter("instance"),JOB_TYPE.FULL.name())
						|| GlobalParam.FLOW_INFOS.get(rq.getParameter("instance"),JOB_TYPE.FULL.name()).size() == 0) {
					sb.append(",[全量状态] " + "full:null");
				} else {
					sb.append(",[全量状态] " + "full:" + GlobalParam.FLOW_INFOS.get(rq.getParameter("instance"),JOB_TYPE.FULL.name()));
				}
				if (!GlobalParam.FLOW_INFOS.containsKey(rq.getParameter("instance"),JOB_TYPE.INCREMENT.name())
						|| GlobalParam.FLOW_INFOS.get(rq.getParameter("instance"),JOB_TYPE.INCREMENT.name()).size() == 0) {
					sb.append(",[增量状态] " + "increment:null");
				} else {
					sb.append(",[增量状态] " + "increment:"
							+ GlobalParam.FLOW_INFOS.get(rq.getParameter("instance"),JOB_TYPE.INCREMENT.name()));
				}
			}

			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(config.getPipeParam().getWriteTo()) != null) {
				String poolname = GlobalParam.nodeTreeConfigs.getNoSqlParamMap()
						.get(config.getPipeParam().getWriteTo()).getPoolName(null);
				sb.append(",[WriteTo Pool Status] " + FnConnectionPool.getStatus(poolname));
			} else if (GlobalParam.nodeTreeConfigs.getSqlParamMap().get(config.getPipeParam().getWriteTo()) != null) {
				String poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(config.getPipeParam().getWriteTo())
						.getPoolName(null);
				sb.append(",[WriteTo Pool Status] " + FnConnectionPool.getStatus(poolname));
			}

			String destination = GlobalParam.nodeTreeConfigs.getSearchConfigs().get(config.getAlias()).getPipeParam()
					.getSearcher();
			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(destination) != null) {
				String poolname = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(destination).getPoolName(null);
				sb.append(",[Searcher Pool Status] " + FnConnectionPool.getStatus(poolname));
			} else {
				String poolname = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(destination).getPoolName(null);
				sb.append(",[Searcher Pool Status] " + FnConnectionPool.getStatus(poolname));
			}

			setResponse(1, sb.toString());
		}
	}

	/**
	 * get all instances info
	 * 
	 * @param rq
	 */
	public void getInstances(Request rq) {
		Map<String, NodeConfig> nodes = GlobalParam.nodeTreeConfigs.getNodeConfigs();
		HashMap<String, List<String>> rs = new HashMap<String, List<String>>();
		for (Map.Entry<String, NodeConfig> entry : nodes.entrySet()) {
			NodeConfig config = entry.getValue();
			StringBuffer sb = new StringBuffer();
			sb.append(entry.getKey() + ":[Alias]" + config.getAlias());
			sb.append(":[OptimizeCron]" + config.getPipeParam().getOptimizeCron());
			sb.append(":[DeltaCron]" + config.getPipeParam().getDeltaCron());
			sb.append(":[FullCron]" + config.getPipeParam().getFullCron());
			sb.append(":[Searcher]" + config.getPipeParam().getSearcher());
			sb.append(":[Indexer]" + config.isIndexer());
			sb.append(":[Kafka]" + config.hasKafka() + ":[Rabitmq]" + config.hasRabitmq());
			sb.append(":[WriteTo]" + config.getPipeParam().getWriteTo());
			sb.append(":[DataFrom]" + config.getPipeParam().getDataFrom());
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
		if (rq.getParameter("instance") != null && rq.getParameter("jobtype") != null) {
			if (GlobalParam.nodeTreeConfigs.getNodeConfigs().containsKey(rq.getParameter("instance"))
					&& GlobalParam.nodeTreeConfigs.getNodeConfigs().get(rq.getParameter("instance")).isIndexer()) {
				boolean state = this.taskManager.runIndexJobNow(rq.getParameter("instance"),
						GlobalParam.nodeTreeConfigs.getNodeConfigs().get(rq.getParameter("instance")),
						rq.getParameter("jobtype"));
				if (state) {
					setResponse(1, "Writer " + rq.getParameter("instance") + " job has been started now!");
				} else {
					setResponse(0, "Writer " + rq.getParameter("instance")
							+ " job not exists or run failed or had been stated!");
				}
			} else {
				setResponse(0, "Writer " + rq.getParameter("instance") + " job not open in this node!Run start faild!");
			}
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance")
					+ " job started now error,instance and jobtype parameter not both set!");
		}
	}

	public void removeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 0);
			this.taskManager.removeInstance(rq.getParameter("instance"));
			setResponse(1, "Writer " + rq.getParameter("instance") + " job have removed!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance") + " remove error,instance parameter not set!");
		}
	}

	public void stopInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 0);
			setResponse(1, "Writer " + rq.getParameter("instance") + " job have stopped!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance") + " stop error,index parameter not set!");
		}
	}

	public void resumeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 1);
			setResponse(1, "Writer " + rq.getParameter("instance") + " job have resumed!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance") + " resume error,index parameter not set!");
		}
	}

	public void reloadConfig(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			currentThreadState(rq.getParameter("instance"), 0);
			int type = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(rq.getParameter("instance")).getIndexType();
			String configString = type > 0 ? rq.getParameter("instance") + ":" + type : rq.getParameter("instance");
			if (rq.getParameter("reset") != null && rq.getParameter("reset").equals("true")
					&& rq.getParameter("instance").length() > 2) {
				GlobalParam.nodeTreeConfigs.loadConfig(configString, true);
			} else {
				GlobalParam.FLOW_INFOS.remove(rq.getParameter("instance"),JOB_TYPE.FULL.name());
				GlobalParam.FLOW_INFOS.remove(rq.getParameter("instance"),JOB_TYPE.INCREMENT.name());
				this.freeInstanceConnectPool(rq.getParameter("instance"));
				GlobalParam.nodeTreeConfigs.getSearchConfigs().remove(
						GlobalParam.nodeTreeConfigs.getNodeConfigs().get(rq.getParameter("instance")).getAlias());
				GlobalParam.nodeTreeConfigs.loadConfig(configString, false);
			}
			startIndex(configString);
			currentThreadState(rq.getParameter("instance"), 1);
			setResponse(1, rq.getParameter("instance") + " reload Config Success!");
		} else {
			setResponse(0, rq.getParameter("instance") + " not exists!");
		}
	}

	/**
	 * delete Instance Data through alias or Instance data name
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq) {
		if (!rq.getParameter("passcode").equals(MD5Util.MD5(rq.getParameter("alias")))) {
			setResponse(0, "passcode not correct!");
			return;
		}
		String alias = rq.getParameter("alias");
		Map<String, NodeConfig> configMap = GlobalParam.nodeTreeConfigs.getNodeConfigs();
		boolean state = true;
		for (Map.Entry<String, NodeConfig> ents : configMap.entrySet()) {
			String instance = ents.getKey();
			NodeConfig nodeConfig = ents.getValue();
			if (instance.equals(alias) || nodeConfig.getAlias().equals(alias)) {
				WarehouseParam dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				if (dataMap == null) {
					dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				}
				List<String> seqs = dataMap.getSeq();
				if (seqs.size() == 0) {
					seqs.add(GlobalParam.DEFAULT_RESOURCE_SEQ);
				}
				for (String seq : seqs) { 
					GlobalParam.LAST_UPDATE_TIME.set(instance,seq,"0");
					GlobalParam.FLOW_STATUS.get(instance,seq).set(0);
					WarehouseNosqlParam param = GlobalParam.nodeTreeConfigs.getNoSqlParamMap()
							.get(nodeConfig.getPipeParam().getWriteTo());
					state = state && deleteAction(param, nodeConfig, instance,seq);
					GlobalParam.FLOW_STATUS.get(instance,seq).set(1);
				}
			}
		}
		if (state) {
			setResponse(1, "delete " + alias + " Success!");
		} else {
			setResponse(0, "delete " + alias + " Failed!");
		}
	}

	public void ESIndexDelete(String indexname,String seq, WarehouseNosqlParam param, NodeConfig nodeConfig) {

		WarehouseSqlParam tmpDBParam = GlobalParam.nodeTreeConfigs.getSqlParamMap()
				.get(nodeConfig.getPipeParam().getDataFrom());
		HashMap<String, Object> connectParams = new HashMap<String, Object>();
		connectParams.put("alias", param.getAlias());
		connectParams.put("defaultValue", param.getDefaultValue());
		connectParams.put("ip", param.getIp());
		connectParams.put("name", param.getName());
		connectParams.put("type", param.getType());
		connectParams.put("poolName", param.getPoolName(null));
		FnConnection<?> FC = FnConnectionPool.getConn(connectParams, param.getIp() + "_" + param.getDefaultValue(),
				false);
		Client es = (Client) FC.getConnection(false);
		try {
			DeleteIndexRequest deleteRequest;
			if (tmpDBParam.getSeq().size() > 0) {
				String batchid = getBatchId(
						GlobalParam.CONFIG_PATH + "/" + indexname + "/" + seq + "/" + "batch");
				if (batchid == null) {
					setResponse(0, "Delete action failed,for no storeid get from store position!");
					return;
				}
				deleteRequest = new DeleteIndexRequest(indexname + seq + "_" + batchid);
				DeleteIndexResponse deleteResponse = es.admin().indices().delete(deleteRequest).actionGet();
				if (deleteResponse.isAcknowledged()) {
					resetZk(Common.getTaskStorePath(indexname, seq));
					GlobalParam.LOG.info("index " + indexname + seq + "_" + batchid + " removed ");
				}
			} else {
				deleteRequest = new DeleteIndexRequest(indexname);
				DeleteIndexResponse deleteResponse = es.admin().indices().delete(deleteRequest).actionGet();
				if (deleteResponse.isAcknowledged()) {
					resetZk(Common.getTaskStorePath(indexname, null));
					GlobalParam.LOG.info(indexname + " success removed!"); 
				}
			}
		} catch (Exception e) {
			setResponse(0, indexname + "Delete Exception!");
		} finally {
			FnConnectionPool.freeConn(FC, param.getIp() + "_" + param.getDefaultValue(), false);
		}
	}

	public void SOLRIndexDelete(String indexname, WarehouseNosqlParam param, NodeConfig nodeConfig) {

	}

	public void HBASEIndexDelete(String indexname, WarehouseNosqlParam param, NodeConfig nodeConfig) {

	}

	/**
	 * control current run thread,prevent error data write
	 * 
	 * @param index
	 * @param state
	 */
	private void currentThreadState(String instance, Integer state) {
		if((GlobalParam.SERVICE_LEVEL&6)==0) {
			return;
		}
		for (String inst : instance.split(",")) {
			if (state == 0) {
				int waittime = 0;
				GlobalParam.LOG.info("Instance " + inst + " waitting to stop...");
				NodeConfig nodeConfig = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instance); 
				WarehouseParam dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				if (dataMap == null) {
					dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(nodeConfig.getPipeParam().getDataFrom());
				}
				List<String> seqs = dataMap.getSeq();
				if (seqs.size() == 0) {
					seqs.add(GlobalParam.DEFAULT_RESOURCE_SEQ);
				} 
				for (String seq : seqs) { 
					while ((GlobalParam.FLOW_STATUS.get(inst,seq).get() & 2) > 0) {
						try {
							waittime++;
							Thread.sleep(3000);
							if (waittime > 5) {
								GlobalParam.FLOW_STATUS.get(inst,seq).set(4);
							}
						} catch (InterruptedException e) {
							GlobalParam.LOG.error("currentThreadState InterruptedException", e);
						}
					}
					GlobalParam.FLOW_STATUS.get(inst,seq).set(state);
					GlobalParam.LOG.info("Instance " + inst + " success stop!");
				}
			} 
		}
	}

	private void startIndex(String index) {
		for (String inst : index.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			this.taskManager.startInstance(strs[0], GlobalParam.nodeTreeConfigs.getNodeConfigs().get(strs[0]), true);
		}
	}

	private boolean deleteAction(WarehouseNosqlParam param, NodeConfig nodeConfig, String indexname, String seq) {
		if (param.getType() == DATA_TYPE.ES || param.getType() == DATA_TYPE.SOLR
				|| param.getType() == DATA_TYPE.HBASE) {
			try {
				Method m = NodeMonitor.class.getMethod(param.getType().name() + "IndexDelete", String.class,
						String.class,WarehouseNosqlParam.class, NodeConfig.class);
				m.invoke(this, indexname,seq, param, nodeConfig);
				return true;
			} catch (Exception e) {
				GlobalParam.LOG.error("deleteAction Exception ", e);
			}
		}
		return false;
	}

	private void resetZk(String path) {
		ZKUtil.setData(path, "");
	}

	private String getZkData(String path) {
		String str = null;
		byte[] b = ZKUtil.getData(path, false);
		if (b != null && b.length > 0) {
			str = new String(b);
		}
		return str;
	}

	private void freeInstanceConnectPool(String instanceName) {
		NodeConfig paramConfig = GlobalParam.nodeTreeConfigs.getNodeConfigs().get(instanceName);
		String[] dataSource = { paramConfig.getPipeParam().getDataFrom(), paramConfig.getPipeParam().getWriteTo() };
		for (String dt : dataSource) {
			if (GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(dt) != null) {
				WarehouseNosqlParam dataMap = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(dt);
				if (dataMap == null) {
					break;
				}
				List<String> seqs = dataMap.getSeq();
				if (seqs.size() > 0) {
					for (String seq : seqs) {
						FnConnectionPool.release(dataMap.getPoolName(seq));
					}
				} else {
					FnConnectionPool.release(dataMap.getPoolName(null));
				}
			} else if (GlobalParam.nodeTreeConfigs.getSqlParamMap().get(dt) != null) {
				WarehouseSqlParam dataMap = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(dt);
				if (dataMap == null) {
					break;
				}
				List<String> seqs = dataMap.getSeq();
				if (seqs.size() > 0) {
					for (String seq : seqs) {
						FnConnectionPool.release(dataMap.getPoolName(seq));
					}
				} else {
					FnConnectionPool.release(dataMap.getPoolName(null));
				}
			}
		}
	}

	private String getBatchId(String path) {
		String[] strs = getZkData(path).split(GlobalParam.JOB_STATE_SPERATOR);
		if (strs.length > 0) {
			return strs[0];
		}
		return null;
	}
 
}
