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
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.mortbay.jetty.Request;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.JOB_TYPE;
import com.feiniu.config.GlobalParam.RESOURCE_TYPE;
import com.feiniu.config.GlobalParam.STATUS;
import com.feiniu.config.InstanceConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.model.InstructionTree;
import com.feiniu.param.pipe.InstructionParam;
import com.feiniu.param.warehouse.WarehouseNosqlParam;
import com.feiniu.param.warehouse.WarehouseParam;
import com.feiniu.param.warehouse.WarehouseSqlParam;
import com.feiniu.reader.service.HttpReaderService;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.util.Common;
import com.feiniu.util.NodeUtil;
import com.feiniu.util.SystemInfoUtil;
import com.feiniu.util.ZKUtil;
 
/**
 *  * data-flow router maintain apis,default port
 * 8617,localhost:8617/search.doaction?ac=[actions]
 * 
 * @actions reloadConfig reload instance config and re-add all jobs clean relate
 *          pools
 * @actions runNow/stopInstance/removeInstance/resumeInstance
 *          start/stop/remove/resume once now instance
 * @actions getInstances get all instances in current node
 * @actions getInstanceInfo get specify instance detail informations
 * @author chengwen
 * @version 3.0
 * @date 2018-10-25 09:08
 */
@Component
public final class NodeMonitor {

	@Autowired
	private SearcherService SearcherService;

	@Autowired
	private HttpReaderService HttpReaderService;

	@Value("#{riverPathConfig['config.path']}")
	private String configPath;

	private String response;

	private SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private HashSet<String> actions = new HashSet<String>() {
		private static final long serialVersionUID = -8313429841889556616L;
		{
			// node manage
			add("addResource");
			add("getNodeConfig");
			add("setNodeConfig");
			add("getStatus");
			add("getInstances");
			add("startSearcherService");
			add("stopSearcherService");
			add("startHttpReaderServiceService");
			add("stopHttpReaderServiceService");
			// instance manage
			add("resetInstanceState");
			add("getInstanceSeqs");
			add("reloadConfig");
			add("runNow");
			add("addInstance");
			add("stopInstance");
			add("resumeInstance");
			add("removeInstance");
			add("deleteInstanceData");
			add("getInstanceInfo");
			add("runCode");
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
			setResponse(0, "Actions Exception!"+e.getMessage());
			Common.LOG.error("ac " + rq.getParameter("ac") + " Exception ", e);
		}
	}

	/**
	 * @param socket
	 *            resource configs json string
	 */
	public void addResource(Request rq) { 
		if (rq.getParameter("socket") != null && rq.getParameter("type")!=null) {
			JSONObject jsonObject = JSON.parseObject(rq.getParameter("socket"));
			RESOURCE_TYPE type = RESOURCE_TYPE.valueOf(rq.getParameter("type"));
			Object o = null; 
			Set<String> iter = jsonObject.keySet();
			try {
				switch (type) {
				case SQL:
					o = new WarehouseSqlParam();
					for (String key : iter) {
						Common.setConfigObj(o, WarehouseSqlParam.class, key, jsonObject.getString(key));
					}
					break;
				case NOSQL:
					o = new WarehouseNosqlParam();
					for (String key : iter) {
						Common.setConfigObj(o, WarehouseNosqlParam.class, key, jsonObject.getString(key));
					}
					break;
				case INSTRUCTION:
					o = new InstructionParam();
					for (String key : iter) {
						Common.setConfigObj(o, InstructionParam.class, key, jsonObject.getString(key));
					}
					break;
				}
				if (o != null) {
					GlobalParam.nodeConfig.addSource(type, o);
					setResponse(1, "add Resource to node success!");
				} 
			} catch (Exception e) {
				setResponse(0, "add Resource to node Exception " + e.getMessage());
			}
		} else {
			setResponse(0, "Parameter not match!");
		}
	}

	public void getNodeConfig(Request rq) {
		setResponse(1, GlobalParam.StartConfig);
	}

	/**
	 * @param k
	 *            property key
	 * @param v
	 *            property value
	 * @param type
	 *            action type,set/remove
	 */
	public void setNodeConfig(Request rq) {
		if (rq.getParameter("k") != null && rq.getParameter("v") != null && rq.getParameter("type") != null) {
			if (rq.getParameter("type").equals("set")) {
				GlobalParam.StartConfig.setProperty(rq.getParameter("k"), rq.getParameter("v"));
			} else {
				GlobalParam.StartConfig.remove(rq.getParameter("k"));
			} 
			try {
				saveNodeConfig();
				setResponse(1, "Config set success!");
			} catch (Exception e) {
				setResponse(0, "Config save Exception " + e.getMessage());
			}
		} else {
			setResponse(0, "Config parameters k v or type not exists!");
		}
	} 
	

	public void stopHttpReaderServiceService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
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
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 4) == 0) {
			service_level += 4;
			HttpReaderService.start();
		}
		setResponse(0, "Start Searcher Service Successed!");
	}

	public void stopSearcherService(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
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
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		if ((service_level & 1) == 0) {
			service_level += 1;
			SearcherService.start();
		}
		setResponse(0, "Start Searcher Service Successed!");
	}

	public void getStatus(Request rq) {
		int service_level = Integer.parseInt(GlobalParam.StartConfig.get("service_level").toString());
		HashMap<String, Object> dt = new HashMap<String, Object>();
		dt.put("NODE_TYPE", GlobalParam.StartConfig.getProperty("node_type"));
		dt.put("WRITE_BATCH", GlobalParam.WRITE_BATCH);
		dt.put("SERVICE_LEVEL", service_level);
		dt.put("STATUS", "running");
		dt.put("VERSION", GlobalParam.VERSION);
		dt.put("TASKS", GlobalParam.tasks.size());
		try {
			dt.put("CPU", SystemInfoUtil.getCpuUsage());
			dt.put("MEMORY", SystemInfoUtil.getMemUsage());
		} catch (Exception e) {
			Common.LOG.error(" getStatus Exception ", e);
		}
		setResponse(1, dt);
	}

	public void getInstanceSeqs(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			try {
				String instance = rq.getParameter("instance");
				InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(instance);
				WarehouseParam dataMap = GlobalParam.nodeConfig.getNoSqlWarehouse()
						.get(instanceConfig.getPipeParam().getReadFrom());
				if (dataMap == null) {
					dataMap = GlobalParam.nodeConfig.getSqlWarehouse().get(instanceConfig.getPipeParam().getReadFrom());
				}
				setResponse(1, StringUtils.join(dataMap.getSeq(), ","));
			} catch (Exception e) {
				setResponse(0, rq.getParameter("instance") + " not exists!");
			}
		} else {
			setResponse(0, "Parameter not match!");
		}
	}

	public void resetInstanceState(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			try {
				String instance = rq.getParameter("instance");
				String val = "0";
				if (rq.getParameterMap().get("set_value") != null)
					val = rq.getParameter("set_value");
				String[] seqs = getInstanceSeqs(instance);
				for (String seq : seqs) {
					GlobalParam.LAST_UPDATE_TIME.set(instance, seq, val);
					Common.saveTaskInfo(instance, seq, Common.getStoreId(instance, seq),
							GlobalParam.JOB_INCREMENTINFO_PATH);
				}
				setResponse(1, rq.getParameter("instance") + " reset Success!");
			} catch (Exception e) {
				setResponse(0, rq.getParameter("instance") + " not exists!");
			}
		} else {
			setResponse(0, "Parameter not match!");
		}
	}

	public void getInstanceInfo(Request rq) {
		if (GlobalParam.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance"))) {
			String instance = rq.getParameter("instance");
			StringBuffer sb = new StringBuffer();
			InstanceConfig config = GlobalParam.nodeConfig.getInstanceConfigs().get(instance);
			if (GlobalParam.nodeConfig.getNoSqlWarehouse().get(config.getPipeParam().getReadFrom()) != null) {
				String poolname = GlobalParam.nodeConfig.getNoSqlWarehouse().get(config.getPipeParam().getReadFrom())
						.getPoolName(null);
				sb.append(",[DataFrom Pool Status] " + FnConnectionPool.getStatus(poolname));
			} else if (GlobalParam.nodeConfig.getSqlWarehouse().get(config.getPipeParam().getReadFrom()) != null) {
				WarehouseSqlParam ws = GlobalParam.nodeConfig.getSqlWarehouse().get(config.getPipeParam().getReadFrom());
				String poolname = "";
				if (ws.getSeq() != null && ws.getSeq().length > 0) {
					for (String seq : ws.getSeq()) {
						poolname = GlobalParam.nodeConfig.getSqlWarehouse().get(config.getPipeParam().getReadFrom())
								.getPoolName(seq);
						sb.append(",[Seq(" + seq + ") Reader Pool Status] " + FnConnectionPool.getStatus(poolname));
					}
				} else {
					poolname = GlobalParam.nodeConfig.getSqlWarehouse().get(config.getPipeParam().getReadFrom())
							.getPoolName(null);
					sb.append(",[Reader Pool Status] " + FnConnectionPool.getStatus(poolname));
				}
			}

			if (GlobalParam.nodeConfig.getNoSqlWarehouse().get(config.getPipeParam().getWriteTo()) != null) {
				String poolname = GlobalParam.nodeConfig.getNoSqlWarehouse().get(config.getPipeParam().getWriteTo())
						.getPoolName(null);
				sb.append(",[Writer Pool Status] " + FnConnectionPool.getStatus(poolname));
			} else if (GlobalParam.nodeConfig.getSqlWarehouse().get(config.getPipeParam().getWriteTo()) != null) {
				String poolname = GlobalParam.nodeConfig.getSqlWarehouse().get(config.getPipeParam().getWriteTo())
						.getPoolName(null);
				sb.append(",[Writer Pool Status] " + FnConnectionPool.getStatus(poolname));
			}

			if ((GlobalParam.SERVICE_LEVEL & 1) > 0) {
				String searchFrom = config.getPipeParam().getSearchFrom();
				String searcher_info;
				if (config.getPipeParam().getWriteTo() != null && config.getPipeParam().getWriteTo().equals(searchFrom)) {
					searcher_info = ",[Searcher Pool (Share With Writer) Status] ";
				} else {
					searcher_info = ",[Searcher Pool Status] ";
				}

				if (GlobalParam.nodeConfig.getNoSqlWarehouse().get(searchFrom) != null) {
					String poolname = GlobalParam.nodeConfig.getNoSqlWarehouse().get(searchFrom).getPoolName(null);
					sb.append(searcher_info + FnConnectionPool.getStatus(poolname));
				} else {
					String poolname = GlobalParam.nodeConfig.getSqlWarehouse().get(searchFrom).getPoolName(null);
					sb.append(searcher_info + FnConnectionPool.getStatus(poolname));
				}
			}

			if (config.openTrans()) {
				WarehouseSqlParam wsp = GlobalParam.nodeConfig.getSqlWarehouse()
						.get(config.getPipeParam().getReadFrom());
				if (wsp.getSeq().length > 0) {
					sb.append(",[增量存储状态]");
					StringBuffer fullstate = new StringBuffer();
					for (String seriesDataSeq : wsp.getSeq()) {
						String strs = getZkData(
								Common.getTaskStorePath(instance, seriesDataSeq, GlobalParam.JOB_INCREMENTINFO_PATH));
						if(strs==null)
							continue;
						sb.append(
								"\r\n;(" + seriesDataSeq + ") " + strs.split(GlobalParam.JOB_STATE_SPERATOR)[0] + ":");
						if (strs.split(GlobalParam.JOB_STATE_SPERATOR).length == 1)
							continue;
						for (String str : strs.split(GlobalParam.JOB_STATE_SPERATOR)[1].split(",")) {
							String update;
							if (str.length() > 9 && str.matches("[0-9]+")) {
								update = (this.SDF.format(str.length() < 12 ? new Long(str + "000") : new Long(str)))
										+ " (" + str + ")";
							} else {
								update = str;
							}
							sb.append(", ");
							sb.append(update);
						}
						fullstate.append(seriesDataSeq + ":" + Common.getFullStartInfo(instance, seriesDataSeq) + "; ");
					}
					sb.append(",[全量存储状态]");
					sb.append(fullstate);

				} else {
					String strs = getZkData(
							Common.getTaskStorePath(instance, null, GlobalParam.JOB_INCREMENTINFO_PATH));
					StringBuffer stateStr = new StringBuffer();
					if (strs.split(GlobalParam.JOB_STATE_SPERATOR).length > 1) {
						for (String tm : strs.split(GlobalParam.JOB_STATE_SPERATOR)[1].split(",")) {
							if (tm.length() > 9 && tm.matches("[0-9]+")) {
								stateStr.append(
										this.SDF.format(tm.length() < 12 ? new Long(tm + "000") : new Long(tm)));
								stateStr.append(" (").append(tm).append(")");
							} else {
								stateStr.append(tm);
							}
							stateStr.append(", ");
						}
					}
					sb.append(",[增量存储状态] " + strs.split(GlobalParam.JOB_STATE_SPERATOR)[0] + ":" + stateStr.toString());
					sb.append(",[全量存储状态] ");
					sb.append(Common.getFullStartInfo(instance, null));
				}
				if (!GlobalParam.FLOW_INFOS.containsKey(instance, JOB_TYPE.FULL.name())
						|| GlobalParam.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()).size() == 0) {
					sb.append(",[全量运行状态] " + "full:null");
				} else {
					sb.append(",[全量运行状态] " + "full:" + GlobalParam.FLOW_INFOS.get(instance, JOB_TYPE.FULL.name()));
				}
				if (!GlobalParam.FLOW_INFOS.containsKey(instance, JOB_TYPE.INCREMENT.name())
						|| GlobalParam.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()).size() == 0) {
					sb.append(",[增量运行状态] " + "increment:null");
				} else {
					sb.append(",[增量运行状态] " + "increment:"
							+ GlobalParam.FLOW_INFOS.get(instance, JOB_TYPE.INCREMENT.name()));
				}
				sb.append(",[增量线程状态] ");
				sb.append(threadStateInfo(instance, GlobalParam.JOB_TYPE.INCREMENT.name()));
				sb.append(",[全量线程状态] ");
				sb.append(threadStateInfo(instance, GlobalParam.JOB_TYPE.FULL.name()));
			}
			setResponse(1, sb.toString());
		} else {
			setResponse(0, "instance not exits!");
		}
	}

	/**
	 * get all instances info
	 * 
	 * @param rq
	 */
	public void getInstances(Request rq) {
		Map<String, InstanceConfig> nodes = GlobalParam.nodeConfig.getInstanceConfigs();
		HashMap<String, List<String>> rs = new HashMap<String, List<String>>();
		for (Map.Entry<String, InstanceConfig> entry : nodes.entrySet()) {
			InstanceConfig config = entry.getValue();
			StringBuffer sb = new StringBuffer();
			sb.append(entry.getKey() + ":[Alias]" + config.getAlias());
			sb.append(":[OptimizeCron]" + config.getPipeParam().getOptimizeCron());
			sb.append(":[DeltaCron]" + config.getPipeParam().getDeltaCron());
			if (config.getPipeParam().getFullCron() == null && config.getPipeParam().getReadFrom() != null
					&& config.getPipeParam().getWriteTo() != null) {
				sb.append(":[FullCron] 0 0 0 1 1 ? 2099");
			} else {
				sb.append(":[FullCron]" + config.getPipeParam().getFullCron());
			}
			sb.append(":[SearchFrom]" + config.getPipeParam().getSearchFrom());
			sb.append(":[ReadFrom]" + config.getPipeParam().getReadFrom());
			sb.append(":[WriteTo]" + config.getPipeParam().getWriteTo());
			sb.append(":[openTrans]" + config.openTrans());
			sb.append(":[IsMaster]" + config.getPipeParam().isMaster() + ",");
			sb.append(":[InstanceType]" + config.getInstanceType() + ","); 
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

	public void runCode(Request rq) {
		if (rq.getParameter("script") != null && rq.getParameter("script").contains("Track.cpuFree")) {
			ArrayList<InstructionTree> Instructions = Common.compileCodes(rq.getParameter("script"), CPU.getUUID());
			for (InstructionTree Instruction : Instructions) {
				Instruction.depthRun(Instruction.getRoot());
			}
			setResponse(1, "code run success!");
		} else {
			setResponse(0, "script not set or script grammer is not correct!");
		}
	}

	public void runNow(Request rq) {
		if (rq.getParameter("instance") != null && rq.getParameter("jobtype") != null) {
			if (GlobalParam.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance"))
					&& GlobalParam.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).openTrans()) {
				boolean state = GlobalParam.FlOW_CENTER.runInstanceNow(rq.getParameter("instance"),
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
			removeInstance(rq.getParameter("instance"));
			setResponse(1, "Writer " + rq.getParameter("instance") + " job have removed!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance") + " remove error,instance parameter not set!");
		}
	}

	public void stopInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			if (rq.getParameter("type").toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
				controlThreadState(rq.getParameter("instance"), STATUS.Stop, false);
			} else {
				controlThreadState(rq.getParameter("instance"), STATUS.Stop, true);
			}
			setResponse(1, "Writer " + rq.getParameter("instance") + " job have stopped!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance") + " stop error,index parameter not set!");
		}
	}

	public void resumeInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			if (rq.getParameter("type").toUpperCase().equals(GlobalParam.JOB_TYPE.FULL.name())) {
				controlThreadState(rq.getParameter("instance"), STATUS.Ready, false);
			} else {
				controlThreadState(rq.getParameter("instance"), STATUS.Ready, true);
			}
			setResponse(1, "Writer " + rq.getParameter("instance") + " job have resumed!");
		} else {
			setResponse(0, "Writer " + rq.getParameter("instance") + " resume error,index parameter not set!");
		}
	}

	public void reloadConfig(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			controlThreadState(rq.getParameter("instance"), STATUS.Stop, true);
			int type = GlobalParam.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).getInstanceType();
			String instanceConfig = rq.getParameter("instance");
			if (type > 0) {
				instanceConfig = rq.getParameter("instance") + ":" + type;
			} else {
				if (!GlobalParam.nodeConfig.getInstanceConfigs().containsKey(rq.getParameter("instance")))
					setResponse(0, rq.getParameter("instance") + " not exists!");
			}
			if (rq.getParameter("reset") != null && rq.getParameter("reset").equals("true")
					&& rq.getParameter("instance").length() > 2) {
				GlobalParam.nodeConfig.loadConfig(instanceConfig, true);
			} else {
				GlobalParam.FLOW_INFOS.remove(rq.getParameter("instance"), JOB_TYPE.FULL.name());
				GlobalParam.FLOW_INFOS.remove(rq.getParameter("instance"), JOB_TYPE.INCREMENT.name());
				freeInstanceConnectPool(rq.getParameter("instance"));
				String alias = GlobalParam.nodeConfig.getInstanceConfigs().get(rq.getParameter("instance")).getAlias();
				GlobalParam.nodeConfig.getSearchConfigs().remove(alias);
				GlobalParam.nodeConfig.loadConfig(instanceConfig, false);
				GlobalParam.SOCKET_CENTER.getSearcher(alias, "", "", true);
			}
			rebuildFlowGovern(instanceConfig);
			controlThreadState(rq.getParameter("instance"), STATUS.Ready, true);
			setResponse(1, rq.getParameter("instance") + " reload Config Success!");
		} else {
			setResponse(0, rq.getParameter("instance") + " not exists!");
		}
	}

	/**
	 * 
	 * @param rq
	 *            instance parameter example,instanceName:1
	 */
	public void addInstance(Request rq) {
		if (rq.getParameter("instance").length() > 1) {
			GlobalParam.nodeConfig.loadConfig(rq.getParameter("instance"), false);
			String tmp[] = rq.getParameter("instance").split(":");
			String instanceName = rq.getParameter("instance");
			if (tmp.length > 1)
				instanceName = tmp[0];
			InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(instanceName);
			if (instanceConfig.checkStatus())
				NodeUtil.initParams(instanceConfig);
			rebuildFlowGovern(rq.getParameter("instance")); 
			GlobalParam.StartConfig.setProperty("instances", (GlobalParam.StartConfig.getProperty("instances")+","+rq.getParameter("instance")).replace(",,", ","));
			try {
				saveNodeConfig();
				setResponse(1, instanceName + " add to node " + GlobalParam.IP + " Success!");
			}catch (Exception e) {
				setResponse(0, e.getMessage());
			} 
		} else {
			setResponse(0, "Parameter not match!");
		}
	}

	/**
	 * delete Instance Data through alias or Instance data name
	 * 
	 * @param alias
	 * @return
	 */
	public void deleteInstanceData(Request rq) {
		if(rq.getParameter("instance")!=null) {
			String _instance = rq.getParameter("instance");
			Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
			boolean state = true;
			for (Map.Entry<String, InstanceConfig> ents : configMap.entrySet()) {
				String instance = ents.getKey();
				InstanceConfig instanceConfig = ents.getValue();
				if (instance.equals(_instance) || instanceConfig.getAlias().equals(_instance)) {
					String[] seqs = getInstanceSeqs(instance);
					if (seqs.length == 0) {
						seqs = new String[1];
						seqs[0] = GlobalParam.DEFAULT_RESOURCE_SEQ;
					}
					controlThreadState(instance, STATUS.Stop, true);
					for (String seq : seqs) {
						GlobalParam.LAST_UPDATE_TIME.set(instance, seq, "0");
						WarehouseNosqlParam param = GlobalParam.nodeConfig.getNoSqlWarehouse()
								.get(instanceConfig.getPipeParam().getWriteTo());
						state = state && deleteAction(param, instanceConfig, instance, seq);
					}
					controlThreadState(instance, STATUS.Ready, true);
				}
			}
			if (state) {
				setResponse(1, "delete " + _instance + " Success!");
			} else {
				setResponse(0, "delete " + _instance + " Failed!");
			}
		}else {
			setResponse(0, "Parameter not match!");
		} 
	}

	public void ESIndexDelete(String indexname, String seq, WarehouseNosqlParam param, InstanceConfig instanceConfig) {

		WarehouseSqlParam tmpDBParam = GlobalParam.nodeConfig.getSqlWarehouse()
				.get(instanceConfig.getPipeParam().getReadFrom());
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
			if (tmpDBParam.getSeq().length > 0) {
				String batchid = getBatchId(GlobalParam.CONFIG_PATH + "/" + indexname + "/" + seq + "/" + "batch");
				if (batchid == null) {
					setResponse(0, "Delete action failed,for no storeid get from store position!");
					return;
				}
				deleteRequest = new DeleteIndexRequest(indexname + seq + "_" + batchid);
				DeleteIndexResponse deleteResponse = es.admin().indices().delete(deleteRequest).actionGet();
				if (deleteResponse.isAcknowledged()) {
					resetZk(Common.getTaskStorePath(indexname, seq, GlobalParam.JOB_INCREMENTINFO_PATH));
					Common.LOG.info("index " + indexname + seq + "_" + batchid + " removed ");
				}
			} else {
				deleteRequest = new DeleteIndexRequest(indexname);
				DeleteIndexResponse deleteResponse = es.admin().indices().delete(deleteRequest).actionGet();
				if (deleteResponse.isAcknowledged()) {
					resetZk(Common.getTaskStorePath(indexname, null, GlobalParam.JOB_INCREMENTINFO_PATH));
					Common.LOG.info(indexname + " success removed!");
				}
			}
		} catch (Exception e) {
			setResponse(0, indexname + "Delete Exception!");
		} finally {
			FnConnectionPool.freeConn(FC, param.getIp() + "_" + param.getDefaultValue(), false);
		}
	}

	public void SOLRIndexDelete(String indexname, WarehouseNosqlParam param, InstanceConfig instanceConfig) {

	}

	public void HBASEIndexDelete(String indexname, WarehouseNosqlParam param, InstanceConfig instanceConfig) {

	}

	private void removeInstance(String instance) {
		controlThreadState(instance, STATUS.Stop, true);
		if (GlobalParam.nodeConfig.getInstanceConfigs().get(instance).getInstanceType() > 0) {
			GlobalParam.FLOW_INFOS.remove(instance, JOB_TYPE.FULL.name());
			GlobalParam.FLOW_INFOS.remove(instance, JOB_TYPE.INCREMENT.name());
		}
		GlobalParam.nodeConfig.getInstanceConfigs().remove(instance); 
		GlobalParam.FlOW_CENTER.removeInstance(instance);
		String tmp="";
		for(String str:GlobalParam.StartConfig.getProperty("instances").split(",")) {
			String[] s = str.split(":");
			if(s[0].equals(instance))
				continue;
			tmp+=str+",";
		}
		GlobalParam.StartConfig.setProperty("instances", tmp);
		try {
			saveNodeConfig(); 
		}catch (Exception e) {
			setResponse(0, e.getMessage());
		} 
	}

	/**
	 * control current run thread, prevent error data write
	 * 
	 * @param instance
	 *            multi-instances seperate with ","
	 * @param state
	 * @param isIncrement
	 *            control thread type
	 */
	private void controlThreadState(String instance, STATUS state, boolean isIncrement) {
		if ((GlobalParam.SERVICE_LEVEL & 6) == 0) {
			return;
		}
		String controlType = GlobalParam.JOB_TYPE.FULL.name();
		if (isIncrement)
			controlType = GlobalParam.JOB_TYPE.INCREMENT.name();

		for (String inst : instance.split(",")) {
			Common.LOG.info("Instance " + inst + " waitting set state " + state + " ...");
			int waittime = 0;
			String[] seqs = getInstanceSeqs(instance);
			for (String seq : seqs) {
				if (Common.checkFlowStatus(inst, seq, controlType, STATUS.Running)) {
					Common.setFlowStatus(inst, seq, controlType, STATUS.Blank, STATUS.Termination);
					while (!Common.checkFlowStatus(inst, seq, controlType, STATUS.Ready)) {
						try {
							waittime++;
							Thread.sleep(300);
							if (waittime > 200) {
								break;
							}
						} catch (InterruptedException e) {
							Common.LOG.error("currentThreadState InterruptedException", e);
						}
					}
				}
				Common.setFlowStatus(inst, seq, controlType, STATUS.Blank, STATUS.Termination);
				if (Common.setFlowStatus(inst, seq, controlType, STATUS.Termination, state)) {
					Common.LOG.info("Instance " + inst + " success set state " + state);
				} else {
					Common.LOG.info("Instance " + inst + " fail set state " + state);
				}
			}
		}
	}

	private String threadStateInfo(String instance, String tag) {
		String[] seqs = getInstanceSeqs(instance);
		StringBuffer sb = new StringBuffer();
		for (String seq : seqs) {
			sb.append(seq + ":");
			if (Common.checkFlowStatus(instance, seq, tag, STATUS.Stop))
				sb.append("Stop,");
			if (Common.checkFlowStatus(instance, seq, tag, STATUS.Ready))
				sb.append("Ready,");
			if (Common.checkFlowStatus(instance, seq, tag, STATUS.Running))
				sb.append("Running,");
			if (Common.checkFlowStatus(instance, seq, tag, STATUS.Termination))
				sb.append("Termination,");
			sb.append(" ;");
		}
		return sb.toString();
	}

	private String[] getInstanceSeqs(String instance) {
		InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(instance);
		WarehouseParam dataMap = GlobalParam.nodeConfig.getNoSqlWarehouse()
				.get(instanceConfig.getPipeParam().getReadFrom());
		if (dataMap == null) {
			dataMap = GlobalParam.nodeConfig.getSqlWarehouse().get(instanceConfig.getPipeParam().getReadFrom());
		}
		String[] seqs;
		if (dataMap == null) {
			seqs = new String[] {};
		} else {
			seqs = dataMap.getSeq();
		}

		if (seqs.length == 0) {
			seqs = new String[1];
			seqs[0] = GlobalParam.DEFAULT_RESOURCE_SEQ;
		}
		return seqs;
	}

	private void rebuildFlowGovern(String index) {
		for (String inst : index.split(",")) {
			String[] strs = inst.split(":");
			if (strs.length < 1)
				continue;
			GlobalParam.FlOW_CENTER.addFlowGovern(strs[0], GlobalParam.nodeConfig.getInstanceConfigs().get(strs[0]),
					true);
		}
	}

	private boolean deleteAction(WarehouseNosqlParam param, InstanceConfig instanceConfig, String indexname,
			String seq) {
		switch (param.getType()) {
		case ES:
		case SOLR:
		case HBASE:
			try {
				Method m = NodeMonitor.class.getMethod(param.getType().name() + "IndexDelete", String.class,
						String.class, WarehouseNosqlParam.class, InstanceConfig.class);
				m.invoke(this, indexname, seq, param, instanceConfig);
				return true;
			} catch (Exception e) {
				Common.LOG.error("deleteAction Exception ", e);
			}
			break;

		default:
			break;
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
		InstanceConfig paramConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(instanceName);
		String[] dataSource = { paramConfig.getPipeParam().getReadFrom(), paramConfig.getPipeParam().getWriteTo() };
		for (String dt : dataSource) {
			if (GlobalParam.nodeConfig.getNoSqlWarehouse().get(dt) != null) {
				WarehouseNosqlParam dataMap = GlobalParam.nodeConfig.getNoSqlWarehouse().get(dt);
				if (dataMap == null) {
					break;
				}
				String[] seqs = dataMap.getSeq();
				if (seqs.length > 0) {
					for (String seq : seqs) {
						FnConnectionPool.release(dataMap.getPoolName(seq));
					}
				} else {
					FnConnectionPool.release(dataMap.getPoolName(null));
				}
			} else if (GlobalParam.nodeConfig.getSqlWarehouse().get(dt) != null) {
				WarehouseSqlParam dataMap = GlobalParam.nodeConfig.getSqlWarehouse().get(dt);
				if (dataMap == null) {
					break;
				}
				String[] seqs = dataMap.getSeq();
				if (seqs.length > 0) {
					for (String seq : seqs) {
						FnConnectionPool.release(dataMap.getPoolName(seq));
					}
				} else {
					FnConnectionPool.release(dataMap.getPoolName(null));
				}
			}
		}
	}
	
	private void saveNodeConfig() throws Exception {
		OutputStream os = null;
		os = new FileOutputStream(configPath.replace("file:", "") + "/config/config.properties");
		GlobalParam.StartConfig.store(os, "Auto Save Config with no format,BeCarefull!");  
	}

	private String getBatchId(String path) {
		String[] strs = getZkData(path).split(GlobalParam.JOB_STATE_SPERATOR);
		if (strs.length > 0) {
			return strs[0];
		}
		return null;
	}

}
