package com.feiniu.task;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.STATUS;
import com.feiniu.config.InstanceConfig;
import com.feiniu.node.CPU;
import com.feiniu.piper.TransDataFlow;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;

/**
 * schedule task description,to manage task job
 * 
 * @author chengwen
 * @version 1.0
 */
public class FlowTask {

	private boolean recompute = true;
	private boolean masterControl = false;
	private String instanceName;
	private TransDataFlow transDataFlow;
	/**
	 * seq for scan series datas
	 */
	private String seq = "";

	private final static Logger log = LoggerFactory.getLogger(FlowTask.class);

	public static FlowTask createTask(String instanceName, TransDataFlow transDataFlow) {
		return new FlowTask(instanceName, transDataFlow, GlobalParam.DEFAULT_RESOURCE_SEQ);
	}

	public static FlowTask createTask(String instanceName, TransDataFlow transDataFlow, String seq) {
		return new FlowTask(instanceName, transDataFlow, seq);
	}

	private FlowTask(String instanceName, TransDataFlow transDataFlow, String seq) {
		this.instanceName = instanceName;
		this.transDataFlow = transDataFlow;
		this.seq = seq;
		if (transDataFlow.getInstanceConfig().getPipeParams().getInstanceName() != null)
			masterControl = true;
	}

	/**
	 * if no full job will auto open optimize job
	 */
	public void optimizeInstance() {
		String storeName = Common.getInstanceName(instanceName, seq);
		CPU.RUN(transDataFlow.getID(), "Pond", "optimizeInstance", true, storeName,
				Common.getStoreId(instanceName, seq, transDataFlow, true, false));
	}

	/**
	 * slave instance full job
	 */
	public void runFull() {
		if (Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running)) {
			try {
				String keepCurrentUpdateTime = GlobalParam.LAST_UPDATE_TIME.get(instanceName, seq);
				String storeId;
				if (masterControl) {
					storeId = GlobalParam.FLOW_INFOS
							.get(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
									GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_STOREID.name());
				} else {
					storeId = Common.getStoreId(instanceName, seq, transDataFlow, false, false);
					CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
							Common.getInstanceName(instanceName, seq), storeId);
				}
				if(storeId!=null) {
					transDataFlow.run(instanceName, storeId, Common.getFullStartInfo(instanceName, seq), seq, true,
							masterControl);
					GlobalParam.LAST_UPDATE_TIME.set(instanceName, seq, keepCurrentUpdateTime);
					Common.saveTaskInfo(instanceName, seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
				} 
			} catch (Exception e) {
				log.error(instanceName + " Full Exception", e);
			} finally {
				Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready);
			}
		}
	}

	public void runMasterFull() {
		if (Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running)) {
			try {
				String storeId = Common.getStoreId(instanceName, seq, transDataFlow, false, false);
				if (!GlobalParam.FLOW_INFOS.containsKey(instanceName, GlobalParam.FLOWINFO.MASTER.name())) {
					GlobalParam.FLOW_INFOS.set(instanceName, GlobalParam.FLOWINFO.MASTER.name(),
							new HashMap<String, String>());
				}
				GlobalParam.FLOW_INFOS.get(instanceName, GlobalParam.FLOWINFO.MASTER.name())
						.put(GlobalParam.FLOWINFO.FULL_STOREID.name(), storeId);

				CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
						Common.getInstanceName(instanceName, seq), storeId);

				GlobalParam.FLOW_INFOS.get(instanceName, GlobalParam.FLOWINFO.MASTER.name()).put(
						GlobalParam.FLOWINFO.FULL_JOBS.name(),
						getNextJobs(transDataFlow.getInstanceConfig().getPipeParams().getNextJob()));

				for (String slave : transDataFlow.getInstanceConfig().getPipeParams().getNextJob()) {
					GlobalParam.FlOW_CENTER.runInstanceNow(slave, "full");
				}
			} catch (Exception e) {
				log.error(instanceName + " Full Exception", e);
			} finally {
				Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready);
			}
		}
	}

	public void runMasterIncrement() {
		if (Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running)) {
			String storeId = Common.getStoreId(instanceName, seq, transDataFlow, true, recompute);
			if (!GlobalParam.FLOW_INFOS.containsKey(instanceName, GlobalParam.FLOWINFO.MASTER.name())) {
				GlobalParam.FLOW_INFOS.set(instanceName, GlobalParam.FLOWINFO.MASTER.name(),
						new HashMap<String, String>());
			}
			GlobalParam.FLOW_INFOS.get(instanceName, GlobalParam.FLOWINFO.MASTER.name())
					.put(GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId);
			try {
				for (String slave : transDataFlow.getInstanceConfig().getPipeParams().getNextJob()) {
					GlobalParam.FlOW_CENTER.runInstanceNow(slave, "increment");
				}
			} finally {
				Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready);  
				recompute = false;
			}
		} else {
			log.info(instanceName + " flow have been closed!startIncrement flow failed!");
		}
	}

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {  
		if (Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running)) {
			String storeId;
			if (masterControl) {
				storeId = GlobalParam.FLOW_INFOS.get(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
						GlobalParam.FLOWINFO.MASTER.name()).get(GlobalParam.FLOWINFO.INCRE_STOREID.name());
				Common.setAndGetLastUpdateTime(instanceName, seq, storeId);
			} else {
				storeId = Common.getStoreId(instanceName, seq, transDataFlow, true, recompute);
			}

			String lastUpdateTime;
			try {
				lastUpdateTime = transDataFlow.run(instanceName, storeId,
						GlobalParam.LAST_UPDATE_TIME.get(instanceName, seq), seq, false, masterControl);
				GlobalParam.LAST_UPDATE_TIME.set(instanceName, seq, lastUpdateTime);
			} catch (FNException e) {
				if (!masterControl && e.getMessage().equals("storeId not found")) {
					storeId = Common.getStoreId(instanceName, seq, transDataFlow, true, true);
					try {
						lastUpdateTime = transDataFlow.run(instanceName, storeId,
								GlobalParam.LAST_UPDATE_TIME.get(instanceName, seq), seq, false, masterControl);
						GlobalParam.LAST_UPDATE_TIME.set(instanceName, seq, lastUpdateTime);
					} catch (FNException ex) {
						log.error(instanceName + " Increment Exception", ex);
					}
				}
				log.error(instanceName + " IncrementJob Exception", e);
			} finally {
				recompute = false;
				Common.setFlowStatus(instanceName,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready); 
			}
		} else {
			log.info(instanceName + " flow have been closed!Current Start Increment flow failed!");
		}
	}

	private static String getNextJobs(String[] nextJobs) {
		StringBuilder sf = new StringBuilder();
		for (String job : nextJobs) {
			InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(job);
			if (instanceConfig.openTrans()) {
				String[] _seqs = Common.getSeqs(instanceConfig, true);
				for (String seq : _seqs) {
					if (seq == null)
						continue;
					sf.append(Common.getInstanceName(job, seq) + " ");
				}
			}
		}
		return sf.toString();
	} 
}
