package com.feiniu.task;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.GlobalParam.STATUS;
import com.feiniu.config.InstanceConfig;
import com.feiniu.node.CPU;
import com.feiniu.piper.PipePump;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
 
/**
 * schedule task description,to manage task job
 * @author chengwen
 * @version 2.0
 * @date 2018-11-27 10:23
 */
public class FlowTask {

	private boolean recompute = true;
	private boolean masterControl = false;
	private String instance;
	private PipePump transDataFlow;
	/**
	 * seq for scan series datas
	 */
	private String seq = "";

	private final static Logger log = LoggerFactory.getLogger(FlowTask.class);

	public static FlowTask createTask(String instanceName, PipePump transDataFlow) {
		return new FlowTask(instanceName, transDataFlow, GlobalParam.DEFAULT_RESOURCE_SEQ);
	}

	public static FlowTask createTask(String instanceName, PipePump transDataFlow, String seq) {
		return new FlowTask(instanceName, transDataFlow, seq);
	}

	private FlowTask(String instance, PipePump transDataFlow, String seq) {
		this.instance = instance;
		this.transDataFlow = transDataFlow;
		this.seq = seq;
		if (transDataFlow.getInstanceConfig().getPipeParams().getInstanceName() != null)
			masterControl = true;
	}

	/**
	 * if no full job will auto open optimize job
	 */
	public void optimizeInstance() {
		String storeName = Common.getMainName(instance, seq);
		CPU.RUN(transDataFlow.getID(), "Pond", "optimizeInstance", true, storeName,
				Common.getStoreId(instance, seq, transDataFlow, true, false));
	}

	/**
	 * slave instance full job
	 */
	public void runFull() {
		if (Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running)) {
			try {
				GlobalParam.SCAN_POSITION.get(Common.getMainName(instance, seq)).keepCurrentPos();
				String storeId;
				if (masterControl) {
					storeId = GlobalParam.FLOW_INFOS
							.get(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
									GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_STOREID.name());
				} else {
					storeId = Common.getStoreId(instance, seq, transDataFlow, false, false);
					CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
							Common.getMainName(instance, seq), storeId);
				}
				if(storeId!=null) {
					transDataFlow.run(instance, storeId, seq, true,
							masterControl);
					GlobalParam.SCAN_POSITION.get(Common.getMainName(instance, seq)).recoverKeep(); 
					Common.saveTaskInfo(instance, seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
				} 
			} catch (Exception e) {
				log.error(instance + " Full Exception", e);
			} finally {
				Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready);
			}
		}
	}

	public void runMasterFull() {
		if (Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready,STATUS.Running)) {
			try {
				String storeId = Common.getStoreId(instance, seq, transDataFlow, false, false);
				if (!GlobalParam.FLOW_INFOS.containsKey(instance, GlobalParam.FLOWINFO.MASTER.name())) {
					GlobalParam.FLOW_INFOS.set(instance, GlobalParam.FLOWINFO.MASTER.name(),
							new HashMap<String, String>());
				}
				GlobalParam.FLOW_INFOS.get(instance, GlobalParam.FLOWINFO.MASTER.name())
						.put(GlobalParam.FLOWINFO.FULL_STOREID.name(), storeId);

				CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
						Common.getMainName(instance, seq), storeId);

				GlobalParam.FLOW_INFOS.get(instance, GlobalParam.FLOWINFO.MASTER.name()).put(
						GlobalParam.FLOWINFO.FULL_JOBS.name(),
						getNextJobs(transDataFlow.getInstanceConfig().getPipeParams().getNextJob()));

				for (String slave : transDataFlow.getInstanceConfig().getPipeParams().getNextJob()) {
					GlobalParam.FlOW_CENTER.runInstanceNow(slave, "full");
				}
			} catch (Exception e) {
				log.error(instance + " Full Exception", e);
			} finally {
				Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Blank,STATUS.Ready);
			}
		}
	}

	public void runMasterIncrement() {
		if (Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running)) {
			String storeId = Common.getStoreId(instance, seq, transDataFlow, true, recompute);
			if (!GlobalParam.FLOW_INFOS.containsKey(instance, GlobalParam.FLOWINFO.MASTER.name())) {
				GlobalParam.FLOW_INFOS.set(instance, GlobalParam.FLOWINFO.MASTER.name(),
						new HashMap<String, String>());
			}
			GlobalParam.FLOW_INFOS.get(instance, GlobalParam.FLOWINFO.MASTER.name())
					.put(GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId);
			try {
				for (String slave : transDataFlow.getInstanceConfig().getPipeParams().getNextJob()) {
					GlobalParam.FlOW_CENTER.runInstanceNow(slave, "increment");
				}
			} finally {
				Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready);  
				recompute = false;
			}
		} else {
			log.info(instance + " flow have been closed!startIncrement flow failed!");
		}
	}

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {  
		if (Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready,STATUS.Running)) {
			String storeId;
			if (masterControl) {
				storeId = GlobalParam.FLOW_INFOS.get(transDataFlow.getInstanceConfig().getPipeParams().getInstanceName(),
						GlobalParam.FLOWINFO.MASTER.name()).get(GlobalParam.FLOWINFO.INCRE_STOREID.name());
				Common.setAndGetScanInfo(instance, seq, storeId);
			} else {
				storeId = Common.getStoreId(instance, seq, transDataFlow, true, recompute);
			}
 
			try {
				transDataFlow.run(instance, storeId, seq, false, masterControl); 
			} catch (FNException e) {
				if (!masterControl && e.getMessage().equals("storeId not found")) {
					storeId = Common.getStoreId(instance, seq, transDataFlow, true, true);
					try {
						transDataFlow.run(instance, storeId,seq, false, masterControl);
					} catch (FNException ex) {
						log.error(instance + " Increment Exception", ex);
					}
				}
				log.error(instance + " IncrementJob Exception", e);
			} finally {
				recompute = false;
				Common.setFlowStatus(instance,seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready); 
			}
		} else {
			log.info(instance + " flow have been closed!Current Start Increment flow failed!");
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
					sf.append(Common.getMainName(job, seq) + " ");
				}
			}
		}
		return sf.toString();
	} 
}
