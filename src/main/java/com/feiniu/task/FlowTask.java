package com.feiniu.task;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.instruction.flow.TransDataFlow;
import com.feiniu.node.CPU;
import com.feiniu.util.Common;

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
	/**
	 * runState,1 increment running 2 full running;
	 */
	private AtomicInteger runState = new AtomicInteger(0);

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
		if (transDataFlow.getInstanceConfig().getPipeParam().getInstanceName() != null)
			masterControl = true;
	}

	/**
	 * if no full job will auto open optimize job
	 */
	public void optimizeInstance() {
		GlobalParam.FLOW_STATUS.get(instanceName, seq).set(0);
		String storeName = Common.getInstanceName(instanceName, seq);
		CPU.RUN(transDataFlow.getID(), "Pond", "optimizeInstance", true, storeName,
				Common.getStoreId(instanceName, seq, transDataFlow, true, false));
		GlobalParam.FLOW_STATUS.get(instanceName, seq).set(1);
	}

	/**
	 * slave instance full job
	 */
	public void runFull() {
		if ((this.runState.get() & 2) == 0) {
			this.runState.getAndAdd(2);
			try {
				String keepCurrentUpdateTime = GlobalParam.LAST_UPDATE_TIME.get(instanceName, seq);
				String storeId;
				if (masterControl) {
					storeId = GlobalParam.FLOW_INFOS
							.get(transDataFlow.getInstanceConfig().getPipeParam().getInstanceName(),
									GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_STOREID.name());
				} else {
					storeId = Common.getStoreId(instanceName, seq, transDataFlow, false, false);
					CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition", true,
							Common.getInstanceName(instanceName, seq), storeId);
				}
				transDataFlow.run(instanceName, storeId, Common.getFullStartInfo(instanceName, seq), seq, true,
						masterControl);
				GlobalParam.LAST_UPDATE_TIME.set(instanceName, seq, keepCurrentUpdateTime);
				GlobalParam.FLOW_STATUS.get(instanceName, seq).set(4);
				Common.saveTaskInfo(instanceName, seq, storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
			} catch (Exception e) {
				log.error(instanceName + " Full Exception", e);
			} finally {
				this.runState.getAndAdd(-2);
				GlobalParam.FLOW_STATUS.get(instanceName, seq).set(1);
			}
		} else {
			log.info(instanceName + " full job is running, ignore this time job!");
		}
	}

	public void runMasterFull() {
		if ((this.runState.get() & 2) == 0) {
			this.runState.getAndAdd(2);
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
						getNextJobs(transDataFlow.getInstanceConfig().getPipeParam().getNextJob()));

				for (String slave : transDataFlow.getInstanceConfig().getPipeParam().getNextJob()) {
					GlobalParam.TASKMANAGER.runInstanceNow(slave, "full");
				}
				GlobalParam.FLOW_STATUS.get(instanceName, seq).set(4);
			} catch (Exception e) {
				log.error(instanceName + " Full Exception", e);
			} finally {
				this.runState.getAndAdd(-2);
				GlobalParam.FLOW_STATUS.get(instanceName, seq).set(1);
			}
		} else {
			log.info(instanceName + " full job is running, ignore this time job!");
		}
	}
	
	private static String getNextJobs(String[] nextJobs) {
		StringBuffer sf = new StringBuffer();
		for(String job:nextJobs) {
			InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(job);
			if (instanceConfig.isIndexer() != false) {
				String[] _seqs = Common.getSeqs(instanceConfig,true);   
				for (String seq : _seqs) {
					if (seq == null)
						continue;
					sf.append(Common.getInstanceName(job,seq)+" ");
				}
			}  
		}
		return sf.toString();
	}

	public void runMasterIncrement() {
		synchronized (this.runState) {
			if ((this.runState.get() & 1) == 0) {
				if ((GlobalParam.FLOW_STATUS.get(instanceName, seq).get() & 1) > 0) {
					this.runState.getAndAdd(1);
					GlobalParam.FLOW_STATUS.get(instanceName, seq).set(3);
					String storeId = Common.getStoreId(instanceName, seq, transDataFlow, true, recompute);
					if (!GlobalParam.FLOW_INFOS.containsKey(instanceName, GlobalParam.FLOWINFO.MASTER.name())) {
						GlobalParam.FLOW_INFOS.set(instanceName, GlobalParam.FLOWINFO.MASTER.name(),
								new HashMap<String, String>());
					}
					GlobalParam.FLOW_INFOS.get(instanceName, GlobalParam.FLOWINFO.MASTER.name())
							.put(GlobalParam.FLOWINFO.INCRE_STOREID.name(), storeId);
					try {
						for (String slave : transDataFlow.getInstanceConfig().getPipeParam().getNextJob()) {
							GlobalParam.TASKMANAGER.runInstanceNow(slave, "increment");
						}
					}finally {
						GlobalParam.FLOW_STATUS.get(instanceName, seq).set(1);
						recompute = false;
						this.runState.getAndAdd(-1);
					} 
				} else {
					log.info(instanceName + " job have been stopped!startIncrement JOB failed!");
				}
			} else {
				log.info(instanceName + " increment job is running, ignore this time job!");
			}
		}
	} 

	/**
	 * slave instance increment job
	 */
	public void runIncrement() {
		synchronized (this.runState) {
			if ((this.runState.get() & 1) == 0) {
				if ((GlobalParam.FLOW_STATUS.get(instanceName, seq).get() & 1) > 0) {
					this.runState.getAndAdd(1);
					GlobalParam.FLOW_STATUS.get(instanceName, seq).set(3);
					String storeId;
					if (masterControl) {
						storeId = GlobalParam.FLOW_INFOS
								.get(transDataFlow.getInstanceConfig().getPipeParam().getInstanceName(),
										GlobalParam.FLOWINFO.MASTER.name())
								.get(GlobalParam.FLOWINFO.INCRE_STOREID.name());
						Common.setAndGetLastUpdateTime(instanceName, seq,storeId);
					} else {
						storeId = Common.getStoreId(instanceName, seq, transDataFlow, true, recompute);
					}

					String lastUpdateTime;
					try {
						lastUpdateTime = transDataFlow.run(instanceName, storeId,
								GlobalParam.LAST_UPDATE_TIME.get(instanceName, seq), seq, false, masterControl);
						GlobalParam.LAST_UPDATE_TIME.set(instanceName, seq, lastUpdateTime);
					} catch (Exception e) {
						if (!masterControl && e.getMessage().equals("storeId not found")) {
							storeId = Common.getStoreId(instanceName, seq, transDataFlow, true, true);
							try {
								lastUpdateTime = transDataFlow.run(instanceName, storeId,
										GlobalParam.LAST_UPDATE_TIME.get(instanceName, seq), seq, false, masterControl);
								GlobalParam.LAST_UPDATE_TIME.set(instanceName, seq, lastUpdateTime);
							} catch (Exception ex) {
								log.error(instanceName + " Increment Exception", ex);
							}
						}
						log.error(instanceName + " startIncrementJob Exception", e);
					} finally {
						recompute = false;
						GlobalParam.FLOW_STATUS.get(instanceName, seq).set(1);
						this.runState.getAndAdd(-1);
					}
				} else {
					log.info(instanceName + " job have been stopped!startIncrement JOB failed!");
				}
			} else {
				log.info(instanceName + " increment job is running, ignore this time job!");
			}
		}
	}

}
