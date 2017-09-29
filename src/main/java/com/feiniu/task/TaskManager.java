package com.feiniu.task;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.task.schedule.JobModel;
import com.feiniu.task.schedule.TaskJobCenter;

/**
 * Manage node all flow tasks
 * @author chengwen
 * @version 1.0 
 */
public class TaskManager{
	private final static Logger log = LoggerFactory
			.getLogger(TaskManager.class); 
	@Autowired
	private TaskJobCenter taskJobCenter; 
	
	private String default_cron = "0 PARAM 01 * * ?";
	
	private HashSet<String> cron_exists=new HashSet<String>();

	public void start() { 
		Map<String, NodeConfig> configMap = GlobalParam.nodeTreeConfigs.getConfigMap();
		for (Map.Entry<String, NodeConfig> entry : configMap.entrySet()) {
			String instanceName = entry.getKey();
			NodeConfig NodeConfig = entry.getValue();
			initParams(instanceName);
			startInstance(instanceName, NodeConfig,false);
			try {
				if (NodeConfig.hasRabitmq()) {
					//message deal 
				}
			} catch (Exception e) {
				log.error("RabitMqMessageHandler Exception", e);
			}
		} 
	}
	
	/**
	 * run job now
	 */
	public boolean runIndexJobNow(String instanceName, NodeConfig NodeConfig,String type){
		if (NodeConfig.isIndexer() == false)
			return false; 
		List<String> seqs = getSeqs(instanceName, NodeConfig); 
		if (seqs == null) { 
			log.error(instanceName+" job start run Exception with invalid data source!");
			return false;
		} 
		boolean state = true;
		try {
			if (seqs.size() > 0) {
				for (String seq : seqs) {
					if (seq == null)
						continue;
					state = jobAction(instanceName + seq, type, "run") && state;
				}
			} else {
				state = jobAction(instanceName, type, "run") && state;
			}
		} catch (Exception e) {
			log.error("runIndexJobNow Exception", e);
			return false;
		}
		return state;
	}
	/**
	 * clear node instance flow info
	 * @param instanceName
	 * @return
	 */
	public boolean removeInstance(String instanceName){
		Map<String, NodeConfig> configMap = GlobalParam.nodeTreeConfigs.getConfigMap();
		boolean state = true;
		if(configMap.containsKey(instanceName)){
			try{
				NodeConfig nodeConfig = configMap.get(instanceName);
				List<String> seqs = getSeqs(instanceName, nodeConfig);
				if (seqs.size() > 0) {
					for (String seq : seqs) {
						if (seq == null)
							continue;  
						if(GlobalParam.tasks.containsKey(instanceName+seq)){
							GlobalParam.tasks.remove(instanceName+seq);
						}
						state = removeFlowScheduleJob(instanceName + seq,nodeConfig) && state;
					}
				}else{
					if(GlobalParam.tasks.containsKey(instanceName)){
						GlobalParam.tasks.remove(instanceName);
					}
					state = removeFlowScheduleJob(instanceName,nodeConfig) && state;
				}
			}catch(Exception e){
				log.error("remove Instance "+instanceName+" Exception", e);
				return false;
			} 
		}
		return state;
	}
	
	/**
	 * start or restart add flow job
	 */
	public void startInstance(String instanceName, NodeConfig NodeConfig,boolean needClear) { 
		if (NodeConfig.isIndexer() == false)
			return;
		List<String> seqs = getSeqs(instanceName, NodeConfig); 
		if (seqs == null) { 
			log.error(instanceName+" job create Exception with invalid data source!");
			return;
		}
		try {
			if (seqs.size() > 0) {
				for (String seq : seqs) {
					if (seq == null)
						continue; 
					if(!GlobalParam.tasks.containsKey(instanceName+seq) || needClear){
						GlobalParam.tasks.put(instanceName+seq, Task.createTask(instanceName,
								GlobalParam.NODE_CENTER.getWriterChannel(instanceName, seq,needClear), seq));
					}  
					createFlowScheduleJob(instanceName + seq, GlobalParam.tasks.get(instanceName+seq),
							NodeConfig,needClear);
				}
			} else { 
				if(!GlobalParam.tasks.containsKey(instanceName) || needClear){
					GlobalParam.tasks.put(instanceName, Task.createTask(instanceName,
							GlobalParam.NODE_CENTER.getWriterChannel(instanceName, null,needClear)));
				} 
				createFlowScheduleJob(instanceName, GlobalParam.tasks.get(instanceName), NodeConfig,needClear);
			} 
		} catch (Exception e) {
			log.error("Start Instance "+instanceName+" Exception", e);
		}
	}

	public boolean jobAction(String indexName, String type, String actype) {
		String jobname = this.getJobName(indexName, type);
		boolean state = false;
		switch (actype) {
		case "stop":
			state = taskJobCenter.stopJob(jobname);
			break;
		case "run":
			state = taskJobCenter.startNow(jobname);
			break;
		case "resume":
			state = taskJobCenter.restartJob(jobname);
			break;
		case "remove":
			state = taskJobCenter.deleteJob(jobname);
			break;
		}
		if(state){
			log.info("[Job " + actype + "] Success " + jobname);
		}else{
			log.info("[Job " + actype + "] Failed! " + jobname);
		} 
		return state;
	}
	
	private List<String> getSeqs(String instanceName, NodeConfig NodeConfig){
		List<String> seqs = null;
		WarehouseParam whParam;
		if(GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(NodeConfig.getTransParam().getDataFrom())!=null){
			whParam = GlobalParam.nodeTreeConfigs.getNoSqlParamMap().get(
					NodeConfig.getTransParam().getDataFrom());
		}else{
			whParam = GlobalParam.nodeTreeConfigs.getSqlParamMap().get(
					NodeConfig.getTransParam().getDataFrom());
		}
		if (null != whParam) {
			seqs = whParam.getSeq();
		}  
		return seqs;
	}

	private void initParams(String indexName){
		GlobalParam.FLOW_STATUS.put(indexName, new AtomicInteger(1));
		GlobalParam.LAST_UPDATE_TIME.put(indexName, "0");
	} 
	
	private boolean removeFlowScheduleJob(String instance,NodeConfig NodeConfig)throws SchedulerException {
		boolean state = true;
		if (NodeConfig.getTransParam().getFullCron() != null) { 
			state= jobAction(instance, "full", "remove") && state;
		}
		if(NodeConfig.getTransParam().getFullCron() == null || NodeConfig.getOptimizeCron().equals("")){
			state = jobAction(instance, "optimize", "remove") && state;
		}
		if(NodeConfig.getTransParam().getDeltaCron() != null){
			state = jobAction(instance, "increment", "remove") && state;
		}
		return state;
	}

	private void createFlowScheduleJob(String instance, Task task,
			NodeConfig NodeConfig,boolean needclear)
			throws SchedulerException {
		if (NodeConfig.getTransParam().getFullCron() != null) { 
			if(needclear){
				jobAction(instance, "full", "remove");
			}
			JobModel _sj = new JobModel(
					getJobName(instance, "full"), NodeConfig.getTransParam().getFullCron(),
					"com.feiniu.manager.Task", "startFullIndex", task); 
			taskJobCenter.addJob(_sj); 
		} 
		
		if(NodeConfig.getTransParam().getFullCron() == null || NodeConfig.getOptimizeCron().equals("")){
			if(needclear){
				jobAction(instance, "optimize", "remove");
			}
			String cron = NodeConfig.getOptimizeCron().equals("")?default_cron.replace("PARAM",String.valueOf((int)(Math.random()*60))):NodeConfig.getOptimizeCron();
			NodeConfig.setOptimizeCron(cron);
			createOptimizeJob(instance, task,cron);
		}
		
		if (NodeConfig.getTransParam().getDeltaCron() != null) { 
			if(needclear){
				jobAction(instance, "increment", "remove");
			}
			String cron = NodeConfig.getTransParam().getDeltaCron();
			if(this.cron_exists.contains(cron)){
				String[] strs = cron.trim().split(" ");
				strs[0] = String.valueOf((int)(Math.random()*60));
				String _s="";
				for(String s:strs){
					_s+=s+" ";
				}
				cron = _s.trim();
			}else{
				this.cron_exists.add(cron);
			}
			JobModel _sj = new JobModel(
					getJobName(instance, "increment"),
					NodeConfig.getTransParam().getDeltaCron(), "com.feiniu.manager.Task",
					"startIncrementIndex", task); 
			taskJobCenter.addJob(_sj);
		} 
	}
	
	private void createOptimizeJob(String indexName, Task batch,String cron) throws SchedulerException{
		JobModel _sj = new JobModel(
				getJobName(indexName, "optimize"),cron,
				"com.feiniu.manager.Task", "optimizeIndex", batch); 
		taskJobCenter.addJob(_sj); 
	}

	private String getJobName(String indexName, String type) {
		if (type.equals("full")) {
			return indexName + "_FullJob";
		} else if (type.equals("increment")) {
			return indexName + "_IncrementJob";
		}else{
			return indexName + "_OptimizeJob";
		}
	} 
	
}
