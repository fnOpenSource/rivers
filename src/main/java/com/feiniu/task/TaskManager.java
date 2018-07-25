package com.feiniu.task;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.model.param.InstructionParam;
import com.feiniu.task.schedule.JobModel;
import com.feiniu.task.schedule.TaskJobCenter;
import com.feiniu.util.Common;

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

	public void startWriteJob() { 
		Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) {
			String instanceName = entry.getKey();
			InstanceConfig instanceConfig = entry.getValue(); 
			startInstance(instanceName, instanceConfig,false); 
		}
		//startRabbitmqMessage(MQconsumerMonitorMap);
	}
	
	public void startInstructions() {
		Map<String, InstructionParam> instructions = GlobalParam.nodeConfig.getInstructions();
		for (Map.Entry<String,InstructionParam> entry : instructions.entrySet()){
			createInstructionScheduleJob(entry.getValue(),InstructionTask.createTask(entry.getKey()));
		}
	}
	
	/**
	 * run job now
	 */
	public boolean runIndexJobNow(String instanceName, InstanceConfig instanceConfig,String type){
		if (instanceConfig.isIndexer() == false)
			return false; 
		List<String> seqs = Common.getSeqs(instanceConfig); 
		if (seqs == null) { 
			log.error(instanceName+" job start run Exception with invalid data source!");
			return false;
		} 
		boolean state = true; 
		try {
			if (seqs.size() == 0) {
				seqs.add(GlobalParam.DEFAULT_RESOURCE_SEQ);
			} 
			for (String seq : seqs) {
				if (seq == null)
					continue;
				if ((GlobalParam.FLOW_STATUS.get(instanceName, seq).get() & 1) > 0)
					state = jobAction(instanceName + seq, type, "run") && state;
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
		Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
		boolean state = true;
		if(configMap.containsKey(instanceName)){
			try{
				InstanceConfig instanceConfig = configMap.get(instanceName);
				List<String> seqs = Common.getSeqs(instanceConfig);
				if (seqs.size() > 0) {
					for (String seq : seqs) {
						if (seq == null)
							continue;  
						if(GlobalParam.tasks.containsKey(instanceName+seq)){
							GlobalParam.tasks.remove(instanceName+seq);
						}
						state = removeFlowScheduleJob(instanceName + seq,instanceConfig) && state;
					}
				}else{
					if(GlobalParam.tasks.containsKey(instanceName)){
						GlobalParam.tasks.remove(instanceName);
					}
					state = removeFlowScheduleJob(instanceName,instanceConfig) && state;
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
	public void startInstance(String instanceName, InstanceConfig instanceConfig,boolean needClear) { 
		if (instanceConfig.checkStatus()==false || instanceConfig.isIndexer() == false)
			return;
		List<String> seqs = Common.getSeqs(instanceConfig); 
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
						GlobalParam.tasks.put(instanceName+seq, FlowTask.createTask(instanceName,
								GlobalParam.SOCKET_CENTER.getTransDataFlow(instanceName, seq,needClear,GlobalParam.DEFAULT_RESOURCE_TAG), seq));
					}  
					createFlowScheduleJob(instanceName + seq, GlobalParam.tasks.get(instanceName+seq),
							instanceConfig,needClear);
				}
			} else { 
				if(!GlobalParam.tasks.containsKey(instanceName) || needClear){
					GlobalParam.tasks.put(instanceName, FlowTask.createTask(instanceName,
							GlobalParam.SOCKET_CENTER.getTransDataFlow(instanceName, null,needClear,GlobalParam.DEFAULT_RESOURCE_TAG)));
				} 
				createFlowScheduleJob(instanceName, GlobalParam.tasks.get(instanceName), instanceConfig,needClear);
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
	 
	/*
	private void startRabbitmqMessage(
			Map<String, IMessageHandler> MQconsumerMonitorMap) {
		log.info("start Rabbitmq Message...");
		RabbitmqConsumerClient RC = new RabbitmqConsumerClient(rabbitmqConfig,
				MQconsumerMonitorMap);
		RC.init();
	}
	*/
	private boolean removeFlowScheduleJob(String instance,InstanceConfig instanceConfig)throws SchedulerException {
		boolean state = true;
		if (instanceConfig.getPipeParam().getFullCron() != null) { 
			state= jobAction(instance, "full", "remove") && state;
		}
		if(instanceConfig.getPipeParam().getFullCron() == null || instanceConfig.getPipeParam().getOptimizeCron()!=null){
			state = jobAction(instance, "optimize", "remove") && state;
		}
		if(instanceConfig.getPipeParam().getDeltaCron() != null){
			state = jobAction(instance, "increment", "remove") && state;
		}
		return state;
	}
	
	private void createInstructionScheduleJob(InstructionParam param, InstructionTask task) {
		JobModel _sj = new JobModel(
				getJobName(param.getId(), "instruction"), param.getCron(),
				"com.feiniu.task.InstructionTask", "startInstructions", task); 
		try {
			taskJobCenter.addJob(_sj); 
		}catch (Exception e) {
			log.error("create Instruction Job "+param.getId()+" Exception", e);
		} 
	}

	private void createFlowScheduleJob(String instance, FlowTask task,
			InstanceConfig instanceConfig,boolean needclear)
			throws SchedulerException {
		if (instanceConfig.getPipeParam().getFullCron() != null) { 
			if(needclear){
				jobAction(instance, "full", "remove");
			}
			JobModel _sj = new JobModel(
					getJobName(instance, "full"), instanceConfig.getPipeParam().getFullCron(),
					"com.feiniu.task.FlowTask", "startFullJob", task); 
			taskJobCenter.addJob(_sj); 
		} 
		
		if(instanceConfig.getPipeParam().getFullCron() == null || instanceConfig.getPipeParam().getOptimizeCron()!=null){
			if(needclear){
				jobAction(instance, "optimize", "remove");
			}
			String cron = instanceConfig.getPipeParam().getOptimizeCron()==null?default_cron.replace("PARAM",String.valueOf((int)(Math.random()*60))):instanceConfig.getPipeParam().getOptimizeCron();
			instanceConfig.getPipeParam().setOptimizeCron(cron);
			if(instanceConfig.getPipeParam().getInstanceName()==null) {
				createOptimizeJob(instance, task,cron); 
			} 
		}
		
		if (instanceConfig.getPipeParam().getDeltaCron() != null) { 
			if(needclear){
				jobAction(instance, "increment", "remove");
			}
			String cron = instanceConfig.getPipeParam().getDeltaCron();
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
					instanceConfig.getPipeParam().getDeltaCron(), "com.feiniu.task.FlowTask",
					"startIncrementJob", task); 
			taskJobCenter.addJob(_sj);
		} 
	}
	
	private void createOptimizeJob(String indexName, FlowTask batch,String cron) throws SchedulerException{
		JobModel _sj = new JobModel(
				getJobName(indexName, "optimize"),cron,
				"com.feiniu.manager.Task", "optimizeInstance", batch); 
		taskJobCenter.addJob(_sj); 
	}

	private String getJobName(String indexName, String type) {
		if (type.equals("full")) {
			return indexName + "_FullJob";
		} else if (type.equals("increment")) {
			return indexName + "_IncrementJob";
		}else if(type.equals("instruction")){
			return indexName + "_InstructionJob";
		}else{
			return indexName + "_OptimizeJob";
		}
	}  
}
