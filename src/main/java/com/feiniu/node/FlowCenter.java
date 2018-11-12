package com.feiniu.node;

import java.util.HashSet;
import java.util.Map;

import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.config.GlobalParam.STATUS;
import com.feiniu.param.pipe.InstructionParam;
import com.feiniu.task.FlowTask;
import com.feiniu.task.InstructionTask;
import com.feiniu.task.schedule.JobModel;
import com.feiniu.task.schedule.TaskJobCenter;
import com.feiniu.util.Common;

/**
 * read to write flow build center
 * @author chengwen
 * @version 2.0 
 */
public class FlowCenter{ 
	
	@Autowired
	private TaskJobCenter taskJobCenter; 
	
	private String default_cron = "0 PARAM 01 * * ?";
	
	private String not_run_cron = "0 0 0 1 1 ? 2099";
	
	private HashSet<String> cron_exists=new HashSet<String>();

	/**
	 * build read to flow flow
	 */
	public void buildRWFlow() { 
		Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
		for (Map.Entry<String, InstanceConfig> entry : configMap.entrySet()) { 
			addFlowGovern(entry.getKey(), entry.getValue(),false); 
		} 
	}
	
	public void startInstructionsJob() {
		Map<String, InstructionParam> instructions = GlobalParam.nodeConfig.getInstructions();
		for (Map.Entry<String,InstructionParam> entry : instructions.entrySet()){
			createInstructionScheduleJob(entry.getValue(),InstructionTask.createTask(entry.getKey()));
		}
	}
 
	public boolean runInstanceNow(String instanceName,String type){
		InstanceConfig instanceConfig = GlobalParam.nodeConfig.getInstanceConfigs().get(instanceName); 
		boolean state = true; 
		try {
			if (instanceConfig.openTrans() == false)
				return false;   
			String[] seqs = Common.getSeqs(instanceConfig,true);  
			for (String seq : seqs) {
				if (seq == null)
					continue;
				
				if(GlobalParam.JOB_TYPE.FULL.name().equals(type.toUpperCase())) {
					if (Common.checkFlowStatus(instanceName, seq,GlobalParam.JOB_TYPE.FULL.name(),STATUS.Ready))
						state = jobAction(Common.getInstanceName(instanceName, seq), GlobalParam.JOB_TYPE.FULL.name(), "run") && state;
				}else {
					if (Common.checkFlowStatus(instanceName, seq,GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Ready))
						state = jobAction(Common.getInstanceName(instanceName, seq), GlobalParam.JOB_TYPE.INCREMENT.name(), "run") && state;
				}
				
			}
		} catch (Exception e) {
			Common.LOG.error("runInstanceNow "+instanceName+" Exception", e);
			return false;
		}

		return state;
	}
 
	public boolean removeInstance(String instance){
		Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getInstanceConfigs();
		boolean state = true;
		if(configMap.containsKey(instance)){
			try{
				InstanceConfig instanceConfig = configMap.get(instance);
				String[] seqs = Common.getSeqs(instanceConfig,true);
				for (String seq : seqs) {
					if (seq == null)
						continue;  
					if(GlobalParam.tasks.containsKey(Common.getInstanceName(instance, seq)))
						GlobalParam.tasks.remove(Common.getInstanceName(instance, seq));
					
					for(GlobalParam.FLOW_TAG tag:GlobalParam.FLOW_TAG.values()) {
						GlobalParam.SOCKET_CENTER.clearTransDataFlow(instance, seq, tag.name());
					} 
					state = removeFlowScheduleJob(Common.getInstanceName(instance, seq),instanceConfig) && state;
				}
			}catch(Exception e){
				Common.LOG.error("remove Instance "+instance+" Exception", e);
				return false;
			} 
		}
		return state;
	}
 
	public void addFlowGovern(String instanceName, InstanceConfig instanceConfig,boolean needClear) { 
		if (instanceConfig.checkStatus()==false || instanceConfig.openTrans() == false)
			return;
		String[] seqs = Common.getSeqs(instanceConfig,true);  
		try {
			for (String seq : seqs) {
				if (seq == null)
					continue; 
				if(!GlobalParam.tasks.containsKey(Common.getInstanceName(instanceName, seq)) || needClear){
					GlobalParam.tasks.put(Common.getInstanceName(instanceName, seq), FlowTask.createTask(instanceName,
							GlobalParam.SOCKET_CENTER.getTransDataFlow(instanceName, seq,needClear,GlobalParam.FLOW_TAG._DEFAULT.name()), seq));
				}  
				createFlowScheduleJob(Common.getInstanceName(instanceName, seq), GlobalParam.tasks.get(Common.getInstanceName(instanceName, seq)),
						instanceConfig,needClear);
			}
		} catch (Exception e) {
			Common.LOG.error("Add Flow Govern "+instanceName+" Exception", e);
		}
	}

	public boolean jobAction(String mainName, String type, String actype) {
		String jobname = getJobName(mainName, type);
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
			Common.LOG.info("Success " + actype + " Job " + jobname);
		}else{
			Common.LOG.info("Fail " + actype + " Job " + jobname);
		} 
		return state;
	} 
 
	private boolean removeFlowScheduleJob(String instance,InstanceConfig instanceConfig)throws SchedulerException {
		boolean state = true;
		if (instanceConfig.getPipeParams().getFullCron() != null) { 
			state= jobAction(instance, GlobalParam.JOB_TYPE.FULL.name(), "remove") && state;
		}
		if(instanceConfig.getPipeParams().getFullCron() == null || instanceConfig.getPipeParams().getOptimizeCron()!=null){
			state = jobAction(instance, GlobalParam.JOB_TYPE.OPTIMIZE.name(), "remove") && state;
		}
		if(instanceConfig.getPipeParams().getDeltaCron() != null){
			state = jobAction(instance, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove") && state;
		}
		return state;
	}
	
	private void createInstructionScheduleJob(InstructionParam param, InstructionTask task) {
		JobModel _sj = new JobModel(
				getJobName(param.getId(), GlobalParam.JOB_TYPE.INSTRUCTION.name()), param.getCron(),
				"com.feiniu.task.InstructionTask", "runInstructions", task); 
		try {
			taskJobCenter.addJob(_sj); 
		}catch (Exception e) {
			Common.LOG.error("create Instruction Job "+param.getId()+" Exception", e);
		} 
	}

	private void createFlowScheduleJob(String instance, FlowTask task,
			InstanceConfig instanceConfig,boolean needclear)
			throws SchedulerException {
		String fullFun="runFull";
		String incrementFun="runIncrement";
		if(instanceConfig.getPipeParams().isMaster()) {
			fullFun="runMasterFull";
			incrementFun="runMasterIncrement";
		} 
		
		if (instanceConfig.getPipeParams().getFullCron() != null) { 
			if(needclear)
				jobAction(instance, GlobalParam.JOB_TYPE.FULL.name(), "remove"); 
			JobModel _sj = new JobModel(
					getJobName(instance, GlobalParam.JOB_TYPE.FULL.name()), instanceConfig.getPipeParams().getFullCron(),
					"com.feiniu.task.FlowTask", fullFun, task); 
			taskJobCenter.addJob(_sj); 
		}else if(instanceConfig.getPipeParams().getReadFrom()!= null && instanceConfig.getPipeParams().getWriteTo()!=null) { 
			if(needclear)
				jobAction(instance, GlobalParam.JOB_TYPE.FULL.name(), "remove");
			JobModel _sj = new JobModel(
					getJobName(instance,GlobalParam.JOB_TYPE.FULL.name()), not_run_cron,
					"com.feiniu.task.FlowTask", fullFun, task); 
			taskJobCenter.addJob(_sj); 
		}  
		
		if (instanceConfig.getPipeParams().getDeltaCron() != null) { 
			if(needclear)
				jobAction(instance, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove");
			
			String cron = instanceConfig.getPipeParams().getDeltaCron();
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
					getJobName(instance, GlobalParam.JOB_TYPE.INCREMENT.name()),
					instanceConfig.getPipeParams().getDeltaCron(), "com.feiniu.task.FlowTask",
					incrementFun, task); 
			taskJobCenter.addJob(_sj);
		}else if(instanceConfig.getPipeParams().getReadFrom()!= null && instanceConfig.getPipeParams().getWriteTo()!=null) {
			if(needclear)
				jobAction(instance, GlobalParam.JOB_TYPE.INCREMENT.name(), "remove");
			JobModel _sj = new JobModel(
					getJobName(instance,GlobalParam.JOB_TYPE.INCREMENT.name()),
					not_run_cron, "com.feiniu.task.FlowTask",
					incrementFun, task); 
			taskJobCenter.addJob(_sj);
		}
		
		if(instanceConfig.getPipeParams().getFullCron() == null || instanceConfig.getPipeParams().getOptimizeCron()!=null){
			if(needclear)
				jobAction(instance,GlobalParam.JOB_TYPE.OPTIMIZE.name(), "remove");
		
			String cron = instanceConfig.getPipeParams().getOptimizeCron()==null?default_cron.replace("PARAM",String.valueOf((int)(Math.random()*60))):instanceConfig.getPipeParams().getOptimizeCron();
			instanceConfig.getPipeParams().setOptimizeCron(cron);
			if(instanceConfig.getPipeParams().getInstanceName()==null) {
				createOptimizeJob(instance, task,cron); 
			} 
		}
	}
	
	private void createOptimizeJob(String indexName, FlowTask batch,String cron) throws SchedulerException{
		JobModel _sj = new JobModel(
				getJobName(indexName, GlobalParam.JOB_TYPE.OPTIMIZE.name()),cron,
				"com.feiniu.manager.Task", "optimizeInstance", batch); 
		taskJobCenter.addJob(_sj); 
	}

	private String getJobName(String instance, String type) { 
		return instance+"_"+type;
	}  
}
