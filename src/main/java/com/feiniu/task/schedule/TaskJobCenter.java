package com.feiniu.task.schedule;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * shedule job manager
 * for add,remove,stop,restart,startonce manager
 * @author chengwen
 * @version 1.0 
 */
public class TaskJobCenter{
	private final static Logger log = LoggerFactory.getLogger(TaskJobCenter.class);  
	@Autowired
	Scheduler scheduler;

	public boolean addJob(JobModel job) throws SchedulerException {
		if (job == null){
			log.error("add null Job!");
			return false;
		} 

		TriggerKey triggerKey = TriggerKey.triggerKey(job.getJobName());
		CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

		if (trigger == null) {
			log.info("add Schedule Job " + job.getJobName());			
			JobDetail jobDetail = JobBuilder.newJob(JobRunFactory.class).withIdentity(job.getJobName()).build();
			jobDetail.getJobDataMap().put("scheduleJob", job);
			log.info(job.getJobName() + " isConcurrentExectionDisallowed=" +  jobDetail.isConcurrentExectionDisallowed());

			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCronExpression());
			trigger = TriggerBuilder.newTrigger().withIdentity(job.getJobName(),job.getJobName()).withSchedule(scheduleBuilder).build();
			scheduler.scheduleJob(jobDetail, trigger);
		} else {
			log.info("modify Schedule Job " + job.getJobName());
			CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(job.getCronExpression());
			trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).build();
			scheduler.rescheduleJob(triggerKey, trigger);
		}
		return true;
	}
	
	public boolean stopJob(String jobName){
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try { 
			scheduler.pauseJob(jobKey);
		} catch (Exception e) {
			log.error("SchedulerException stop Job",e);
			return false;
		} 	 
		return true;
	}
	
	public boolean startNow(String jobName){
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try {
			scheduler.triggerJob(jobKey);
		} catch (Exception e) {
			log.error("SchedulerException start do Job now",e);
			return false;
		}
		return true;
	}
	
	public boolean restartJob(String jobName){
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try {
			scheduler.resumeJob(jobKey);
		} catch (Exception e) {
			log.error("SchedulerException restart Job",e);
			return false;
		}
		return true;
	}

	public boolean deleteJob(String jobName) {   
		JobKey jobKey = JobKey.jobKey(jobName,"DEFAULT");
		try {
			scheduler.deleteJob(jobKey);
		} catch (Exception e) {
			log.error("SchedulerException delete Job",e);
			return false;
		} 
		return true;
    }  
}
