package com.feiniu.task.schedule;

import java.lang.reflect.Method;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;

/**
 * job run factory Disallow Concurrent run the same job
 * @author chengwen
 * @version 1.0 
 */
@DisallowConcurrentExecution  
public class JobRunFactory implements Job {  	
	private final static Logger log = LoggerFactory.getLogger(JobRunFactory.class);  
	
    @Override  
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobModel job = (JobModel) context.getMergedJobDataMap().get("scheduleJob"); 
        if(!invokeMethod(job)){
        	GlobalParam.mailSender.sendHtmlMailBySynchronizationMode(
					" [SearchPlatForm] " + GlobalParam.run_environment,
					"job [" + job.getJobName() + "] is stopped,with invokeMethod error!"); 
        }
    }  
    
    private boolean invokeMethod(JobModel scheduleJob) {   
        Object object = scheduleJob.getObject();  
        Class<?> clazz = null;  
        if (object == null) {  
            log.error("job [" + scheduleJob.getJobName() + "] not exists!");  
            return false;  
        }  
        clazz = object.getClass();  
        Method method = null;  
        try {  
            method = clazz.getDeclaredMethod(scheduleJob.getMethodName());  
            method.invoke(object); 
            return true;
        } catch (Exception e) {  
            log.error(scheduleJob.getJobName()+" invokMethod "+method+" Exception ",e);  
        } 
        return false;
    }  

}  