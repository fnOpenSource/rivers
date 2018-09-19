package com.feiniu.task.schedule;

import java.lang.reflect.Method;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.feiniu.config.GlobalParam;
import com.feiniu.util.Common;

/**
 * job run factory Disallow Concurrent run the same job
 * @author chengwen
 * @version 1.0 
 */  
public class JobRunFactory implements Job {  
	
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
        Class<?> CL = null;  
        if (object == null) {  
        	Common.LOG.error("job [" + scheduleJob.getJobName() + "] not exists!");  
            return false;  
        }  
        CL = object.getClass();  
        Method method = null;  
        try {  
            method = CL.getDeclaredMethod(scheduleJob.getMethodName());  
            method.invoke(object); 
            return true;
        } catch (Exception e) {  
        	Common.LOG.error(scheduleJob.getJobName()+" invokMethod "+method+" Exception ",e);  
        } 
        return false;
    }  

}  