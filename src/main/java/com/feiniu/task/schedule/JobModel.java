package com.feiniu.task.schedule;

/**
 * ScheduleJob model
 * @author chengwen
 * @version 1.0 
 */
public class JobModel {  
	   
    private String jobName;   
    private String cronExpression;  
    /** 
     * 任务执行时调用哪个类的方法 包名+类名 
     */  
    private String className;  
    /** 
     * 任务调用的方法名 
     */  
    private String methodName;
    /** 
     * 执行对象
     */ 
    private Object object;
    
    public JobModel(String jobName, String cronExpression, String className, String methodName, Object object){
    	this.jobName = jobName;
    	this.cronExpression = cronExpression;
    	this.className = className;
    	this.methodName = methodName;
    	this.object = object;
    }
    
	public String getJobName() {
		return jobName;
	}
	public String getCronExpression() {
		return cronExpression;
	}
	public String getClassName() {
		return className;
	}
	public String getMethodName() {
		return methodName;
	}
	public Object getObject() {
		return object;
	}
}  
