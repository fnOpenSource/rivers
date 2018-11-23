package com.feiniu.yarn;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.feiniu.config.GlobalParam;
import com.feiniu.task.JobPage;
import com.feiniu.util.Common;

/**
 * Running thread resources center
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:54
 */
public class ThreadPools {

	private ArrayBlockingQueue<JobPage> waitJob = new ArrayBlockingQueue<>(GlobalParam.POOL_SIZE * 10);

	private int maxThreadNums = GlobalParam.POOL_SIZE;

	ThreadPoolExecutor cachedThreadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            30L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());

	public void submitJobPage(JobPage jobPage) {
		try {
			waitJob.put(jobPage);
		} catch (Exception e) {
			Common.LOG.error("SubmitJobPage Exception", e);
		}
	}
	
	public void cleanWaitJob(int id) {
		Iterator<JobPage> iter = waitJob.iterator();
		JobPage jp;
        while(iter.hasNext()) {
        	jp = iter.next();
        	if(jp.getId()==id)
        		waitJob.remove(jp);
        }
	}

	public void start() {
		try {
			while(true) {
				JobPage jp = waitJob.take();
				while(cachedThreadPool.getTaskCount()>=maxThreadNums) {
					Thread.sleep(900);
				} 
				cachedThreadPool.execute(()->runJobPage(jp));
			}  
		} catch (Exception e) {
			Common.LOG.error("start run JobPage Exception", e);
		}
	}
	
	private static void runJobPage(JobPage jp) {
		
	}
}
