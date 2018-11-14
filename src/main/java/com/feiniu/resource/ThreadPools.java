package com.feiniu.resource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Running thread resources center
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:54
 */
public class ThreadPools {
	  
	ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	 
}
