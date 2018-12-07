package com.feiniu.yarn;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.feiniu.config.NodeConfig;
import com.feiniu.model.RiverState;
import com.feiniu.node.FlowCenter;
import com.feiniu.node.NodeMonitor;
import com.feiniu.node.SocketCenter;
import com.feiniu.node.startup.Run;
import com.feiniu.task.FlowTask;
import com.feiniu.util.email.FNEmailSender;

/**
 * Statistics current node resources
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:53
 */
public final class Resource {

	public static SocketCenter SOCKET_CENTER;
	
	public static FlowCenter FlOW_CENTER;
	
	public static NodeMonitor nodeMonitor; 
	
	public static FNEmailSender mailSender; 
	
	public static NodeConfig nodeConfig;
	
	public static Run RIVERS;
	
	public final static RiverState<AtomicInteger> FLOW_STATUS = new RiverState<>();
	/**FLOW_INFOS store current flow running state information*/
	public final static RiverState<HashMap<String,String>> FLOW_INFOS = new RiverState<HashMap<String,String>>();

	public static HashMap<String, FlowTask> tasks; 
	
	public final static ThreadPools ThreadPools = new ThreadPools();
	
	public void collectNodeResource() {
		
	}
	 
}
