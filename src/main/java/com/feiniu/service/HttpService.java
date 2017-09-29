package com.feiniu.service;

import java.util.HashMap;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * open http port
 * @author chengwen
 * @version 1.0 
 */
public class HttpService implements FNService {

	private HashMap<String, Object> serviceParams;

	private Server server = new Server();
	
	private final static Logger log = LoggerFactory
			.getLogger(HttpService.class);

	public static FNService getInstance(HashMap<String, Object> serviceParams){
		FNService s = new HttpService();
		s.init(serviceParams);
		return s;
	}
	
	@Override
	public void init(HashMap<String, Object> params) {
		this.serviceParams = params;
		QueuedThreadPool threadPool = new QueuedThreadPool();
		threadPool.setMaxThreads(Integer.valueOf((String) serviceParams
				.get("thread_pool")));
		server.setThreadPool(threadPool);

		SelectChannelConnector select_connector = new SelectChannelConnector();
		select_connector.setPort(Integer.valueOf((String) this.serviceParams
				.get("port")));
		select_connector.setMaxIdleTime(Integer.valueOf((String) this.serviceParams
				.get("max_idle_time")));
		select_connector.setConfidentialPort(Integer
				.valueOf((String) this.serviceParams
						.get("confident_port")));
		server.addConnector(select_connector);

		Handler shandle = (Handler) this.serviceParams.get("httpHandle");
		HandlerCollection handlers = new HandlerCollection();
		handlers.setHandlers(new Handler[] { shandle });
		server.setHandler(handlers);

		server.setStopAtShutdown(true);
		server.setSendServerVersion(true);
	}

	@Override
	public void close() {
		try {
			server.stop();
		} catch (Exception e) {
			log.error("close Exception,",e);
		}
	}

	@Override
	public void start() {
		try {
			server.start(); 
		} catch (Exception e) {
			log.error("start Exception,",e);
		} 
	} 
}
