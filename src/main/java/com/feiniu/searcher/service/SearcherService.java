package com.feiniu.searcher.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.model.FNRequest;
import com.feiniu.model.FNResponse;
import com.feiniu.node.NodeCenter;
import com.feiniu.node.NodeMonitor;
import com.feiniu.searcher.FNSearcher;
import com.feiniu.service.FNService;
import com.feiniu.service.HttpService;

/**
 * searcher open http port support service
 * @author chengwen
 *
 */
public class SearcherService{
	 
	@Autowired
	private NodeCenter nodeCenter;  

	private final static Logger log = LoggerFactory
			.getLogger(SearcherService.class);

	@Autowired
	NodeMonitor indexManagerImpl;
 
	@Value("#{globalConfigBean['http_service_thread_pool']}")
	private String http_service_thread_pool;

	@Value("#{globalConfigBean['http_service_port']}")
	private String http_service_port;

	@Value("#{globalConfigBean['http_service_max_idle_time']}")
	private String http_service_max_idle_time;

	@Value("#{globalConfigBean['http_service_confident_port']}")
	private String http_service_confident_port;
	
	private FNService FS;
	 
	public boolean start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", http_service_confident_port);
		serviceParams.put("max_idle_time", http_service_max_idle_time);
		serviceParams.put("port", http_service_port);
		serviceParams.put("thread_pool", http_service_thread_pool);
		serviceParams.put("httpHandle", new httpHandle());
		FS=HttpService.getInstance(serviceParams);		
		FS.start();
		return true;
	}
	
	public boolean close(){
		if(FS!=null){
			FS.close();
		} 
		return true;
	}
  
	public FNRequest parseRequest(Object input) {
		FNRequest rq = FNRequest.getInstance(); 
		Request base_request = (Request) input;
		String path = base_request.getPathInfo();
		String handle = path.substring(1); 
		rq.setHandle(handle); 
		@SuppressWarnings("rawtypes")
		Iterator iter = base_request.getParameterMap().entrySet().iterator();
		while (iter.hasNext()) {
			@SuppressWarnings("rawtypes")
			Map.Entry entry = (Map.Entry) iter.next();
			String key = (String) entry.getKey();
			String value = base_request.getParameter(key);
			rq.addParam(key, value);
		}
		return rq;
	}
	
	public FNResponse process(FNRequest request) { 
		long startTime = System.currentTimeMillis();
		FNResponse response = null; 
		String handleName = request.getHandle(); 
		Map<String, NodeConfig> configMap = GlobalParam.nodeTreeConfigs.getSearchConfigs();
		if (configMap.containsKey(handleName)) { 
			FNSearcher searcher = nodeCenter.getSearcher(handleName);
			response = searcher.startSearch(request);
		} 
		long endTime = System.currentTimeMillis();
		if (response != null){
			response.setStartTime(startTime);
			response.setEndTime(endTime);
		}
		return response;
	}

	public class httpHandle extends AbstractHandler {
		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();

			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);

			rq.setHandled(true);
			FNRequest _request = parseRequest((Object) rq);

			if (GlobalParam.nodeTreeConfigs.getSearchConfigs().containsKey(
					_request.getHandle())) {
				FNResponse _response = null;
				try {
					_response = process(_request);
				} catch (Exception e) {
					log.error("httpHandle error,",e);
				}
				if (_response != null)
					response.getWriter().println(_response.toJson());
			} else {
				response.getWriter().println("The Alias is Not Exists OR Not Start Up!");
			}
			
			response.getWriter().flush();
			response.getWriter().close();
		}
	}  
}
