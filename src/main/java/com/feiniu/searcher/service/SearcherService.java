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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.feiniu.config.GlobalParam;
import com.feiniu.config.InstanceConfig;
import com.feiniu.model.SearcherRequest;
import com.feiniu.model.SearcherState;
import com.feiniu.node.SocketCenter;
import com.feiniu.searcher.Searcher;
import com.feiniu.service.FNService;
import com.feiniu.service.HttpService;
import com.feiniu.util.Common;

/**
 * searcher open http port support service
 * @author chengwen
 *
 */
public class SearcherService{
	 
	@Autowired
	private SocketCenter SocketCenter;   
 
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
  
	public static SearcherRequest parseRequest(Object input) {
		SearcherRequest rq = SearcherRequest.getInstance(); 
		Request base_request = (Request) input;
		String path = base_request.getPathInfo();
		String pipe = path.substring(1); 
		rq.setPipe(pipe);  
		@SuppressWarnings("unchecked")
		Iterator<Map.Entry<String,String>> iter = base_request.getParameterMap().entrySet().iterator();
		while (iter.hasNext()) { 
			Map.Entry<String,String> entry = iter.next();
			String key = (String) entry.getKey();
			String value = base_request.getParameter(key);
			rq.addParam(key, value);
		}
		return rq;
	}
	
	public SearcherState process(SearcherRequest request) { 
		long startTime = System.currentTimeMillis();
		SearcherState response = null; 
		String pipe = request.getPipe(); 
		Map<String, InstanceConfig> configMap = GlobalParam.nodeConfig.getSearchConfigs();
		if (configMap.containsKey(pipe)) { 
			Searcher searcher = SocketCenter.getSearcher(pipe,"","");
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
			SearcherRequest _request = parseRequest((Object) rq);

			if (GlobalParam.nodeConfig.getSearchConfigs().containsKey(
					_request.getPipe())) {
				SearcherState _response = null;
				try {
					_response = process(_request);
				} catch (Exception e) {
					Common.LOG.error("httpHandle error,",e);
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
