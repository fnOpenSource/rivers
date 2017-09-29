package com.feiniu.reader.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.feiniu.config.GlobalParam;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;
import com.feiniu.service.FNService;
import com.feiniu.service.HttpService;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.util.MD5Util;
import com.feiniu.writer.flow.JobWriter;

/**
 * Reader open http port support data read
 * 
 * @author chengwen
 * @version 1.0
 */
public class HttpReaderService {

	@Value("#{globalConfigBean['http_reader_thread_pool']}")
	private String http_reader_thread_pool;

	@Value("#{globalConfigBean['http_reader_port']}")
	private String http_reader_port;

	@Value("#{globalConfigBean['http_reader_max_idle_time']}")
	private String http_reader_max_idle_time;

	@Value("#{globalConfigBean['http_reader_confident_port']}")
	private String http_reader_confident_port;
	
	private final static Logger log = LoggerFactory.getLogger(HttpReaderService.class);
	
	private static ConcurrentHashMap<String, Boolean> reCompute = new ConcurrentHashMap<String, Boolean>();

	private FNService FS;
	
	public boolean start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", http_reader_confident_port);
		serviceParams.put("max_idle_time", http_reader_max_idle_time);
		serviceParams.put("port", http_reader_port);
		serviceParams.put("thread_pool", http_reader_thread_pool);
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

	public class httpHandle extends AbstractHandler {

		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) {
			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);

			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();

			String dataTo = rq.getPathInfo().substring(1);
			try {
				if (dataTo.length() < 1) {
					response.getWriter()
							.println(
									"{\"status\":0,\"info\":\"The write destination is empty!\"}");
					response.getWriter().flush();
					response.getWriter().close();
					return;
				}
				if (rq.getParameterMap().get("ac") != null
						&& rq.getParameterMap().get("code") != null
						&& rq.getParameter("code").equals(
								MD5Util.SaltMd5(dataTo))) {
					switch (rq.getParameter("ac")) {
					case "add":
						if (rq.getParameterMap().get("data") != null
								&& rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("type") != null
								&& rq.getParameterMap().get("seq") != null
								&& rq.getParameterMap().get("keycolumn") != null
								&& rq.getParameterMap().get("updatecolumn") != null) {
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							String keycolumn = rq.getParameter("keycolumn");
							String updatecolumn = rq.getParameter("updatecolumn");
							JobWriter coreWriter = GlobalParam.NODE_CENTER
									.getWriterChannel(instance, seq,false);
							if (coreWriter == null) {
								response.getWriter().println(
										"{\"status\":0,\"info\":\"参数错误!\"}");
								break;
							}
							String storeid;
							if (rq.getParameter("type").equals("full")
									&& rq.getParameterMap().get("storeid") != null) {
								storeid = rq.getParameter("storeid");
							} else { 
								if(reCompute.containsKey(instance+seq)){
									storeid = Common.getStoreId(instance, seq,
											coreWriter, true, false);
								}else{
									reCompute.put(instance+seq, true);
									storeid = Common.getStoreId(instance, seq,
											coreWriter, true, true);
								} 
							}
							boolean isUpdate = false;
							if(rq.getParameterMap().get("fn_is_update") != null && rq.getParameter("fn_is_update").equals("true"))
								isUpdate = true;
							try {
								coreWriter.writeDataSet(
										"HTTP PUT",
										Common.getInstanceName(instance, seq),
										storeid,"",
										getJobPage(rq.getParameter("data"),keycolumn,updatecolumn,coreWriter.getWriteParamMap()), "",isUpdate);
								response.getWriter().println(
										"{\"status\":1,\"info\":\"success\"}");
							} catch (Exception e) {
								e.printStackTrace();
								response.getWriter().println(
										"{\"status\":0,\"info\":\"写入失败!参数错误，instance:"
												+ instance + ",seq:" + seq
												+ "\"}");
								try {
									throw new FNException("写入参数错误，instance:"
											+ instance + ",seq:" + seq);
								} catch (FNException fe) {
								}
							}
						} else {
							response.getWriter().println(
									"{\"status\":0,\"info\":\"参数没有全部设置!\"}");
						}
						break;
					case "get_new_storeid":
						if (rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("seq") != null) {
							JobWriter coreWriter = GlobalParam.NODE_CENTER
									.getWriterChannel(
											rq.getParameter("instance"),
											rq.getParameter("seq"),false);
							String storeid = Common.getStoreId(
									rq.getParameter("instance"),
									rq.getParameter("seq"), coreWriter, false,
									false);
							coreWriter.createStorePosition(
									rq.getParameter("instance"), storeid);
							response.getWriter().println(
									"{\"status\":1,\"info\":\"success\",\"storeid\":\""
											+ storeid + "\"}");
						} else {
							response.getWriter().println(
									"{\"status\":0,\"info\":\"创建新索引ID失败!\"}");
						}
						break;
					case "switch":
						if (rq.getParameterMap().get("storeid") != null
								&& rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("seq") != null) {
							GlobalParam.NODE_CENTER.getWriterChannel(
									rq.getParameter("instance"),
									rq.getParameter("seq"),false).switchSearcher(
									rq.getParameter("instance"),
									rq.getParameter("storeid"));
							response.getWriter().println(
									"{\"status\":1,\"info\":\"success\"}");
						} else {
							response.getWriter().println(
									"{\"status\":0,\"info\":\"切换索引失败!\"}");
						}
						break;

					default:
						response.getWriter().println(
										"{\"status\":0,\"info\":\"action not exists!\"}");
						break;
					}

				} else {
					response.getWriter()
							.println(
									"{\"status\":0,\"info\":\"code is empty OR code not match!\"}");
				}
				response.getWriter().flush();
				response.getWriter().close();
			} catch (Exception e) {
				log.error("http Handle Exception",e);
			}
		}

		private HashMap<String, Object> getJobPage(Object data, String keycolumn,String updatecolumn,Map<String, WriteParam> writeParamMap) {
			HashMap<String, Object> jobPage = new HashMap<String, Object>();
			LinkedList<WriteUnit> datas = new LinkedList<WriteUnit>();
			jobPage.put("keyColumn",keycolumn);
			jobPage.put("IncrementColumn",updatecolumn);  
			jobPage.put("lastUpdateTime", System.currentTimeMillis());
			JSONArray jr = JSONArray.fromObject(data);
			String maxId = null;
			String updateFieldValue=null;
			for (int j = 0; j < jr.size(); j++) {
				WriteUnit u = WriteUnit.getInstance();
				JSONObject jo = jr.getJSONObject(j);
				@SuppressWarnings("unchecked")
				Set<Entry<String, String>> itr = jo.entrySet();
				for (Entry<String, String> k : itr) {
					if(k.getKey().equals(jobPage.get("keyColumn"))){
						u.setKeyColumnVal(k.getValue());
						maxId = String.valueOf(k.getValue());
					}
					if(k.getKey().equals(jobPage.get("IncrementColumn"))){
						updateFieldValue = String.valueOf(k.getValue());
					} 
					u.addFieldValue(k.getKey(), k.getValue(), writeParamMap);
				}
				datas.add(u);
			}
			if (updateFieldValue==null){ 
				jobPage.put("lastUpdateTime", System.currentTimeMillis()); 
			}else{
				jobPage.put("lastUpdateTime", updateFieldValue); 
			}
			jobPage.put("maxId", maxId);
			jobPage.put("datas", datas);
			return jobPage;
		}
	}

}
