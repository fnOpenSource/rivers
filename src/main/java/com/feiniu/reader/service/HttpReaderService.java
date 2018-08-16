package com.feiniu.reader.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.feiniu.config.GlobalParam;
import com.feiniu.instruction.flow.TransDataFlow;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.model.SearcherESModel;
import com.feiniu.model.SearcherModel;
import com.feiniu.model.param.TransParam;
import com.feiniu.model.param.WarehouseParam;
import com.feiniu.node.CPU;
import com.feiniu.searcher.service.SearcherService;
import com.feiniu.service.FNService;
import com.feiniu.service.HttpService;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;
import com.feiniu.util.MD5Util;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

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

	private FNService FS;

	public boolean start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", http_reader_confident_port);
		serviceParams.put("max_idle_time", http_reader_max_idle_time);
		serviceParams.put("port", http_reader_port);
		serviceParams.put("thread_pool", http_reader_thread_pool);
		serviceParams.put("httpHandle", new httpHandle());
		FS = HttpService.getInstance(serviceParams);
		FS.start();
		return true;
	}

	public boolean close() {
		if (FS != null) {
			FS.close();
		}
		return true;
	}

	public class httpHandle extends AbstractHandler {

		@Override
		public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) {
			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);

			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();

			String dataTo = rq.getPathInfo().substring(1);
			try {
				if (dataTo.length() < 1) {
					response.getWriter().println("{\"status\":0,\"info\":\"The write destination is empty!\"}");
					response.getWriter().flush();
					response.getWriter().close();
					return;
				}
				if (rq.getParameterMap().get("ac") != null && rq.getParameterMap().get("code") != null
						&& rq.getParameter("code").equals(MD5Util.SaltMd5(dataTo))) {
					switch (rq.getParameter("ac")) {
					case "add":
						if (rq.getParameterMap().get("data") != null && rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("type") != null && rq.getParameterMap().get("seq") != null
								&& rq.getParameterMap().get("keycolumn") != null
								&& rq.getParameterMap().get("updatecolumn") != null) {
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							String keycolumn = rq.getParameter("keycolumn");
							String updatecolumn = rq.getParameter("updatecolumn");
							boolean monopoly = false;
							if (rq.getParameterMap().get("fn_is_monopoly") != null
									&& rq.getParameter("fn_is_monopoly").equals("true"))
								monopoly = true;
							
							TransDataFlow transDataFlow = GlobalParam.SOCKET_CENTER.getTransDataFlow(instance, seq, false,monopoly?"_MOP":GlobalParam.DEFAULT_RESOURCE_TAG);
							if (transDataFlow == null || !transDataFlow.getInstanceConfig().getAlias().equals(dataTo)) {
								response.getWriter().println("{\"status\":0,\"info\":\"Writer get Error,Instance not exits!\"}");
								break;
							}
							String storeid;
							if (rq.getParameter("type").equals("full") && rq.getParameterMap().get("storeid") != null) {
								storeid = rq.getParameter("storeid");
							} else {
								storeid = Common.getStoreId(instance, seq, transDataFlow, true, false);
							}
							boolean isUpdate = false; 
							
							if (rq.getParameterMap().get("fn_is_update") != null
									&& rq.getParameter("fn_is_update").equals("true"))
								isUpdate = true;  
							
							try {
								String writeTo = transDataFlow.getInstanceConfig().getPipeParam().getInstanceName();
								if(writeTo==null) {
									writeTo = Common.getInstanceName(instance, seq);
								}
								transDataFlow.writeDataSet("HTTP PUT",
										writeTo,
										storeid, "", getJobPage(rq.getParameter("data"), keycolumn, updatecolumn,
												transDataFlow.getInstanceConfig().getTransParams()),
										"", isUpdate,monopoly);
								response.getWriter().println("{\"status\":1,\"info\":\"success\"}");
							} catch (Exception e) {
								Common.LOG.error("Http Write Exception,",e);
								response.getWriter().println("{\"status\":0,\"info\":\"写入失败!参数错误，instance:" + instance
										+ ",seq:" + seq + "\"}");
								try {
									throw new FNException("写入参数错误，instance:" + instance + ",seq:" + seq);
								} catch (FNException fe) {
									e.printStackTrace();
								}
							}
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"参数没有全部设置!\"}");
						}
						break;
					case "get_new_storeid":
						if (rq.getParameterMap().get("instance") != null && rq.getParameterMap().get("seq") != null) {
							TransDataFlow transDataFlow = GlobalParam.SOCKET_CENTER.getTransDataFlow(rq.getParameter("instance"),
									rq.getParameter("seq"), false,"");
							String storeid = Common.getStoreId(rq.getParameter("instance"), rq.getParameter("seq"),
									transDataFlow, false, false);
							CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition",true,rq.getParameter("instance"), storeid);   
							response.getWriter()
									.println("{\"status\":1,\"info\":\"success\",\"storeid\":\"" + storeid + "\"}");
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"创建新索引ID失败!\"}");
						}
						break;
					case "switch":
						if (rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("seq") != null) {
							String storeid;
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							TransDataFlow transDataFlow = GlobalParam.SOCKET_CENTER.getTransDataFlow(instance, seq, false,GlobalParam.DEFAULT_RESOURCE_TAG);
							if(rq.getParameterMap().get("storeid")!=null) {
								storeid = rq.getParameter("storeid");
							}else {
								storeid = Common.getStoreId(instance, seq,
										transDataFlow, false, false);
								CPU.RUN(transDataFlow.getID(), "Pond", "createStorePosition",true,instance, storeid);    
							} 
							CPU.RUN(transDataFlow.getID(), "Pond", "switchInstance",true, Common.getInstanceName(instance,seq),storeid);
							transDataFlow.run(instance, storeid, "-1", seq, true,transDataFlow.getInstanceConfig().getPipeParam().getInstanceName()==null?false:true);
							response.getWriter().println("{\"status\":1,\"info\":\"success\"}");
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"切换索引失败!\"}");
						}
						break;
					case "delete":
						if (rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("seq") != null && rq.getParameterMap().get("search_dsl") != null) { 
							SearcherModel<?, ?, ?> query=null;
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							TransDataFlow transFlow = GlobalParam.SOCKET_CENTER.getTransDataFlow(instance, seq, false,GlobalParam.DEFAULT_RESOURCE_TAG); 
							if (transFlow == null) {
								response.getWriter().println("{\"status\":0,\"info\":\"Writer get Error,Instance and seq Error!\"}");
								break;
							}
							String storeid = Common.getStoreId(instance,seq, transFlow, true, true);
							WarehouseParam param = GlobalParam.SOCKET_CENTER.getWHP(transFlow.getInstanceConfig().getPipeParam().getWriteTo());
							switch (param.getType()) {
							case ES:
								query = SearcherESModel.getInstance(SearcherService.parseRequest(rq), GlobalParam.SEARCH_ANALYZER,transFlow.getInstanceConfig());
								break; 
							default:
								break;
							}
							CPU.RUN(transFlow.getID(), "Pond", "deleteByQuery",true, query, instance, storeid);							 
							response.getWriter().println("{\"status\":1,\"info\":\"success\"}");
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"instance,seq,search_dsl not set!\"}");
						}
						break;

					default:
						response.getWriter().println("{\"status\":0,\"info\":\"action not exists!\"}");
						break;
					}

				} else {
					response.getWriter().println("{\"status\":0,\"info\":\"code is empty OR code not match!\"}");
				}
				response.getWriter().flush();
				response.getWriter().close();
			} catch (Exception e) {
				log.error("http Handle Exception", e);
			}
		}

		private HashMap<String, Object> getJobPage(Object data, String keycolumn, String updatecolumn,
				Map<String, TransParam> transParams) {
			HashMap<String, Object> jobPage = new HashMap<String, Object>();
			LinkedList<PipeDataUnit> datas = new LinkedList<PipeDataUnit>();
			jobPage.put(GlobalParam.READER_KEY, keycolumn);
			jobPage.put(GlobalParam.READER_SCAN_KEY, updatecolumn);
			jobPage.put(GlobalParam.READER_STATUS, true);
			jobPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
			JSONArray jr = JSONArray.fromObject(data);
			String maxId = null;
			String updateFieldValue = null;
			for (int j = 0; j < jr.size(); j++) {
				PipeDataUnit u = PipeDataUnit.getInstance();
				JSONObject jo = jr.getJSONObject(j);
				@SuppressWarnings("unchecked")
				Set<Entry<String, String>> itr = jo.entrySet();
				for (Entry<String, String> k : itr) {
					if (k.getKey().equals(jobPage.get(GlobalParam.READER_KEY))) {
						u.setKeyColumnVal(k.getValue());
						maxId = String.valueOf(k.getValue());
					}
					if (k.getKey().equals(jobPage.get(GlobalParam.READER_SCAN_KEY))) {
						updateFieldValue = String.valueOf(k.getValue());
					}
					u.addFieldValue(k.getKey(), k.getValue(), transParams);
				}
				datas.add(u);
			}
			if (updateFieldValue == null) {
				jobPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
			} else {
				jobPage.put(GlobalParam.READER_LAST_STAMP, updateFieldValue);
			}
			jobPage.put("maxId", maxId);
			jobPage.put("datas", datas);
			return jobPage;
		}
	}

}
