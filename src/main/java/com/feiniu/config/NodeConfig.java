package com.feiniu.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap; 
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feiniu.model.param.BasicParam;
import com.feiniu.model.param.MessageParam;
import com.feiniu.model.param.NOSQLParam;
import com.feiniu.model.param.SQLParam;
import com.feiniu.model.param.SearchParam;
import com.feiniu.model.param.PipeParam;
import com.feiniu.model.param.TransParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

/**
 * store single index node params
 * 
 * @author chengwen
 * @version 1.0 
 */
public class NodeConfig {

	private String filename;
	private String alias = "";
	private boolean status = true;
	private String name;
	private Map<String, TransParam> transParams;
	private Map<String,SearchParam> searchParams;
	private PipeParam pipeParam;
	private MessageParam messageParam ; 
	/**
	 *  1 do index 2 do rabitmq 4 do kafka
	 */
	private int indexType = 0;  

	public NodeConfig(String fileName, int indexType) {
		this.filename = fileName;
		this.indexType = indexType;
	}

	public void init() { 
		this.pipeParam = new PipeParam();
		this.transParams = new HashMap<String, TransParam>(); 
		this.searchParams = new HashMap<String, SearchParam>();
		this.messageParam = new MessageParam(); 
		loadConfigFromZk();
		GlobalParam.LOG.info(filename + " config loaded");
	}

	public void reload() {
		GlobalParam.LOG.info("starting reload " + filename);
		init();
	} 
	
	public boolean checkTransParam(String key, String value) {
		if (!transParams.containsKey(key)) {
			return true;
		} else {
			return transParams.get(key).isValid(value);
		}
	}

	public TransParam getTransParam(String key) {
		return this.transParams.get(key);
	}
	
	public SearchParam getSearchParam(String key) {
		return this.searchParams.get(key);
	}

	public PipeParam getPipeParam() {
		return this.pipeParam;
	}

	public MessageParam getMessageParam() {
		return this.messageParam;
	} 

	public Map<String, TransParam> getTransParams() {
		return this.transParams;
	}
 
	public boolean isIndexer() {
		if((indexType & 1) > 0){
			if(pipeParam.getDataFrom()!=null && pipeParam.getWriteTo()!=null){
				return true;
			}
		}
		return false;
	}
	
	public int getIndexType(){
		return this.indexType;
	}

	public boolean hasKafka() {
		return (indexType & 4) > 0 && messageParam.getHandler() != null ? true
				: false;
	}

	public boolean hasRabitmq() {
		return (indexType & 2) > 0 && messageParam.getHandler() != null ? true
				: false;
	}
	
	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getAlias() {
		return this.alias;
	} 
	
	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return this.name;
	} 
 
	public boolean checkStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	private void loadConfigFromZk() {
		InputStream in;
		try {
			byte[] bt = ZKUtil.getData(this.filename,false);
			if (bt.length <= 0)
				return;
			in = new ByteArrayInputStream(bt, 0, bt.length);
			configParse(in);
			in.close();
		} catch (Exception e) {
			in = null;
			setStatus(false);
			GlobalParam.LOG.error("loadConfigFromZk error,",e);
		}
	}
	
	/**
	 * node xml config parse
	 * searchparams store in readParamTypes all can for search
	 */
	private void configParse(InputStream in) {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);

			Element params;
			NodeList paramlist;
			Element tmp;

			params = (Element) doc.getElementsByTagName("dataflow").item(0);

			if (params != null) { 
				if (!params.getAttribute("alias").equals("")) {
					this.alias = params.getAttribute("alias");
				} 
			 
				paramlist = params.getElementsByTagName("configs");
				if (paramlist.getLength() > 0) {
					tmp = (Element) doc.getElementsByTagName("configs").item(0);
					parseNode(tmp.getElementsByTagName("sql"), "dumpSql",
							SQLParam.class); 
					parseNode(tmp.getElementsByTagName("nosql"), "nosql",
							NOSQLParam.class);
					tmp = (Element) doc.getElementsByTagName("TransParam").item(0); 
					if (tmp!=null) {
						parseNode(tmp.getElementsByTagName("param"), "pipeParam", BasicParam.class);
					}else{
						GlobalParam.LOG.error(this.filename+" config setting not correct");
						return;
					}
					if(doc.getElementsByTagName("pageSql").getLength() > 0) {
						SQLParam _sq = (SQLParam) Common.getNode2Obj(doc.getElementsByTagName("pageSql").item(0), SQLParam.class);
						pipeParam.getSqlParam().setPageSql(_sq.getPageSql());
					} 
				}

				paramlist = params.getElementsByTagName("message");
				if (paramlist.getLength() > 0) {
					parseNode(paramlist, "MessageParam", MessageParam.class);
					tmp = (Element) doc.getElementsByTagName("message").item(0);
					parseNode(tmp.getElementsByTagName("sql"), "MessageSql",
							SQLParam.class);
				}
			}

			params = (Element) doc.getElementsByTagName("fields").item(0);
			paramlist = params.getElementsByTagName("field");
			parseNode(paramlist, "TransParam", TransParam.class); 

			params = (Element) doc.getElementsByTagName("searchParams").item(0);
			paramlist = params.getElementsByTagName("param");
			parseNode(paramlist, "SearchParam", SearchParam.class);
		} catch (Exception e) {
			setStatus(false);
			GlobalParam.LOG.error(this.filename+" configParse error,",e);
		}
	}
	
	private void parseNode(NodeList paramlist, String type, Class<?> c)
			throws Exception {
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) {
					Object o = Common.getNode2Obj(param, c);
					switch (type) {
					case "TransParam":
						TransParam e = (TransParam) o;
						transParams.put(e.getName(), e);
						break;
					case "pipeParam":
						BasicParam tbp = (BasicParam) o;
						pipeParam.setKeyValue(tbp.getName(), tbp.getValue());
						break;
					case "nosql":
						pipeParam.setNoSqlParam((NOSQLParam) o);
						break;
					case "dumpSql":
						pipeParam.setSqlParam((SQLParam) o);
						break; 
					case "MessageParam":
						messageParam = (MessageParam) o;
						break;
					case "MessageSql":
						messageParam.setSqlParam((SQLParam) o);
						break;
					case "SearchParam":
						SearchParam v  = (SearchParam) o;
						searchParams.put(v.getName(), v);
						break;  
					default: 
						break;
					}
				}
			}
		}
	} 
}
