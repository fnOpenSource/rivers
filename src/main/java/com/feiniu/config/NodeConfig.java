package com.feiniu.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feiniu.model.param.BasicParam;
import com.feiniu.model.param.FNParam;
import com.feiniu.model.param.NOSQLParam;
import com.feiniu.model.param.WriteParam;
import com.feiniu.model.param.MessageParam;
import com.feiniu.model.param.SQLParam;
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
	private Map<String, FNParam> readParamTypes;
	private TransParam transParam;
	private MessageParam messageParam ;
	private Map<String, WriteParam> writeParamTypes ;
	/**
	 *  1 do index 2 do rabitmq 4 do kafka
	 */
	private int indexType = 0;
	private String optimizeCron = "";
	private final static Logger log = LoggerFactory.getLogger(NodeConfig.class);

	public NodeConfig(String fileName, int indexType) {
		this.filename = fileName;
		this.indexType = indexType;
	}

	public void init() {
		this.readParamTypes = new HashMap<String, FNParam>();
		this.transParam = new TransParam();
		this.messageParam = new MessageParam();
		this.writeParamTypes = new LinkedHashMap<String, WriteParam>();
		loadConfigFromZk();
		log.info(filename + " config loaded");
	}

	public void reload() {
		log.info("starting reload " + filename);
		init();
	} 
	
	public boolean checkParam(String key, String value) {
		if (!readParamTypes.containsKey(key)) {
			return true;
		} else {
			return readParamTypes.get(key).isValid(value);
		}
	}

	public FNParam getParam(String key) {
		return this.readParamTypes.get(key);
	}

	public TransParam getTransParam() {
		return this.transParam;
	}

	public MessageParam getMessageParam() {
		return this.messageParam;
	}

	public Map<String, FNParam> getParamMap() {
		return this.readParamTypes;
	}

	public Map<String, WriteParam> getWriteParamMap() {
		return this.writeParamTypes;
	}

	public boolean isIndexer() {
		if((indexType & 1) > 0){
			if(transParam.getDataFrom()!=null && transParam.getWriteTo()!=null){
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

	public String getOptimizeCron() {
		return optimizeCron;
	}

	public void setOptimizeCron(String optimizeCron) {
		this.optimizeCron = optimizeCron;
	}
 
	private void loadConfigFromZk() {
		InputStream in;
		try {
			byte[] bt = ZKUtil.getData(this.filename);
			if (bt.length <= 0)
				return;
			in = new ByteArrayInputStream(bt, 0, bt.length);
			configParse(in);
			in.close();
		} catch (Exception e) {
			in = null;
			log.error("loadConfigFromZk error,",e);
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

				if (!params.getAttribute("optimizeCron").equals("")) {
					this.optimizeCron = params.getAttribute("optimizeCron");
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
						parseNode(tmp.getElementsByTagName("param"), "transParam", BasicParam.class);
					}else{
						log.error(this.filename+" config setting not correct");
						return;
					}
					if(doc.getElementsByTagName("pageSql").getLength() > 0)
						transParam.getSqlParam().setPageSql(doc.getElementsByTagName("pageSql").item(0).getFirstChild().getTextContent());
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
			parseNode(paramlist, "writeParamTypes", WriteParam.class);
			parseNode(paramlist, "FNParam", FNParam.class);

			params = (Element) doc.getElementsByTagName("searchParams").item(0);
			paramlist = params.getElementsByTagName("param");
			parseNode(paramlist, "FNParam", FNParam.class);
		} catch (Exception e) {
			log.error(this.filename+" configParse error,",e);
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
					case "writeParamTypes":
						WriteParam e = (WriteParam) o;
						writeParamTypes.put(e.getName(), e);
						break;
					case "transParam":
						BasicParam tbp = (BasicParam) o;
						transParam.setKeyValue(tbp.getName(), tbp.getValue());
						break;
					case "nosql":
						transParam.setNoSqlParam((NOSQLParam) o);
						break;
					case "dumpSql":
						transParam.setSqlParam((SQLParam) o);
						break; 
					case "MessageParam":
						messageParam = (MessageParam) o;
						break;
					case "MessageSql":
						messageParam.setSqlParam((SQLParam) o);
						break;
					default:
						FNParam f = (FNParam) o;
						readParamTypes.put(f.getAlias(), f);
						break;
					}
				}
			}
		}
	} 
}
