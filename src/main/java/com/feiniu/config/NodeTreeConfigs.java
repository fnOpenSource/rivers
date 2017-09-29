package com.feiniu.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

/**
 * node configs center,manage flows config and sql/nosql dataflows configs
 * @author chengwen
 * @version 1.0 
 */
public class NodeTreeConfigs { 
	
	private Map<String, NodeConfig> configMap;
	private Map<String, NodeConfig> searchConfigMap = new HashMap<String, NodeConfig>();
	private Map<String, WarehouseSqlParam> SqlParamMap = new HashMap<String, WarehouseSqlParam>();
	private Map<String, WarehouseNosqlParam> NoSqlParamMap = new HashMap<String, WarehouseNosqlParam>(); 
	private String noSqlFileName = null;
	private String sqlFileName = null;
	
	private final static Logger log = LoggerFactory.getLogger(NodeTreeConfigs.class);   

	/**
	 * load config from zk
	 * @param cfgPath
	 * @param instances
	 * @param commonParam
	 * @param noSqlFileName
	 * @param sqlFileName
	 * @param zkHost
	 */
	public NodeTreeConfigs(String cfgPath, String instances, String write_batch,String pool_size, String noSqlFileName,
			String sqlFileName,String zkHost) {
		    ZKUtil.setZkHost(zkHost);
		    GlobalParam.CONFIG_PATH = cfgPath;
		    this.noSqlFileName = noSqlFileName;
		    this.sqlFileName = sqlFileName;
		    GlobalParam.POOL_SIZE = Integer.parseInt(pool_size);
		    GlobalParam.WRITE_BATCH = write_batch.equals("false")?false:true;
			this.loadConfig(instances,true,false);
	}
	
	public void loadConfig(String instances,boolean reset,boolean init){
		if(reset){
			configMap = new HashMap<String, NodeConfig>();
		} 
		parseDatasConfig(GlobalParam.CONFIG_PATH + "/" +  this.noSqlFileName,"nosql");
		parseDatasConfig(GlobalParam.CONFIG_PATH + "/" +  this.sqlFileName,"sql");
		for(String inst : instances.split(",")){
			String[] strs = inst.split(":");
			if (strs.length <= 0)
				continue;
			int indexType=0;
			String name = strs[0];
			if(strs.length==2){
				indexType = Integer.parseInt(strs[1]); 
			}
			String filename = GlobalParam.CONFIG_PATH + "/" +  name  + "/" +  name + ".xml";
			NodeConfig nconfig = new NodeConfig(filename, indexType); 
			if(init){
				nconfig.init();
			}
			configMap.put(name, nconfig);  
		}	  
	}
	
	private void parseDatasConfig(String dataSrc,String type){
		InputStream in = null;
		try {
		    byte[] bt = ZKUtil.getData(dataSrc);
		    if (bt.length <= 0)
		    	return; 
			in = new ByteArrayInputStream(bt, 0, bt.length);  
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);
			if(type.equals("nosql")){
				Element params = (Element) doc.getElementsByTagName("services").item(0);
				NodeList paramlist = params.getElementsByTagName("group");
				parseNode(paramlist,WarehouseNosqlParam.class); 
			}else{
				Element params = (Element) doc.getElementsByTagName("databases").item(0);
				NodeList paramlist = params.getElementsByTagName("database");
				parseNode(paramlist,WarehouseSqlParam.class); 
			} 
		} catch (Exception e) {
			log.error("parseNoSqlConfig(" + dataSrc + ") error,",e);
		} finally{
			try {
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				log.error("parseNoSqlConfig(" + dataSrc + ") error,",e);
			}
		}
	} 
	
	private void parseNode(NodeList paramlist, Class<?> c)
			throws NoSuchMethodException, SecurityException, IllegalAccessException, 
			IllegalArgumentException,InvocationTargetException, InstantiationException 
	{
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) {
					Object o = Common.getNode2Obj(param, c);
					if (c == WarehouseNosqlParam.class) {
						WarehouseNosqlParam e = (WarehouseNosqlParam) o;
						NoSqlParamMap.put(e.getAlias(), e);
					}else if(c == WarehouseSqlParam.class){
						WarehouseSqlParam e = (WarehouseSqlParam) o;
						SqlParamMap.put(e.getAlias(), e);
					}
				}
			}
		}
	}
	public Map<String, NodeConfig> getConfigMap(){
		return this.configMap;
	}
	
	public Map<String, NodeConfig> getSearchConfigs(){
		return this.searchConfigMap;
	}
	
    public Map<String, WarehouseSqlParam> getSqlParamMap() {
		return this.SqlParamMap;
	}
    
	public Map<String, WarehouseNosqlParam> getNoSqlParamMap() {
		return this.NoSqlParamMap;
	}

	public void init(){
		for(Map.Entry<String, NodeConfig> e : configMap.entrySet()){
			e.getValue().init();
			if(e.getValue().getAlias().equals("")){
				e.getValue().setAlias(e.getKey());
			}
			searchConfigMap.put(e.getValue().getAlias(), e.getValue());
		}
	}
	
	public void reload() {
		for(Map.Entry<String, NodeConfig> e : configMap.entrySet()){
			e.getValue().reload();
		}
	} 
}
