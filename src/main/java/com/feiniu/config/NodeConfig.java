package com.feiniu.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.feiniu.model.param.InstructionParam;
import com.feiniu.model.param.WarehouseNosqlParam;
import com.feiniu.model.param.WarehouseSqlParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

/**
 * node configs center,manage flows config and sql/nosql dataflows configs
 * @author chengwen
 * @version 1.0 
 */
public class NodeConfig { 
	
	private Map<String, InstanceConfig> instanceConfigs;
	private Map<String, InstanceConfig> searchConfigMap = new HashMap<String, InstanceConfig>();
	private Map<String, WarehouseSqlParam> SqlParamMap = new HashMap<String, WarehouseSqlParam>();
	private Map<String, WarehouseNosqlParam> NoSqlParamMap = new HashMap<String, WarehouseNosqlParam>(); 
	private Map<String,InstructionParam> instructions = new HashMap<String, InstructionParam>();
	private String pondFile = null; 
	private String instructionsFile = null; 
	
/**
 * 
 * @param instances
 * @param pondFile
 * @param instructionsFile
 * @return
 */
	public static NodeConfig getInstance(String instances, String pondFile,String instructionsFile) { 
		    NodeConfig o = new NodeConfig(); 
		    o.pondFile = pondFile;
		    o.instructionsFile = instructionsFile; 
			o.loadConfig(instances,true);
			return o;
	}
	
	public void loadConfig(String instances,boolean reset){
		if(reset){
			this.instanceConfigs = new HashMap<String, InstanceConfig>();
		} 
		parsePondFile(GlobalParam.CONFIG_PATH + "/" +  this.pondFile);
		parseInstructionsFile(GlobalParam.CONFIG_PATH + "/" +  this.instructionsFile); 
		if(instances.trim().length()<1)
			return;
		for(String inst : instances.split(",")){
			String[] strs = inst.split(":");
			if (strs.length <= 0)
				continue;
			int indexType=0;
			String name = strs[0].trim();
			if(strs.length==2){
				indexType = Integer.parseInt(strs[1]); 
			}
			String filename = GlobalParam.CONFIG_PATH + "/" +  name  + "/" +  name + ".xml";
			InstanceConfig nconfig = new InstanceConfig(filename, indexType);  
			nconfig.init();
			if(nconfig.getAlias().equals("")){
				nconfig.setAlias(name);
			}  
			nconfig.setName(name);
			this.searchConfigMap.put(nconfig.getAlias(), nconfig);
			this.instanceConfigs.put(name, nconfig);  
		}	  
	}
	
	private void parseInstructionsFile(String src){
		InputStream in = null;
		try {
		    byte[] bt = ZKUtil.getData(src,false);
		    if (bt.length <= 0)
		    	return; 
			in = new ByteArrayInputStream(bt, 0, bt.length);  
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);
			
			Element sets = (Element) doc.getElementsByTagName("sets").item(0);
			NodeList paramlist = sets.getElementsByTagName("instruction");
			parseNode(paramlist,InstructionParam.class); 
			
		} catch (Exception e) {
			Common.LOG.error("parse (" + src + ") error,",e);
		} finally{
			try {
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				Common.LOG.error("parse (" + src + ") error,",e);
			}
		}
	}
	
	private void parsePondFile(String src){
		InputStream in = null;
		try {
		    byte[] bt = ZKUtil.getData(src,false);
		    if (bt.length <= 0)
		    	return; 
			in = new ByteArrayInputStream(bt, 0, bt.length);  
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(in);
			
			Element NoSql = (Element) doc.getElementsByTagName("NoSql").item(0);
			NodeList paramlist = NoSql.getElementsByTagName("socket");
			parseNode(paramlist,WarehouseNosqlParam.class); 
		 
			Element Sql = (Element) doc.getElementsByTagName("Sql").item(0);
			paramlist = Sql.getElementsByTagName("socket");
			parseNode(paramlist,WarehouseSqlParam.class); 
			
		} catch (Exception e) {
			Common.LOG.error("parse (" + src + ") error,",e);
		} finally{
			try {
				if (null != in) {
					in.close();
				}
			} catch (Exception e) {
				Common.LOG.error("parse (" + src + ") error,",e);
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
					}else if(c == InstructionParam.class) {
						InstructionParam e = (InstructionParam) o;
						instructions.put(e.getId(),e);
					}
				}
			}
		}
	}
	
	public Map<String, InstructionParam> getInstructions(){
		return this.instructions;
	}
	
	public Map<String, InstanceConfig> getInstanceConfigs(){
		return this.instanceConfigs;
	}
	
	public Map<String, InstanceConfig> getSearchConfigs(){
		return this.searchConfigMap;
	}
	
    public Map<String, WarehouseSqlParam> getSqlParamMap() {
		return this.SqlParamMap;
	}
    
	public Map<String, WarehouseNosqlParam> getNoSqlParamMap() {
		return this.NoSqlParamMap;
	}

	public void init(){
		for(Map.Entry<String, InstanceConfig> e : this.instanceConfigs.entrySet()){  
			this.searchConfigMap.put(e.getValue().getAlias(), e.getValue());
		}
	}
	
	public void reload() {
		for(Map.Entry<String, InstanceConfig> e : this.instanceConfigs.entrySet()){
			e.getValue().reload();
		}
	} 
}
