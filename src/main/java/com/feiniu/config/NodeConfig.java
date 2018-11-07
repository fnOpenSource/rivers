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

import com.feiniu.config.GlobalParam.INSTANCE_TYPE;
import com.feiniu.config.GlobalParam.RESOURCE_TYPE;
import com.feiniu.param.pipe.InstructionParam;
import com.feiniu.param.warehouse.WarehouseNosqlParam;
import com.feiniu.param.warehouse.WarehouseSqlParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

/**
 * node configs center,manage flows config and sql/nosql dataflows configs
 * @author chengwen
 * @version 4.1
 * @date 2018-10-11 14:50
 */
public class NodeConfig { 
	
	private Map<String, InstanceConfig> instanceConfigs;
	private Map<String, InstanceConfig> searchConfigMap = new HashMap<String, InstanceConfig>();
	private Map<String, WarehouseSqlParam> sqlWarehouse = new HashMap<String, WarehouseSqlParam>();
	private Map<String, WarehouseNosqlParam> NoSqlWarehouse = new HashMap<String, WarehouseNosqlParam>(); 
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
			parsePondFile(GlobalParam.CONFIG_PATH + "/" +  this.pondFile);
			parseInstructionsFile(GlobalParam.CONFIG_PATH + "/" +  this.instructionsFile); 
		}  
		if(instances.trim().length()<1)
			return;
		for(String inst : instances.split(",")){
			String[] strs = inst.split(":");
			if (strs.length <= 0 || strs[0].length()<1)
				continue;
			int instanceType = INSTANCE_TYPE.Blank.getVal();
			String name = strs[0].trim();
			if(strs.length==2){
				instanceType = Integer.parseInt(strs[1]); 
			}
			String filename = GlobalParam.INSTANCE_PATH + "/" +  name  + "/" +  name + ".xml";
			InstanceConfig nconfig = new InstanceConfig(filename, instanceType);  
			nconfig.init();
			if(nconfig.getAlias().equals("")){
				nconfig.setAlias(name);
			}  
			nconfig.setName(name);
			this.searchConfigMap.put(nconfig.getAlias(), nconfig);
			this.instanceConfigs.put(name, nconfig);  
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
	
    public Map<String, WarehouseSqlParam> getSqlWarehouse() {
		return this.sqlWarehouse;
	}
    
	public Map<String, WarehouseNosqlParam> getNoSqlWarehouse() {
		return this.NoSqlWarehouse;
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
	
	public void addSource(RESOURCE_TYPE type,Object o) { 
		switch (type) {
		case SQL:
			WarehouseSqlParam e1 = (WarehouseSqlParam) o;
			sqlWarehouse.put(e1.getAlias(), e1);
			break;

		case NOSQL:
			WarehouseNosqlParam e2 = (WarehouseNosqlParam) o;
			NoSqlWarehouse.put(e2.getAlias(), e2);
			break;
			
		case INSTRUCTION:
			InstructionParam e3 = (InstructionParam) o;
			instructions.put(e3.getId(),e3);
			break; 
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
	
	private void parseNode(NodeList paramlist, Class<?> c) throws Exception 
	{
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) {
					Object o = Common.getXmlObj(param, c);
					if (c == WarehouseNosqlParam.class) {
						addSource(RESOURCE_TYPE.NOSQL,o);
					}else if(c == WarehouseSqlParam.class){
						addSource(RESOURCE_TYPE.SQL,o);
					}else if(c == InstructionParam.class) {
						addSource(RESOURCE_TYPE.INSTRUCTION,o);
					}
				}
			}
		}
	} 
}
