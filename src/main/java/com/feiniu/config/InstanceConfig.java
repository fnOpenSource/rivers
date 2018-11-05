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
import com.feiniu.field.RiverField;
import com.feiniu.param.BasicParam;
import com.feiniu.param.end.MessageParam;
import com.feiniu.param.end.SearcherParam;
import com.feiniu.param.ml.ComputeParam;
import com.feiniu.param.pipe.PipeParam;
import com.feiniu.param.warehouse.NoSQLParam;
import com.feiniu.param.warehouse.SQLParam;
import com.feiniu.util.Common;
import com.feiniu.util.ZKUtil;

/**
 * instance configs loading 
 * @author chengwen
 * @version 3.1 
 * @date 2018-10-11 15:13
 */
public class InstanceConfig {

	private String filename;
	private String alias = "";
	private boolean status = true;
	private String name;
	private volatile Map<String, RiverField> writeFields;
	private volatile Map<String, RiverField> computeFields;
	private volatile Map<String,SearcherParam> searchParams;
	private volatile PipeParam pipeParams;
	private volatile MessageParam messageParam ; 
	private volatile ComputeParam computeParams;
	private int instanceType = INSTANCE_TYPE.Blank.getVal();  

	public InstanceConfig(String fileName, int instanceType) {
		this.filename = fileName; 
		this.instanceType = instanceType;
	}

	public void init() { 
		this.pipeParams = new PipeParam();
		this.writeFields = new HashMap<>();
		this.computeFields = new HashMap<>();
		this.searchParams = new HashMap<>();
		this.messageParam = new MessageParam();
		this.computeParams = new ComputeParam();
		loadConfigFromZk();
		Common.LOG.info(filename + " config loaded");
	}

	public void reload() {
		Common.LOG.info("starting reload " + filename);
		init();
	} 
	
	public boolean checkWriteField(String key, String value) {
		if (!writeFields.containsKey(key)) {
			return true;
		} else {
			return writeFields.get(key).isValid(value);
		}
	}

	public RiverField getWriteField(String key) {
		return writeFields.get(key);
	} 
	
	public SearcherParam getSearchParam(String key) {
		return searchParams.get(key);
	}
	
	public ComputeParam getComputeParams() {
		return computeParams;
	}
	public PipeParam getPipeParam() {
		return pipeParams;
	}

	public MessageParam getMessageParam() {
		return messageParam;
	} 

	public Map<String, RiverField> getWriteFields() {
		return writeFields;
	}
	
	public Map<String, RiverField> getComputeFields() {
		return computeFields;
	}
 
	public boolean openTrans() {
		if((instanceType & INSTANCE_TYPE.Trans.getVal()) > 0){
			if(pipeParams.getReadFrom()!=null && pipeParams.getWriteTo()!=null){
				return true;
			}
		}
		return false;
	}
	
	public boolean openCompute() {
		if((instanceType & INSTANCE_TYPE.WithCompute.getVal()) > 0) {
			return true;
		}
		return false;
	}
 
	public int getInstanceType(){
		return this.instanceType;
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
			Common.LOG.error("loadConfigFromZk error,",e);
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
			 
			Element dataflow = (Element) doc.getElementsByTagName("dataflow").item(0);

			if (dataflow!=null) {  
				if (!dataflow.getAttribute("alias").equals("")) {
					this.alias = dataflow.getAttribute("alias");
				}  
				
				params = (Element) dataflow.getElementsByTagName("TransParam").item(0); 
				if (params!=null) {
					parseNode(params.getElementsByTagName("param"), "pipeParam", BasicParam.class);
				}else{
					Common.LOG.error(this.filename+" config setting not correct");
					return;
				}
				
				params = (Element) dataflow.getElementsByTagName("ReadParam").item(0);
				if(params!=null) {
					parseNode(params.getElementsByTagName("sql"), "readParamSql",
						SQLParam.class); 
					parseNode(params.getElementsByTagName("nosql"), "readParamNoSql",
						NoSQLParam.class);
					params = (Element) params.getElementsByTagName("PageScan").item(0);
					if(params!=null) {
						pipeParams.getReadParam().setPageScan(params.getTextContent().trim());
					}  
				} 
				
				params = (Element) dataflow.getElementsByTagName("ComputeParam").item(0);   
				if (params!=null) {
					parseNode(params.getElementsByTagName("param"), "computeParam", BasicParam.class);
					params = (Element) params.getElementsByTagName("fields").item(0);
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "computeFields", RiverField.class); 
				}
				
				params = (Element) dataflow.getElementsByTagName("WriteParam").item(0);   
				if (params!=null) {
					params = (Element) params.getElementsByTagName("fields").item(0);
					paramlist = params.getElementsByTagName("field");
					parseNode(paramlist, "writeFields", RiverField.class); 
				}
				
				params = (Element) dataflow.getElementsByTagName("SearchParam").item(0);   
				if (params!=null) {
					paramlist = params.getElementsByTagName("param");
					parseNode(paramlist, "SearchParam", SearcherParam.class);
				} 
				
			} 
			
		} catch (Exception e) {
			setStatus(false);
			Common.LOG.error(this.filename+" configParse error,",e);
		}
	}
	
	private void parseNode(NodeList paramlist, String type, Class<?> c)
			throws Exception {
		if (paramlist != null && paramlist.getLength() > 0) {
			for (int i = 0; i < paramlist.getLength(); i++) {
				Node param = paramlist.item(i);
				if (param.getNodeType() == Node.ELEMENT_NODE) {
					Object o = Common.getXmlObj(param, c);
					switch (type) {
					case "writeFields":
						RiverField wf = (RiverField) o;
						writeFields.put(wf.getName(), wf);
						break;
					case "computeFields":
						RiverField cf = (RiverField) o;
						computeFields.put(cf.getName(), cf);
						break;
					case "computeParam":
						BasicParam cbp = (BasicParam) o; 
						computeParams.setKeyValue(cbp.getName(), cbp.getValue());
						break;
					case "pipeParam":
						BasicParam pbp = (BasicParam) o;
						pipeParams.setKeyValue(pbp.getName(), pbp.getValue());
						break;
					case "readParamNoSql":
						pipeParams.setReadParam((NoSQLParam) o);
						break;
					case "readParamSql":
						pipeParams.setReadParam((SQLParam) o);
						break; 
					case "MessageParam":
						messageParam = (MessageParam) o;
						break;
					case "MessageSql":
						messageParam.setSqlParam((SQLParam) o);
						break;
					case "SearchParam":
						SearcherParam v  = (SearcherParam) o;
						searchParams.put(v.getName(), v);
						break;  
					case "PageScan":
						SQLParam sp = (SQLParam) o;
						pipeParams.getReadParam().setPageScan(sp.getSql()); 
						break;
					}
				}
			}
		}
	} 
}
