package com.feiniu.writer.flow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.FNIocConfig;
import com.feiniu.config.GlobalParam;
import com.feiniu.config.NodeConfig;
import com.feiniu.connect.FnConnection;
import com.feiniu.model.WriteUnit;
import com.feiniu.model.param.WriteParam;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;

/**
 * solr flow Writer Manager
 * @author chengwen
 * @version 1.0 
 */
@NotThreadSafe
public class SolrFlow extends WriterFlowSocket{
	
	private CloudSolrClient conn;
	private FnConnection<?> FC;   
	private static String linux_spilt= "/";
	private static String srcDir= "config/template";
	private static String zkDir = "/configs";
	private final static int BUFFER_LEN = 1024;
	private final static int END = -1;
	private Properties property;
	private List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(); 
	
	private final static Logger log = LoggerFactory.getLogger(SolrFlow.class);  
	
	public static SolrFlow getInstance(HashMap<String, Object> connectParams) {
		SolrFlow o = new SolrFlow();
		o.INIT(connectParams);
		return o;
	}
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams; 
		this.poolName = String.valueOf(connectParams.get("poolName"));
		this.property = (Properties)FNIocConfig.getInstance().getBean("configPathConfig");
		this.batch = GlobalParam.WRITE_BATCH;
	}
	
	@Override
	public void getResource(){
		while(locked.get()){
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				log.error("getResource Exception",e);
			} 
		}
		locked.set(true); 
		PULL(false);
		this.conn = (CloudSolrClient) this.FC.getConnection();
	}
	 
	@Override
	public boolean settings(String instantcName, String storeId, Map<String, WriteParam> paramMap) {
		String name = Common.getStoreName(instantcName, storeId);
		String path = null; 
		try {
			log.info("setting index " + name);
			path = this.property.getProperty("config.path"); 
			String zkHost = this.conn.getZkHost();
			moveFile2ZookeeperDest((path+"/"+srcDir).replace("file:", ""), zkDir+"/"+instantcName, zkHost);
			getSchemaFile(paramMap, instantcName, storeId, zkHost); 
			CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
			create.setConfigName(instantcName);
			create.setCollectionName(name);
			create.setNumShards(3);
			create.setMaxShardsPerNode(2);
			create.setReplicationFactor(2);
			create.process(this.conn, name);
			return true;
		} catch (Exception e) {
			log.error("setting core " + name +" failed.",e); 
		} 
		return false;
	} 
	
	@Override
	public void write(WriteUnit unit,Map<String, WriteParam> writeParamMap, String instantcName, String storeId,boolean isUpdate) throws Exception { 
		String name = Common.getStoreName(instantcName,storeId);
		if (unit.getData().size() == 0){
			log.warn("Empty IndexUnit for " + name );
			return;
		} 
		if(this.conn.getDefaultCollection() == null){
			this.conn.setDefaultCollection(name);
		}  
		SolrInputDocument doc = new SolrInputDocument();
		
		for(Entry<String, Object> r:unit.getData().entrySet()){
			String field = r.getKey(); 
			if (r.getValue() == null)
				continue;
			String value = String.valueOf(r.getValue());
			WriteParam indexParam = writeParamMap.get(field);
			if (indexParam == null)
				continue;
			
			if (indexParam.getAnalyzer().equalsIgnoreCase(("not_analyzed"))){
				if (indexParam.getSeparator() != null){
					String[] vs = value.split(indexParam.getSeparator());
					doc.addField(field, vs);
				}
				else
					doc.addField(field, value); 
			} else {
				doc.addField(field, value.toLowerCase());
			}
		} 
		if(isUpdate)
			doc.addField("_version_", 1);
		docs.add(doc); 
		if (!this.batch) {
			this.conn.add(docs);
			this.conn.commit();
			docs.clear();
		}
	}
	 
	@Override
	public void doDelete(WriteUnit unit, String instantcName, String storeId) throws Exception {  
		String name = Common.getStoreName(instantcName,storeId); 
		Method getMethod = unit.getClass().getMethod("getId", new Class[] {});
		String id = String.valueOf(getMethod.invoke(unit, new Object[] {}));
		log.info("doDelete " + id + "," + name); 
		this.conn.deleteById(id); 
	} 

	@Override
	public void remove(String instanceName, String storeId) {
		if(null == storeId){
			log.info("storeId is Null");
			return;
		} 
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to remove core " + name);
			CollectionAdminRequest.Delete delete = new CollectionAdminRequest.Delete();
			delete.setCollectionName(name);
			delete.process(this.conn,name);
			log.info("solr core " + name + " removed Success!"); 
		} catch (Exception e) {
			log.error("remove core " + name + " Exception,",e);
		} 
	}

	@Override
	public void setAlias(String instanceName, String storeId, String aliasName) { 
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to setting Alais " + instanceName + " to collection " + name);		
			CollectionAdminRequest.CreateAlias createAlias = new CollectionAdminRequest.CreateAlias(); 
			CollectionAdminResponse response = createAlias.setAliasName(instanceName).setAliasedCollections(name).process(this.conn);
			if (response.isSuccess()){
				log.info("alias " + instanceName + " setted to " + name);
			}
		} catch (Exception e) {
			log.error("alias " + instanceName + " set to " + name + " Exception,",e); 
		}  
	} 

	@Override
	public void flush() throws FNException { 
		if(this.batch){
			try{
				this.conn.add(docs);
				this.conn.commit(true, true, true); 
			}catch(Exception e){
				if(e.getMessage().contains("Collection not found")){ 
					throw new FNException("storeId not found");
				}else{
					throw new FNException(e.getMessage());
				}
			}		
			docs.clear();
		} 
	}

	@Override
	public void optimize(String instanceName, String storeId)  { 
		String name = Common.getStoreName(instanceName, storeId);
		try {
			UpdateResponse updateRespons = this.conn.optimize(name, true, true, 1);
			int status = updateRespons.getStatus();
			if (status > 0) {
				log.warn("index " + name + " optimize Failed ");
			}
		} catch (Exception e) {
			log.error("index " + name + " optimize failed.",e); 
		} 
	} 
	

	@Override
	public String getNewStoreId(String instance,boolean isIncrement,String seq, final NodeConfig nodeConfig) { 
		String instanceName = Common.getInstanceName(instance, seq);
		String b = Common.getStoreName(instanceName, "b");
		String a = Common.getStoreName(instanceName, "a");
		String select="";  
		if(isIncrement){
			if(this.existsCollection(a)){ 
				select = "a";
			}else if(this.existsCollection(b)){
				select = "b"; 
			}else{
				select = "a"; 
				settings(instanceName,select, nodeConfig.getWriteParamMap());
				setAlias(instanceName, select, nodeConfig.getAlias());
			}   
		}else{
			select =  "b";
			if(this.existsCollection(b)){ 
				select =  "a";
			}
			settings(instanceName,select, nodeConfig.getWriteParamMap());
		} 
		return select;
	}
 
	private void getSchemaFile(Map<String,WriteParam> paramMap,String instantcName, String storeId,String zkHost) {
		BufferedReader head_reader = null;
		BufferedReader tail_reader = null; 
		String path = null;
		String destPaht = zkDir+"/"+instantcName+"/schema.xml";
		ZooKeeper zk = null;
		Stat stat = null;
		final CountDownLatch connectedSemaphore = new CountDownLatch( 1 ); //防止出现ConnectionLossException
		StringBuffer sb = new StringBuffer();
		try { 
			Watcher watcher = new Watcher() { 
				public void process(WatchedEvent event) {
					connectedSemaphore.countDown();
				}
			}; 
			zk = new ZooKeeper(zkHost, 5000, watcher);
			connectedSemaphore.await();
			 
			stat = zk.exists(destPaht, watcher);
			if (null == stat) {
				zk.create(destPaht, "".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			
			path = property.getProperty("config.path");
			String head = path+"/"+srcDir+"/schema_head.txt";
			String tail = path+"/"+srcDir+"/schema_tail.txt";
			head_reader = new BufferedReader(new InputStreamReader(new FileInputStream(head.replace("file:", "")), "UTF-8"));
			tail_reader = new BufferedReader(new FileReader(tail.replace("file:", "")));
			String str = null;
			while ((str = head_reader.readLine()) != null) {
				sb.append(str).append("\n");
			}
			
			StringBuilder field = new StringBuilder();
			String firstFiled = null;
			for (Map.Entry<String, WriteParam> e : paramMap.entrySet()) {
				WriteParam p = e.getValue();
				field.delete(0, field.length());
				if (p.getName() == null)
					continue;
				if(firstFiled == null){
					firstFiled = p.getName();
				}
				field.append("<field ").append("name=\"").append(p.getName()).append("\" ");
				if("string".equals(p.getIndextype()) && "not_analyzed".equalsIgnoreCase(p.getAnalyzer())) {
					field.append("type=\"").append("string\" ");
				}else{
					field.append("type=\"").append(p.getIndextype()).append("\" ");
				}
				field.append("indexed=\"").append(p.getIndexed()).append("\" ");
				field.append("stored=\"").append(p.getStored()).append("\" ");
				field.append("required=\"false\" />");
				sb.append(field.toString()).append("\n");
			}
			
			String uniqeKey = "<uniqueKey>"+firstFiled+"</uniqueKey>";
			String defaultSearchField = "<defaultSearchField>"+firstFiled+"</defaultSearchField>";
			sb.append(uniqeKey).append("\n");
			sb.append(defaultSearchField).append("\n");
			while ((str = tail_reader.readLine()) != null) {
				sb.append(str).append("\n");
			}

			zk.setData(destPaht, sb.toString().getBytes(), -1);
		} catch (Exception e) {
			log.error("getSchemaFile Exception ",e); 
		} finally {
			try {
				if(zk != null){
					zk.close();
				}
				head_reader.close();
				tail_reader.close();
			} catch (Exception e) {
				log.error("zookeeper close Exception" ,e); 
			}

		}
	}
	
	private boolean existsCollection(String collection){ 
		SolrQuery qb = new SolrQuery();
		try {
			this.conn.query(collection, qb);
			return true;
		} catch (Exception e) {
			return false;
		} 
	}  
	
	private static void moveFile(String sourceAdd, ZooKeeper zk,Watcher watcher, String destinationAdd) { 
		InputStream in = null;
		Stat stat = null;
		String remoteAdd = null;
		try {
			File file = new File(sourceAdd); 
			if(!file.isDirectory()){
				in = new FileInputStream(sourceAdd);
				remoteAdd = destinationAdd+linux_spilt+file.getName(); 
				stat = zk.exists(remoteAdd, watcher);
				if (null == stat) {
					zk.create(remoteAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				StringBuffer sb = new StringBuffer();
				byte[] buffer = new byte[BUFFER_LEN];
				while (true) {
					int byteRead = in.read(buffer);
					if (byteRead == END)
						break;
					sb.append(new String(buffer, 0, byteRead));
				}
				zk.setData(remoteAdd, sb.toString().getBytes(), -1); 
			}else{
                String[] filelist = file.list();
                for (int i = 0; i < filelist.length; i++) {
                    File readfile = new File(sourceAdd + File.separator + filelist[i]);
                    if (!readfile.isDirectory()) {
                   	   in = new FileInputStream(sourceAdd +  File.separator + filelist[i]);
                   	   
                   	   stat = zk.exists(destinationAdd, watcher);
					   if (null == stat) {
						zk.create(destinationAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					    } 
						remoteAdd =  destinationAdd+linux_spilt + file.getName();
						stat = zk.exists(remoteAdd, watcher);
						if (null == stat) {
							zk.create(remoteAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						remoteAdd =  destinationAdd+ linux_spilt + file.getName()+linux_spilt+readfile.getName(); 
						stat = zk.exists(remoteAdd, watcher);
						if (null == stat) {
							zk.create(remoteAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						} 
						StringBuffer sb = new StringBuffer();
						byte[] buffer = new byte[BUFFER_LEN];
						while (true) {
							int byteRead = in.read(buffer);
							if (byteRead == END)
								break;
							sb.append(new String(buffer,0,byteRead));
						}
						zk.setData(remoteAdd, sb.toString().getBytes(), -1); 
                    } else if (readfile.isDirectory()) { 
                     String sourceAdd2 = sourceAdd + File.separator + readfile.getName();
                     String destinationAdd2 = destinationAdd+linux_spilt+file.getName(); 
                   	 moveFile(sourceAdd2,zk,watcher,destinationAdd2);
                    }
                }
			}
		} catch (Exception e) {
			log.error("moveFile error,",e);
		} finally {
			try {
				if(null != in){
					in.close();
				}
			} catch (Exception e) {
				log.error("zookeeper close Exception," ,e);
			}
		}
	}
	
	private static void moveFile2ZookeeperDest( String sourceAdd, String destinationAdd,String zkHost) {
		ZooKeeper zk = null;
		Stat stat = null;
		final CountDownLatch connectedSemaphore = new CountDownLatch( 1 ); //防止出现ConnectionLossException
		try { 
			Watcher watcher = new Watcher() { 
				public void process(WatchedEvent event) {
					connectedSemaphore.countDown();
				}
			}; 
			zk = new ZooKeeper(zkHost, 5000, watcher);
			connectedSemaphore.await();
			 
			stat = zk.exists(destinationAdd, watcher);
			if (null == stat) {
				zk.create(destinationAdd, "".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			File file = new File(sourceAdd);
			if (file.isDirectory()) {
				String[] filelist = file.list();
				for(String i_file : filelist){
					moveFile(sourceAdd+File.separator+i_file, zk, watcher, destinationAdd);
				}
			} 
		} catch (Exception e) {
			log.error("moveFile2ZookeeperDest Exception,",e); 
		} finally {
			try {
				if (null != zk) {
					zk.close();
				}
			} catch (Exception e) {
				log.error("zk close Exception,",e); 
			}
		} 
	}
}
