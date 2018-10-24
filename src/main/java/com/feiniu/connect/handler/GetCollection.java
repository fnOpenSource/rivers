package com.feiniu.connect.handler;

import java.util.HashMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.GlobalParam;
import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
/**
 * auto get current store in zookeeper collection name
 * @author chengwen
 * @version 1.0
 */
public class GetCollection implements ConnectionHandler{

	private Watcher watcher; 
	private final String path = "/freshcollectionname";
	private HashMap<String, Object> connectParams;
	private String poolName;
	private final static Logger log = LoggerFactory.getLogger(GetCollection.class);

	@Override
	public void init(HashMap<String, Object> Params) {
		this.connectParams = new HashMap<>();
		this.poolName = "zookeeper_"+Params.get("ip");
		this.connectParams.put("ip", Params.get("ip"));
		this.connectParams.put("type", GlobalParam.DATA_TYPE.ZOOKEEPER);
		this.watcher = new Watcher() {			
			public void process(WatchedEvent event) { 
			}
		};
		this.connectParams.put("watcher", this.watcher); 
		this.connectParams.put("maxConn", "2");
	}

	@Override
	public String getData() {
		FnConnection<?> FC = FnConnectionPool.getConn(this.connectParams,
				this.poolName,true);
		ZooKeeper conn = (ZooKeeper) FC.getConnection(true);
		try {
			byte[] bt = conn.getData(path, this.watcher, null);
			if (bt.length > 0) {
				return new String(bt);
			}  
		} catch (Exception e) {
			log.error("getData Exception", e); 
		}finally{
			FnConnectionPool.freeConn(FC, this.poolName,false); 
		} 
		return null;
	}
}
