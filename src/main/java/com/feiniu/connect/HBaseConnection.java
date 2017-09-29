package com.feiniu.connect;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseConnection implements FnConnection<HTable>{
	private final static Logger log = LoggerFactory
			.getLogger(HBaseConnection.class);
	private Configuration hbaseConfig; 
	private HTable conn;
	private HashMap<String, Object> connectParams;
	private boolean isShare = false;

	public static FnConnection<?> getInstance(HashMap<String, Object> ConnectParams){
		FnConnection<?> o = new HBaseConnection();
		o.init(ConnectParams); 
		o.connect();
		return o;
	}
	
	@Override
	public void init(HashMap<String, Object> ConnectParams) {
		this.connectParams = ConnectParams; 
		Configuration config = new Configuration();
		config.addResource("classpath:/hbase/hbase-site.xml");
		this.hbaseConfig = HBaseConfiguration.create(config);

		String ipString = (String) this.connectParams.get("ip");
		if (ipString != null && ipString.length() > 0) {
			String[] ips = ipString.split(",");
			StringBuilder ipStr = new StringBuilder();
			String port = null;
			if (ips.length > 0) {
				for (String ip : ips) {
					String[] ipPort = ip.split(":");
					if (ipPort != null && ipPort.length > 0) {
						if (ipStr.length() > 0)
							ipStr.append(",");
						ipStr.append(ipPort[0]);
					}
					if (ipPort != null && ipPort.length > 1)
						port = ipPort[1];
				}
			}
			if (ipStr.length() > 0)
				this.hbaseConfig.set("hbase.zookeeper.quorum", ipStr.toString());
			if (port != null && port.length() > 0)
				this.hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
		} 
	}
	
	@Override
	public HTable getConnection() {
		int tryTime=0;
		try {
			while(tryTime<5 && !connect()){ 
				tryTime++;
				Thread.sleep(2000); 
			} 
		} catch (Exception e) {
			log.error("try to get Connection Exception,", e);
		}
		return this.conn;
	}

	@Override
	public boolean status() {
		try {
			if(this.conn != null){
				return true;
			} 
		} catch (Exception e) { 
			log.error("get status Exception,", e);
		} 
		return false;
	}

	@Override
	public boolean free() {
		try {
			this.conn.close();
			this.conn = null;
			this.connectParams = null;
		} catch (Exception e) {
			log.error("free connect Exception,", e);
			return false;
		}
		return true;
	} 

	@Override
	public boolean connect() {
		if (!status()) {
			try {
				this.conn = new HTable(hbaseConfig, (String) this.connectParams.get("tableName"));
			} catch (Exception e) {
				log.error("HBase connect Exception,", e);
				return false;
			}
		}
		return true;
	} 
	
	@Override
	public boolean isShare() { 
		return this.isShare;
	}

	@Override
	public void setShare(boolean share) {
		this.isShare = share;
	} 
}
