package com.feiniu.connect;

import java.util.HashMap;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrConnection implements FnConnection<CloudSolrClient> {

	private final static int zkClientTimeout = 180000;
	private final static int zkConnectTimeout = 60000;
	private long createTime = System.currentTimeMillis();

	private HashMap<String, Object> connectParams = null;

	private CloudSolrClient conn = null;
	
	private boolean isShare = false;

	private final static Logger log = LoggerFactory
			.getLogger(SolrConnection.class);

	public static FnConnection<?> getInstance(
			HashMap<String, Object> ConnectParams) {
		FnConnection<?> o = new SolrConnection();
		o.init(ConnectParams);
		o.connect();
		return o;
	}

	@Override
	public void init(HashMap<String, Object> ConnectParams) {
		this.connectParams = ConnectParams;
	}

	@Override
	public boolean connect() {
		if (this.connectParams.get("ip") != null) {
			if (!status()) {
				this.conn = new CloudSolrClient(
						(String) this.connectParams.get("ip"));
				this.conn.setZkClientTimeout(zkClientTimeout);
				this.conn.setZkConnectTimeout(zkConnectTimeout);
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public CloudSolrClient getConnection() {
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
		if (this.conn == null || System.currentTimeMillis()-createTime>zkClientTimeout) {
			return false;
		}
		createTime = System.currentTimeMillis();
		return true;
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
	public boolean isShare() { 
		return this.isShare;
	}

	@Override
	public void setShare(boolean share) {
		this.isShare = share;
	} 

}
