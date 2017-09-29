package com.feiniu.connect;

import java.net.InetAddress;
import java.util.HashMap;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESConnection implements FnConnection<Client>{ 
 
	private Client conn;
	
	private HashMap<String, Object> connectParams = null;
	
	private boolean isShare = false;
	
	private final static Logger log = LoggerFactory.getLogger(ESConnection.class);  
  
	public static FnConnection<?> getInstance(HashMap<String, Object> ConnectParams){
		FnConnection<?> o = new ESConnection();
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
			if (this.conn == null) {  
				Settings settings = Settings.builder().put("cluster.name", this.connectParams.get("name")).put("client.transport.sniff", true).build();
				this.conn = TransportClient.builder().settings(settings).build(); 
				String Ips = (String) this.connectParams.get("ip");
				for (String ip : Ips.split(",")) {
					try {
						((TransportClient) this.conn).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), 9300));
					} catch (Exception e) {
						log.error("connect Exception",e);
					}
				}
			}
		} else {
			return false;
		}
		return true;
	}

	@Override
	public Client getConnection() {
		connect();
		return this.conn;
	}

	@Override
	public boolean status() {
		if (this.conn == null || this.conn.admin()==null) {
			return false;
		}
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
