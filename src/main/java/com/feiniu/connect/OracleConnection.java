package com.feiniu.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleConnection implements FnConnection<Connection>{
	
	private HashMap<String, Object> connectParams = null;
	
	private Connection conn = null;  
	
	private boolean isShare = false;
	
	private final static Logger log = LoggerFactory
			.getLogger(OracleConnection.class);
	
	static{
		 try {
			Class.forName("oracle.jdbc.OracleDriver");
		} catch (Exception e) {
			log.error("OracleConnection Exception,",e);
		}
	}
	
	public static FnConnection<?> getInstance(HashMap<String, Object> ConnectParams){
		FnConnection<?> o = new OracleConnection();
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
		try {
			if (!status()) {
				this.conn = DriverManager.getConnection(getConnectionUrl(),
						String.valueOf(this.connectParams.get("user")), String.valueOf(this.connectParams.get("password")));
				log.info("build connect to the Database " + this.connectParams.get("host")
						+ this.connectParams.get("port") + this.connectParams.get("dbname"));
			}
			return true;
		} catch (Exception e) {
			log.error("connect Exception,", e);
			return false;
		}
	}

	@Override
	public Connection getConnection() {
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
	public boolean status(){
		try {
			if(this.conn != null && !this.conn.isClosed()){
				return true;
			} 
		} catch (Exception e) { 
			log.error("get status Exception,", e);
		} 
		return false;
	}
	
	@Override
	public boolean isShare() { 
		return this.isShare;
	}

	@Override
	public void setShare(boolean share) {
		this.isShare = share;
	} 
	
	private String getConnectionUrl() {
		return "jdbc:oracle:thin:@" + this.connectParams.get("host") + ":" + this.connectParams.get("port") + "/" + this.connectParams.get("sid");		 
	}

}
