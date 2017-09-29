package com.feiniu.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MysqlConnection implements FnConnection<Connection>{
	
	private HashMap<String, Object> connectParams;
	
	private Connection conn = null; 
	
	private boolean isShare = false;
	
	private final static Logger log = LoggerFactory
			.getLogger(MysqlConnection.class);
	
	static{
		 try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			log.error("MysqlConnection Exception,",e);
		}
	}
	
	public static FnConnection<?> getInstance(HashMap<String, Object> ConnectParams){
		FnConnection<?> o = new MysqlConnection();
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
						this.connectParams.get("user").toString(), this.connectParams.get("password").toString());
				log.info("build connect to the Database " + this.connectParams.get("host")
						+ this.connectParams.get("port") + this.connectParams.get("dbname"));
			}
			return true;
		} catch (Exception e) {
			log.error(this.connectParams.get("host")+" connect Exception,", e);
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
		return "jdbc:mysql://" + this.connectParams.get("host") + ":" + this.connectParams.get("port") + "/" + this.connectParams.get("dbname")
				+ "?autoReconnect=true&failOverReadOnly=false";
	} 

}
