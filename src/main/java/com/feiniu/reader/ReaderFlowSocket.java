package com.feiniu.reader;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.connect.FnConnection;
import com.feiniu.connect.FnConnectionPool;
import com.feiniu.field.RiverField;
import com.feiniu.flow.Flow;
import com.feiniu.model.DataPage;
import com.feiniu.model.PipeDataUnit;
import com.feiniu.reader.handler.Handler;

/**
 * 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-12 14:28
 */
@NotThreadSafe
public class ReaderFlowSocket implements Flow{ 
	
	protected volatile HashMap<String, Object> connectParams;

	protected DataPage dataPage = new DataPage();
	
	protected String poolName;
	
	protected LinkedList<PipeDataUnit> dataUnit = new LinkedList<PipeDataUnit>(); 
	
	public final Lock lock = new ReentrantLock();
	
	protected FnConnection<?> FC;
	
	protected AtomicInteger retainer = new AtomicInteger(0);
	 
	private final static Logger log = LoggerFactory.getLogger(ReaderFlowSocket.class);
	
	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName")); 
	}

	@Override
	public FnConnection<?> PREPARE(boolean isMonopoly,boolean canSharePipe) {  
		if(isMonopoly) {
			synchronized (this) {
				if(this.FC==null) 
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
			} 
		}else {
			synchronized (retainer) { 
				if(retainer.incrementAndGet()==1 || this.FC==null) {
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
					retainer.set(1);;
				}
			} 
		} 
		return this.FC;
	}
	
	@Override
	public void REALEASE(boolean isMonopoly,boolean releaseConn) { 
		if(isMonopoly==false) { 
			synchronized(retainer){ 
				if(retainer.decrementAndGet()<=0){
					FnConnectionPool.freeConn(this.FC, this.poolName,releaseConn);
					retainer.set(0);
				}else{
					log.info(this.FC+" retainer is "+retainer.get());
				}
			} 
		} 
	} 
	
	@Override
	public FnConnection<?> GETSOCKET() {
		return this.FC;
	}

	@Override
	public boolean ISLINK() { 
		if(this.FC==null) 
			return false;
		return true;
	}   

	public DataPage getPageData(HashMap<String, String> param,Map<String, RiverField> transParams,Handler handler) {
		return null;
	}

	public List<String> getPageSplit(HashMap<String, String> param) {
		return null;
	}
	
	public void freeJobPage() {
		this.dataPage.clear(); 
		this.dataUnit.clear();  
	} 
}
