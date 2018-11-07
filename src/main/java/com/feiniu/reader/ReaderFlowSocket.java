package com.feiniu.reader;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.NotThreadSafe;

import com.feiniu.field.RiverField;
import com.feiniu.flow.Flow;
import com.feiniu.model.reader.DataPage;
import com.feiniu.model.reader.PipeDataUnit;
import com.feiniu.reader.handler.Handler;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-12 14:28
 */
@NotThreadSafe
public abstract class ReaderFlowSocket extends Flow{  

	protected DataPage dataPage = new DataPage(); 
	
	protected LinkedList<PipeDataUnit> dataUnit = new LinkedList<>(); 
	
	public final Lock lock = new ReentrantLock();   
	
	@Override
	public void INIT(final HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		this.poolName = String.valueOf(connectParams.get("poolName")); 
	} 
	 
	public abstract DataPage getPageData(final HashMap<String, String> param,final Map<String, RiverField> transParams,Handler handler,int pageSize);

	public abstract List<String> getPageSplit(final HashMap<String, String> param,int pageSize);
	
	public void freeJobPage() {
		this.dataPage.clear(); 
		this.dataUnit.clear();  
	} 
}
