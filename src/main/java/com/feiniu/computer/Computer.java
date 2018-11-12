package com.feiniu.computer;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.config.InstanceConfig;
import com.feiniu.ml.Algorithm;
import com.feiniu.model.ResponseState;
import com.feiniu.model.RiverRequest;
import com.feiniu.model.computer.SamplePoint;
import com.feiniu.model.reader.DataPage;
import com.feiniu.model.reader.PipeDataUnit;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.reader.util.DataSetReader;
import com.feiniu.util.Common; 

/**
 * provide compute service
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Computer {
	private final static Logger log = LoggerFactory.getLogger(Computer.class);
	private InstanceConfig instanceConfig;
	private String instanceName; 
	private ReaderFlowSocket readerFlowSocket;
	private Object model;
	private Algorithm algorithm;
	private String usage;
	
	public static Computer getInstance(String instanceName,InstanceConfig instanceConfig,ReaderFlowSocket readerFlowSocket) {
		return new Computer(instanceName, instanceConfig, readerFlowSocket);
	}
	
	private Computer(String instanceName, InstanceConfig instanceConfig,ReaderFlowSocket readerFlowSocket) {
		this.instanceConfig = instanceConfig;
		this.instanceName = instanceName;
		this.readerFlowSocket = readerFlowSocket;
	}
	
	public ResponseState startCompute(RiverRequest rq) {
		ResponseState response = ResponseState.getInstance();
		response.setInstance(instanceName); 
		if(model==null) {
			log.info("start loading model...");
			HashMap<String, String> params = new HashMap<>(); 
			String table = Common.getStoreName(instanceName, Common.getStoreIdFromZK(instanceName,"",true));
			params.put("sql", "select model,remark from "+table); 
			DataPage dp = readerFlowSocket.getPageData(params, instanceConfig.getWriteFields(), null,instanceConfig.getPipeParams().getReadPageSize());
			DataSetReader DSReader = new DataSetReader();
			DSReader.init(dp);
			while (DSReader.nextLine()) {
				PipeDataUnit pd = DSReader.getLineData();
				model = pd.getData().get("model");
				usage = String.valueOf(pd.getData().get("remark"));
			} 
			log.info("computer model ready to complete!");
			try {
				Class<?> clz = Class.forName("com.feiniu.ml.algorithm."+instanceConfig.getComputeParams().getAlgorithm()); 
				algorithm = (Algorithm) clz.newInstance();
				algorithm.loadModel(model);
			}catch (Exception e) {
				log.error("load algorithm Exception",e);
			} 
		}   
		if(rq.getParam("data")!=null) {
			String[] dt = rq.getParam("data").split(",");
			SamplePoint point = new SamplePoint(dt.length);
			for(int i=0;i<dt.length;i++) {
				point.features[i] = Double.valueOf(dt[i]);
			}  
			response.setPayload(algorithm.predict(point));
		} 
		response.setInfo(usage); 
		return response;
	}
}
