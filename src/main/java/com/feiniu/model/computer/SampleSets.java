package com.feiniu.model.computer;

import com.feiniu.model.reader.PipeDataUnit;
import com.feiniu.param.ml.ComputeParam;

public class SampleSets {
	
	private SamplePoint[] datas;
	private int iterator = 0;
	
	public static SampleSets getInstance(int nums){
		SampleSets sp = new SampleSets();
		sp.datas = new SamplePoint[nums];
		return sp;
	}
	
	public void addPoint(PipeDataUnit PD,ComputeParam computeParam) {
		if(iterator<this.datas.length) { 
			int i=0;
			String[] feature_fields = computeParam.getFeatures().split(",");
			this.datas[iterator] = new SamplePoint(feature_fields.length); 
			for(String field:feature_fields) {
				this.datas[iterator].features[i] = Double.valueOf(String.valueOf(PD.getData().get(field))); 
			}
			this.datas[iterator].value = Double.valueOf(String.valueOf(PD.getData().get(computeParam.getValue()))); 
			iterator++;
		}
	}
	
	public int samplesNums() {
		return this.iterator;
	}
	
	public SamplePoint[] getData() {
		return this.datas;
	}
	
}