package com.feiniu.ml;

import com.feiniu.model.computer.SamplePoint;

public interface Algorithm {
	
	public boolean loadModel(Object datas);
	 
    /**
     * predicte the value of sample s
     * @param s : prediction sample
     * @return : predicted value
     */
	public Object predict(SamplePoint point);
}
