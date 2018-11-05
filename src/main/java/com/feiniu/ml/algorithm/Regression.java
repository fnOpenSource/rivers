package com.feiniu.ml.algorithm;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.ml.Algorithm;
import com.feiniu.model.computer.SamplePoint;
import com.feiniu.model.computer.SampleSets;

public abstract class Regression implements Algorithm{
	
    double[] theta; //parameters
    int paraNum; //the number of parameters
    double rate; //learning rate
    SamplePoint[] samples; // samples
    int samNum; // the number of samples
    double th; // threshold value
    protected final static Logger log = LoggerFactory.getLogger("Regression");
    
    /**
     * initialize the samples
     * @param s : training set
     * @param num : the number of training samples
     */
    public void Initialize(SampleSets samples) {
        samNum = samples.samplesNums();
        this.samples = samples.getData(); 
    }
    
    @Override
    public boolean loadModel(Object datas) {
    	ArrayList<Double> rs = new ArrayList<>();
    	for(String s:String.valueOf(datas).split(",")) {
    		if(s.length()>0)
    			rs.add(Double.parseDouble(s));
    	}
    	theta = new double[rs.size()];
    	int i=0;
    	for(Double d:rs) {
    		theta[i] = d;
    		i++;
    	}
    	return true;
    } 
    
    /**
     * initialize all parameters
     * @param para : theta
     * @param learning_rate 
     * @param threshold 
     */
    public void setPara(double[] para, double learning_rate, double threshold) {
        paraNum = para.length;
        theta = para;
        rate = learning_rate;
        th = threshold;
    }
    
    /**
     * calculate the cost of all samples
     * @return : the cost
     */
    public abstract double CostFun();
    
    /**
     * update the theta
     */
    public abstract void Update();
    
    public String getModel() {
    	StringBuffer sf = new StringBuffer();
    	for(int i = 0; i < paraNum; i++) {
    		sf.append(theta[i] + ","); 
        }
    	return sf.toString();
    }
    public void OutputTheta() {
        System.out.println("The parameters are:");
        for(int i = 0; i < paraNum; i++) {
            System.out.print(theta[i] + " ");
        }
        System.out.println(CostFun());
    }
}