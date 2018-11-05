package com.feiniu.ml.algorithm;

import java.util.LinkedList;
import java.util.Map;

import com.feiniu.field.RiverField;
import com.feiniu.instruction.Context;
import com.feiniu.ml.MathFunction;
import com.feiniu.model.computer.SamplePoint;
import com.feiniu.model.computer.SampleSets;
import com.feiniu.model.reader.DataPage;
import com.feiniu.model.reader.PipeDataUnit;

public class LogisticRegression extends Regression {

	public static DataPage train(Context context,SampleSets samples,Map<String, RiverField> transParam) {
		LogisticRegression lr = new LogisticRegression();
		double[] para = new double[samples.getData()[0].feathures_num];
		double rate = context.getInstanceConfig().getComputeParams().getLearn_rate();
		double th = context.getInstanceConfig().getComputeParams().getThreshold();
		lr.Initialize(samples);
		lr.setPara(para, rate, th);
		lr.Update();
		DataPage DP = new DataPage();
		PipeDataUnit du = new PipeDataUnit();
		LinkedList<PipeDataUnit> dataUnit = new LinkedList<>(); 
		du.addFieldValue("model", lr.getModel(), transParam);
		StringBuffer sf = new StringBuffer();
		sf.append(context.getInstanceConfig().getComputeParams().getAlgorithm()+",<br> ");
		sf.append(context.getInstanceConfig().getComputeParams().getValue()+",<br> ");
		sf.append(context.getInstanceConfig().getComputeParams().getFeatures());
		du.addFieldValue("remark", sf.toString(), transParam);
		dataUnit.add(du);
		DP.putData(dataUnit);
		//lr.OutputTheta();
		return DP;
	}
	
	@Override
	public Object predict(SamplePoint point) { 
		paraNum = point.features.length;
		return PreVal(point);
	}
	
	public static LogisticRegression getInstance(Object model) {
		LogisticRegression lr = new LogisticRegression();
		lr.loadModel(model);
		return lr;
	}

	public double PreVal(SamplePoint s) {
		double val = 0;
		for (int i = 0; i < paraNum; i++) {
			val += theta[i] * s.features[i];
		}
		return MathFunction.sigmoid(val);
	}

	public double CostFun() {
		double sum = 0;
		for (int i = 0; i < samNum; i++) {
			double p = PreVal(samples[i]);
			double d = Math.log(p) * samples[i].label + (1 - samples[i].label) * Math.log(1 - p); 
			sum += d;
		}
		return -1 * (sum / samNum);
	}
/**
 * former = 0; // the cost before update
 * latter = CostFun(); // the cost after update
 */
	public void Update() {
		double former = 0; 
		double latter = CostFun();  
		double d = 0;
		int trainTime=100;
		double[] p = new double[paraNum];
		do {
			former = latter;
			for (int i = 0; i < paraNum; i++) {
				for (int j = 0; j < samNum; j++) {
					d += (PreVal(samples[j]) - samples[j].value) * samples[j].features[i];
				}
				p[i] -= (rate * d) / samNum;
			}
			latter = CostFun();
			if (former - latter < 0) {
				System.out.println("α is larger!!!");
				break;
			}
			trainTime--;
		} while (trainTime==0 || former - latter > th);
		theta = p;
	}
 
}