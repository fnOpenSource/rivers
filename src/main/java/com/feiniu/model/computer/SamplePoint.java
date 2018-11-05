package com.feiniu.model.computer;

public class SamplePoint {
	public double[] features;
	public int feathures_num;
	public double value;
	public int label;

	public SamplePoint(int feathures_num) {
		this.feathures_num = feathures_num;
		features = new double[feathures_num];
	}

}
