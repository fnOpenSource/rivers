package com.feiniu.ml;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-12-05 10:15
 */
public final class Statistic {
	
	public strictfp static double conditionalEntropy() {
		return 0; 
	}

	public static double entropy(double[] datas) {
		double entropy = 0.0;
		for (Double prob : datas) {
			if (prob > 0) {
				entropy -= prob * Math.log(prob);
			}
		}
		return entropy;
	}

	public strictfp static double conditionalEntropy(double[] data, double[] condition) {
		return 0; 
	}
}
