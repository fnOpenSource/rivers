package com.feiniu.param.ml;

public class ComputeParam {
	private String features;
	private String value;
	private String algorithm; 
	private double learn_rate = 0.1;
	private double threshold = 0.001;

	public String getFeatures() {
		return features;
	} 

	public String getValue() {
		return value;
	} 
	
	public String getAlgorithm() {
		return algorithm;
	}  
	
	public double getLearn_rate() {
		return learn_rate;
	} 

	public double getThreshold() {
		return threshold;
	} 
	
	public void setKeyValue(String k, String v) {
		switch (k.toLowerCase()) {
			case "features":
				this.features = v;
				break;
			case "value":
				this.value = v;
				break;
			case "algorithm":
				this.algorithm = v;
				break; 
			case "learn_rate":
				this.learn_rate = Double.parseDouble(v);
				break; 
			case "threshold":
				this.threshold = Double.parseDouble(v);
				break; 
		}
	}
}
