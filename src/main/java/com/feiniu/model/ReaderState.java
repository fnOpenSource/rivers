package com.feiniu.model;

public class ReaderState {
	String maxId = "";
	String ReaderScanStamp = "0";
	int count = 0;
	public String getMaxId() {
		return maxId;
	}
	public void setMaxId(String maxId) {
		this.maxId = maxId;
	}
	public String getReaderScanStamp() {
		return ReaderScanStamp;
	}
	public void setReaderScanStamp(String ReaderScanStamp) {
		this.ReaderScanStamp = ReaderScanStamp;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	
}
