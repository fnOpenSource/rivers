package com.feiniu.model;

public class ReaderState {
	
	private String maxId = "";
	private String ReaderScanStamp = "0";
	private int count = 0;
	boolean status = true;
	
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
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	} 
}
