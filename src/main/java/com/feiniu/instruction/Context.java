package com.feiniu.instruction;

import com.feiniu.config.InstanceConfig;
import com.feiniu.reader.ReaderFlowSocket;
import com.feiniu.writer.WriterFlowSocket;

/** 
 * @author chengwen
 * @version 1.0 
 */
public class Context { 
	
	private InstanceConfig instanceConfig;
	
	private WriterFlowSocket writer;
	
	private ReaderFlowSocket reader;
	
	public static Context initContext(InstanceConfig instanceConfig,WriterFlowSocket writer,ReaderFlowSocket reader) {
		Context c = new Context();
		c.instanceConfig = instanceConfig;
		c.writer = writer;
		c.reader = reader;
		return c;
	}

	public InstanceConfig getInstanceConfig() {
		return instanceConfig;
	}

	public WriterFlowSocket getWriter() {
		return writer;
	}

	public ReaderFlowSocket getReader() {
		return reader;
	} 
	
}
