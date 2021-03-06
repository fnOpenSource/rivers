package org.elasticflow.instruction;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.writer.WriterFlowSocket;
 
/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-01-22 
 * @modify 2019-01-22 11:24
 */
public class Context { 
	
	private InstanceConfig instanceConfig;
	
	private WriterFlowSocket writer;
	
	private ReaderFlowSocket reader;
	
	/**extension reader for special requires like flow computing*/
	private ReaderFlowSocket extReader;
	
	public static Context initContext(InstanceConfig instanceConfig,WriterFlowSocket writer,ReaderFlowSocket reader,ReaderFlowSocket extReader) {
		Context c = new Context();
		c.instanceConfig = instanceConfig;
		c.writer = writer;
		c.reader = reader;
		c.extReader = extReader;
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
	
	public ReaderFlowSocket extExtReader() {
		return extReader;
	} 
	
}
