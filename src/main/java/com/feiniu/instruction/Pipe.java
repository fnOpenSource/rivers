package com.feiniu.instruction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.model.reader.DataPage;
import com.feiniu.model.reader.ReaderState;
import com.feiniu.reader.util.DataSetReader;
import com.feiniu.util.Common;
import com.feiniu.util.FNException;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Pipe extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Pipe");

	/**
	 * @param args
	 *            parameter order is: String mainName, String storeId
	 */
	public static void create(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("Pipe create parameter not match!");
			return;
		}

	}

	public static void remove(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("Pipe remove parameter not match!");
			return;
		}

	}

	/**
	 * @param args
	 *            parameter order is: String id, String instance, String storeId,
	 *            String seq, DataPage pageData, String info, boolean isUpdate,
	 *            boolean monopoly
	 * @throws Exception
	 */
	public static ReaderState writeDataSet(Context context, Object[] args) throws Exception {
		ReaderState rstate = new ReaderState();
		if (!isValid(8, args)) {
			log.error("writeDataSet parameter not match!");
			return rstate;
		}
		String id = String.valueOf(args[0]);
		String instance = String.valueOf(args[1]);
		String storeId = String.valueOf(args[2]);
		String seq = String.valueOf(args[3]);
		DataPage pageData = (DataPage) args[4];
		String info = String.valueOf(args[5]);
		boolean isUpdate = (boolean) args[6];
		boolean monopoly = (boolean) args[7];

		if (pageData.size() == 0)
			return rstate;
		DataSetReader DSReader = new DataSetReader();
		DSReader.init(pageData);
		long start = Common.getNow();
		int num = 0;
		if (DSReader.status()) {
			context.getWriter().PREPARE(monopoly, false);
			if (!context.getWriter().ISLINK()) {
				rstate.setStatus(false);
				return rstate;
			}
			boolean freeConn = false;
			try {
				while (DSReader.nextLine()) {
					context.getWriter().write(context.getInstanceConfig().getWriterParams(), DSReader.getLineData(),
							context.getInstanceConfig().getWriteFields(), instance, storeId, isUpdate);
					num++;
				}
				rstate.setReaderScanStamp(DSReader.getScanStamp());
				rstate.setCount(num);
				log.info(Common.formatLog(" -- " + id + " onepage ", instance, storeId, seq, num,
						DSReader.getDataBoundary(), DSReader.getScanStamp(), Common.getNow() - start, "onepage", info));
			} catch (Exception e) {
				if (e.getMessage().equals("storeId not found")) {
					throw new FNException("storeId not found");
				} else {
					freeConn = true;
				}
			} finally {
				DSReader.close();
				context.getWriter().flush();
				context.getWriter().REALEASE(monopoly, freeConn);
			}
		} else {
			rstate.setStatus(false);
		}
		return rstate;
	}
}
