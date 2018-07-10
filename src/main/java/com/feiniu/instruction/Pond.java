package com.feiniu.instruction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feiniu.model.SearcherModel;

public class Pond extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Pond");

	/**
	 * @param args
	 *            parameter order is: String storeName,String storeId
	 */
	public static void createStorePosition(Context context, Object[] args) {
		context.getWriter().LINK();
		try {
			if (!isValid(2, args)) {
				log.error("Pond createStorePosition parameter not match!");
				return;
			}
			String storeName = (String) args[0];
			String storeId = (String) args[1];
			context.getWriter().create(storeName, storeId, context.getInstanceConfig().getTransParams());
		} finally {
			context.getWriter().REALEASE(false);
		}
	}

	/**
	 * @param args
	 *            parameter order is: SearcherModel<?, ?, ?> query,String instance,
	 *            String storeId
	 */
	public static void deleteByQuery(Context context, Object[] args) {
		context.getWriter().LINK();
		boolean freeConn = false;
		try {
			if (!isValid(3, args)) {
				log.error("deleteByQuery parameter not match!");
				return;
			}
			SearcherModel<?, ?, ?> query = (SearcherModel<?, ?, ?>) args[0];
			String instance = (String) args[1];
			String storeId = (String) args[2];
			context.getWriter().delete(query, instance, storeId);
		} catch (Exception e) {
			log.error("DeleteByQuery Exception", e);
			freeConn = true;
		} finally {
			context.getWriter().REALEASE(freeConn);
		}
	}

	/**
	 * @param args
	 *            parameter order is: String instance, String storeId
	 */
	public static void optimizeInstance(Context context, Object[] args) {
		context.getWriter().LINK();
		try {
			if (!isValid(2, args)) {
				log.error("optimizeInstance parameter not match!");
				return;
			}
			String instance = (String) args[0];
			String storeId = (String) args[1];
			context.getWriter().optimize(instance, storeId);
		} finally {
			context.getWriter().REALEASE(false);
		}
	}

	/**
	 * @param args
	 *            parameter order is: String instance, String storeId
	 */
	public static void switchInstance(Context context, Object[] args) {
		try {
			if (!isValid(2, args)) {
				log.error("optimizeInstance parameter not match!");
				return;
			}
			String removeId = "";
			String instance = (String) args[0];
			String storeId = (String) args[1];
			context.getWriter().LINK();
			if (storeId.equals("a")) {
				context.getWriter().optimize(instance, "a");
				removeId = "b";
			} else {
				context.getWriter().optimize(instance, "b");
				removeId = "a";
			} 
			context.getWriter().removeInstance(instance, removeId);
			context.getWriter().setAlias(instance, storeId, context.getInstanceConfig().getAlias());
		}catch (Exception e) {
			log.error("switchInstance Exception",e);
		} finally {
			context.getWriter().REALEASE(true);
			context.getWriter().freeConnPool();
		}
	}

	/**
	 * @param args
	 *            parameter order is: String instanceName, boolean isIncrement,	String dbseq
	 */
	public static String getNewStoreId(Context context, Object[] args) {
		String taskId = null;
		if (!isValid(3, args)) {
			log.error("optimizeInstance parameter not match!");
			return null;
		}
		String instance = (String) args[0];
		boolean isIncrement = (boolean) args[1];
		String dbseq = (String) args[2]; 
		context.getWriter().LINK(); 
		try{
			taskId = context.getWriter().getNewStoreId(instance, isIncrement, dbseq,
					context.getInstanceConfig());
		}finally{
			context.getWriter().REALEASE(false);
		}  
		return taskId;
	}
}
