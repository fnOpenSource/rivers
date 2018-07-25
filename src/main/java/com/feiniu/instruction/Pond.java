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
	public static boolean createStorePosition(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("Pond createStorePosition parameter not match!");
			return false;
		}
		if (context.getWriter().LINK()) {
			try { 
				String storeName = (String) args[0];
				String storeId = (String) args[1];
				context.getWriter().create(storeName, storeId, context.getInstanceConfig().getTransParams());
				return true;
			} finally {
				context.getWriter().REALEASE(false);
			}
		}
		return false;
	}

	/**
	 * @param args
	 *            parameter order is: SearcherModel<?, ?, ?> query,String instance,
	 *            String storeId
	 */
	public static void deleteByQuery(Context context, Object[] args) {
		boolean freeConn = false;
		if (!isValid(3, args)) {
			log.error("deleteByQuery parameter not match!");
			return;
		}
		if (context.getWriter().LINK()) {
			try {
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
	}

	/**
	 * @param args
	 *            parameter order is: String instance, String storeId
	 */
	public static void optimizeInstance(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("optimizeInstance parameter not match!");
			return;
		}
		if (context.getWriter().LINK()) {
			try {
				String instance = (String) args[0];
				String storeId = (String) args[1];
				context.getWriter().optimize(instance, storeId);
			} finally {
				context.getWriter().REALEASE(false);
			}
		}
	}

	/**
	 * @param args
	 *            parameter order is: String instance, String storeId
	 */
	public static boolean switchInstance(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("optimizeInstance parameter not match!");
			return false;
		}
		String removeId = "";
		String instance = (String) args[0];
		String storeId = (String) args[1];
		if (context.getWriter().LINK()) {
			try {
				if (storeId.equals("a")) {
					context.getWriter().optimize(instance, "a");
					removeId = "b";
				} else {
					context.getWriter().optimize(instance, "b");
					removeId = "a";
				}
				context.getWriter().removeInstance(instance, removeId);
				context.getWriter().setAlias(instance, storeId, context.getInstanceConfig().getAlias());
				return true;
			} catch (Exception e) {
				log.error("switchInstance Exception", e);
			} finally {
				context.getWriter().REALEASE(true);
				context.getWriter().freeConnPool();
			}
		}
		return false;
	}

	/**
	 * @param args
	 *            parameter order is: String instanceName, boolean isIncrement,
	 *            String dbseq
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
		if (context.getWriter().LINK()) {
			try {
				taskId = context.getWriter().getNewStoreId(instance, isIncrement, dbseq, context.getInstanceConfig());
			} finally {
				context.getWriter().REALEASE(false);
			}
		}
		return taskId;
	}
}
