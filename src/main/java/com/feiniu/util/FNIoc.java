package com.feiniu.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public final class FNIoc {
	private ApplicationContext ctx;

	private static final FNIoc m_instance = new FNIoc();

	private FNIoc() {
		ctx = new ClassPathXmlApplicationContext ("spring.xml");
	}

	public static FNIoc getInstance() {
		return m_instance;
	}

	public Object getBean(String beanname) {
		return ctx.getBean(beanname);
	}
}
