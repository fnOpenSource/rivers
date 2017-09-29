package com.feiniu.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class FNIocConfig {
	private ApplicationContext ctx;

	private static final FNIocConfig m_instance = new FNIocConfig();

	private FNIocConfig() {
		ctx = new ClassPathXmlApplicationContext ("spring.xml");
	}

	public static FNIocConfig getInstance() {
		return m_instance;
	}

	public Object getBean(String beanname) {
		return ctx.getBean(beanname);
	}
}
