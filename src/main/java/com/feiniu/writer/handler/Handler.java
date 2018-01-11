package com.feiniu.writer.handler;

public interface Handler {
	public <T>T Handle(Object... args);
}
