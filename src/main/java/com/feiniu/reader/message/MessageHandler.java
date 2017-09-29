package com.feiniu.reader.message; 

public interface MessageHandler {
	public boolean handle(Object... args);
}
