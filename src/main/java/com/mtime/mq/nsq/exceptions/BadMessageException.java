package com.mtime.mq.nsq.exceptions;

public class BadMessageException extends NSQException {

	public BadMessageException(String message) {
		super(message);
	}
}
