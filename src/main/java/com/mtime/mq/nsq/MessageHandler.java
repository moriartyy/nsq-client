package com.mtime.mq.nsq;

/**
 * @author hongmiao.yu
 */
@FunctionalInterface
public interface MessageHandler {

    void process(Message message);
}
