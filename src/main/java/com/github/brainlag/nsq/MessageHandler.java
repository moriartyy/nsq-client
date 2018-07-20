package com.github.brainlag.nsq;

/**
 * @author hongmiao.yu
 */
public interface MessageHandler {

    void process(Message message);
}
