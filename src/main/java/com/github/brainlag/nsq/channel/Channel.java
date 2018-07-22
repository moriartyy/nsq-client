package com.github.brainlag.nsq.channel;

import com.github.brainlag.nsq.*;
import com.github.brainlag.nsq.exceptions.NSQException;

/**
 * @author hongmiao.yu
 */
public interface Channel {

    int getReady();

    int getInFlight();

    Config getConfig();

    void setMessageHandler(MessageHandler messageHandler);

    ServerAddress getRemoteServerAddress();

    void send(Command command) throws NSQException;

    Response sendAndWait(Command command) throws NSQException;

    void close();

    boolean isConnected();

    void sendReady(int count) throws NSQException;

    void sendRequeue(byte[] messageId) throws NSQException;

    void sendRequeue(byte[] messageId, long timeoutMS) throws NSQException;

    void sendFinish(byte[] messageId) throws NSQException;

    void sendTouch(byte[] messageId) throws NSQException;

    Response sendSubscribe(String topic, String channel) throws NSQException;
}
