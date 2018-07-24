package com.mtime.mq.nsq.channel;

import com.mtime.mq.nsq.*;
import com.mtime.mq.nsq.exceptions.NSQException;

import java.io.Closeable;

/**
 * @author hongmiao.yu
 */
public interface Channel extends Closeable {

    int getReadyCount();

    int getInFlight();

    Config getConfig();

    void setMessageHandler(MessageHandler messageHandler);

    ServerAddress getRemoteAddress();

    void send(Command command) throws NSQException;

    Response sendAndWait(Command command) throws NSQException;

    void close();

    boolean isConnected();

    default void sendReady(int count) throws NSQException {
        send(Command.ready(count));
    }

    default void sendRequeue(byte[] messageId) throws NSQException {
        sendRequeue(messageId, 0L);
    }

    default void sendRequeue(byte[] messageId, long timeoutMS) throws NSQException {
        send(Command.requeue(messageId, timeoutMS));
    }

    default void sendFinish(byte[] messageId) throws NSQException {
        send(Command.finish(messageId));
    }

    default void sendTouch(byte[] messageId) throws NSQException {
        send(Command.touch(messageId));
    }

    default Response sendSubscribe(String topic, String channel) throws NSQException {
        return sendAndWait(Command.subscribe(topic, channel));
    }

    default Response sendClose() {
        return sendAndWait(Command.startClose());
    }
}
