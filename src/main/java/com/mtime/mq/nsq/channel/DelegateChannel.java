package com.mtime.mq.nsq.channel;

import com.mtime.mq.nsq.*;
import com.mtime.mq.nsq.exceptions.NSQException;

/**
 * @author hongmiao.yu
 */
public class DelegateChannel implements Channel {

    private final Channel delegate;

    public DelegateChannel(Channel delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getReadyCount() {
        return this.delegate.getReadyCount();
    }

    @Override
    public int getInFlight() {
        return this.delegate.getInFlight();
    }

    @Override
    public Config getConfig() {
        return delegate.getConfig();
    }

    @Override
    public void setMessageHandler(MessageHandler messageHandler) {
        delegate.setMessageHandler(messageHandler);
    }

    @Override
    public ServerAddress getRemoteServerAddress() {
        return delegate.getRemoteServerAddress();
    }

    @Override
    public void send(Command command) {
        delegate.send(command);
    }

    @Override
    public Response sendAndWait(Command command) throws NSQException {
        return delegate.sendAndWait(command);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isConnected() {
        return delegate.isConnected();
    }

    @Override
    public void sendReady(int count) {
        delegate.sendReady(count);
    }

    @Override
    public void sendRequeue(byte[] messageId) throws NSQException {
        this.delegate.sendRequeue(messageId);
    }

    @Override
    public void sendRequeue(byte[] messageId, long timeoutMS) {
        delegate.sendRequeue(messageId, timeoutMS);
    }

    @Override
    public void sendFinish(byte[] messageId) throws NSQException {
        this.delegate.sendFinish(messageId);
    }

    @Override
    public void sendTouch(byte[] messageId) throws NSQException {
        this.delegate.sendTouch(messageId);
    }

    @Override
    public Response sendSubscribe(String topic, String channel) throws NSQException {
        return this.delegate.sendSubscribe(topic, channel);
    }
}
