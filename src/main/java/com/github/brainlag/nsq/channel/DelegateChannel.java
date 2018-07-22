package com.github.brainlag.nsq.channel;

import com.github.brainlag.nsq.*;
import com.github.brainlag.nsq.exceptions.NSQException;

/**
 * @author hongmiao.yu
 */
public class DelegateChannel implements Channel {

    private final Channel delegate;

    public DelegateChannel(Channel delegate) {
        this.delegate = delegate;
    }

    @Override
    public Config getConfig() {
        return delegate.getConfig();
    }

    @Override
    public int getLeftMessages() {
        return delegate.getLeftMessages();
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
    public void sendRequeue(byte[] messageId, long timeoutMS) {
        delegate.sendRequeue(messageId, timeoutMS);
    }
}
