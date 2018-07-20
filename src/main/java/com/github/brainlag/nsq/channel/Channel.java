package com.github.brainlag.nsq.channel;

import com.github.brainlag.nsq.*;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * @author hongmiao.yu
 */
public interface Channel {

    Config getConfig();

    int getLeftMessages();

    void setMessageHandler(MessageHandler messageHandler);

    ServerAddress getRemoteServerAddress();

    void send(Command command);

    Response sendAndWait(Command command) throws TimeoutException, InterruptedException;

    void close();

    boolean isConnected();

    void ready(int count);
}
