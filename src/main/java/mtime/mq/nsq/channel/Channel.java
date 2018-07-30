package mtime.mq.nsq.channel;

import mtime.mq.nsq.Command;
import mtime.mq.nsq.MessageHandler;
import mtime.mq.nsq.Response;
import mtime.mq.nsq.ServerAddress;

import java.io.Closeable;

/**
 * @author hongmiao.yu
 */
public interface Channel extends Closeable {

    int getReadyCount();

    int getInFlight();

    void setMessageHandler(MessageHandler messageHandler);

    ServerAddress getRemoteAddress();

    void send(Command command);

    Response sendAndWait(Command command);

    void close();

    boolean isConnected();

    default void sendReady(int count) {
        send(Command.ready(count));
    }

    default void sendRequeue(byte[] messageId) {
        sendRequeue(messageId, 0L);
    }

    default void sendRequeue(byte[] messageId, long timeoutMS) {
        send(Command.requeue(messageId, timeoutMS));
    }

    default void sendFinish(byte[] messageId) {
        send(Command.finish(messageId));
    }

    default void sendTouch(byte[] messageId) {
        send(Command.touch(messageId));
    }

    default Response sendSubscribe(String topic, String channel) {
        return sendAndWait(Command.subscribe(topic, channel));
    }

    default Response sendClose() {
        return sendAndWait(Command.startClose());
    }
}
