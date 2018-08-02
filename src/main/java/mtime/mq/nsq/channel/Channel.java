package mtime.mq.nsq.channel;

import mtime.mq.nsq.*;

import java.io.Closeable;

/**
 * @author hongmiao.yu
 */
public interface Channel extends Closeable {

    int getReadyCount();

    int getInFlight();

    void setMessageHandler(MessageHandler messageHandler);

    ServerAddress getRemoteAddress();

    ResponseFuture send(Command command);

    void close();

    boolean isConnected();

    default void sendReady(int count) {
        send(Commands.ready(count).expectedResponse(false));
    }

    default void sendRequeue(byte[] messageId) {
        sendRequeue(messageId, 0L);
    }

    default void sendRequeue(byte[] messageId, long timeoutMS) {
        send(Commands.requeue(messageId, timeoutMS).expectedResponse(false));
    }

    default void sendFinish(byte[] messageId) {
        send(Commands.finish(messageId).expectedResponse(false));
    }

    default void sendTouch(byte[] messageId) {
        send(Commands.touch(messageId).expectedResponse(false));
    }

    default ResponseFuture sendSubscribe(String topic, String channel) {
        return send(Commands.subscribe(topic, channel));
    }

    default ResponseFuture sendClose() {
        return send(Commands.startClose());
    }
}
