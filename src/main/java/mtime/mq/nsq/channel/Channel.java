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

    default ResponseFuture sendReady(int count) {
        return send(Commands.ready(count));
    }

    default ResponseFuture sendRequeue(byte[] messageId) {
        return sendRequeue(messageId, 0L);
    }

    default ResponseFuture sendRequeue(byte[] messageId, long timeoutMS) {
        return send(Commands.requeue(messageId, timeoutMS));
    }

    default ResponseFuture sendFinish(byte[] messageId) {
        return send(Commands.finish(messageId));
    }

    default ResponseFuture sendTouch(byte[] messageId) {
        return send(Commands.touch(messageId));
    }

    default ResponseFuture sendSubscribe(String topic, String channel) {
        return send(Commands.subscribe(topic, channel));
    }

    default ResponseFuture sendClose() {
        return send(Commands.startClose());
    }
}
